import re
from collections import defaultdict
from typing import Any, Optional, Sequence

from py_common_lib.logger import EPMPYLogger

from src.config import settings
from src.db.engines.clickhouse.engine import execute_raw_DDL, execute_raw_DQL_or_DML
from src.integrations.modelling_tools_api.codegen import DataStorageField, DbObject
from src.models.consts import DATA_TYPES, DATEFROM, DATETO
from src.models.database import DatabaseTypeEnum
from src.models.database_object import DatabaseObjectGenerationResult, DbObjectTypeEnum
from src.models.types import SemanticDataTypeEnum
from src.repository.table.core import TableRepository
from src.repository.utils import get_database_object_names
from src.utils.backoff import RetryConfig, retry
from src.utils.validators import get_bool_from_str_or_bool, remove_parentheses_content

logger = EPMPYLogger(__name__)
DISTRIBUTED_SHARDING_KEY_PATTERN = re.compile(r"ENGINE\s*=\s*Distributed\s*\((.+)\)", re.IGNORECASE | re.DOTALL)


class TableClickhouseRepository(TableRepository):
    def _get_create_table_prefix(
        self, schema_name: str, table_name: str, cluster_name: Optional[str] = None, not_exists: bool = False
    ) -> str:
        """
        Создать первую часть запроса на создание таблицы.
        Возвращает: CREATE TABLE {table.schema_name}.{table.name} {on_cluster} (
        """
        on_cluster = f"ON CLUSTER {cluster_name} " if cluster_name else ""
        if_not_exists = " IF NOT EXISTS " if not_exists else " "
        create_table_sql = f"CREATE TABLE{if_not_exists}{schema_name}.{table_name} {on_cluster}( "
        return create_table_sql

    def _get_create_column_sql(self, field_name: str, field_type: str, default_value: Optional[str] = None) -> str:
        """
        Создать запрос на добавление колонки.
        """
        default_value = f" DEFAULT {default_value}" if default_value is not None else ""
        return f"ADD COLUMN IF NOT EXISTS `{field_name}` {field_type}{default_value}"

    async def _is_possible_to_drop_column(
        self,
        tables: list[DbObject],
        field_name: str,
    ) -> bool:
        """
        Проверка можно ли дропнуть поле (пустое ли оно)
        """
        possible_to_drop = True
        database_object_names = get_database_object_names(tables)
        if database_object_names.distributed_name is not None and self.database.default_cluster_name:
            select_sql = f'SELECT "{field_name}" FROM {database_object_names.distributed_schema}.{database_object_names.distributed_name} where isNotNull("{field_name}") LIMIT 1'
            possible_to_drop &= not bool(await self._get_data_query(select_sql))
            return possible_to_drop
        for table in tables:
            if (
                table.object_type in (DbObjectTypeEnum.TABLE, DbObjectTypeEnum.REPLICATED_TABLE)
                and not self.database.default_cluster_name
            ):
                select_sql = f'SELECT "{field_name}" FROM {table.schema_name}.{table.name} where isNotNull("{field_name}") LIMIT 1'
                possible_to_drop &= not bool(await self._get_data_query(select_sql))
            elif table.object_type in (DbObjectTypeEnum.TABLE, DbObjectTypeEnum.REPLICATED_TABLE):
                select_sql = f"SELECT * FROM cluster({self.database.default_cluster_name}, {table.schema_name}.{table.name}) LIMIT 1;"
                possible_to_drop &= not bool(await self._get_data_query(select_sql))
        return possible_to_drop

    def set_default_field_value(self, field: DataStorageField) -> str:
        """
        Конвертация семантического типа в технический.
        Например INTEGER -> Int64 DEFAULT 0
        """
        if field.sql_column_type is None:
            raise ValueError("Field has no sql_column_type. ")
        default_value = self.get_default_value_by_field(field)
        if default_value is not None:
            return f"{field.sql_column_type} DEFAULT {default_value}"
        else:
            return field.sql_column_type

    def _get_modify_column_sql(
        self, field_name: str, field_type: str, default_value: Optional[str] = None
    ) -> list[str]:
        """
        Создать запрос на изменение колонки.
        """
        default_value = f" DEFAULT {default_value}" if default_value is not None else default_value
        return [f'MODIFY COLUMN "{field_name}" {field_type}{default_value}']

    def _get_create_usual_table_sql(
        self,
        table: DbObject,
        fields: list[DataStorageField],
        not_exists: bool = False,
    ) -> str:
        """Получить sql запрос на создание обычной таблицы."""
        create_table_sql, primary_keys = self._get_create_table_request_sql_wihout_pk(
            table,
            fields,
            not_exists,
        )
        enable_replicated = get_bool_from_str_or_bool(
            getattr(settings, f"ENABLE_REPLICATED_TABLES_{self.database.name}".upper(), "true")
        )
        if table.object_type == DbObjectTypeEnum.REPLICATED_TABLE and enable_replicated:
            engine_type = f"ReplicatedMergeTree('/clickhouse/store/{table.schema_name}/{table.name}'" + ", '{shard}')"
        else:
            engine_type = "MergeTree()"
        if primary_keys:
            create_table_sql = create_table_sql[:-1] + f") ENGINE = {engine_type} ORDER BY ({','.join(primary_keys)})"
        else:
            create_table_sql = create_table_sql[:-1] + ") ENGINE = Log()"
        return create_table_sql

    def _create_dictionary_sql(
        self,
        table: DbObject,
        original_table_schema: str,
        original_table_name: str,
        fields: list[DataStorageField],
        not_exists: bool = False,
        replace: bool = False,
    ) -> str:
        """
        Генерирует DDL-запрос для создания словаря в ClickHouse на основе переданных данных.

        Args:
            data_storage (DataStorage): Хранилище данных (Orm-объект).
            database_object (DatabaseObjectModel): Модель целевого объекта базы данных (Pydantic).
            model_name (str): имя модели, в которой создана хранилище данных.
            cluster_name (Optional[str]): Необязательное имя кластера (для распределённых систем).
            not_exists (bool): Флаг добавления условия "IF NOT EXISTS".
            replace (bool): Флаг замены существующего словаря.

        Returns:
            str: Строка SQL-запроса для создания словаря
        """
        without_date = True
        for data_storage_field in fields:
            if data_storage_field.name in (DATEFROM, DATETO):
                without_date = False
        replace_sql = " OR REPLACE " if replace else " "
        on_cluster = f"ON CLUSTER {self.database.default_cluster_name} " if self.database.default_cluster_name else ""
        if_not_exists = " IF NOT EXISTS " if not_exists else " "
        create_sql = f"CREATE{replace_sql}DICTIONARY{if_not_exists}{table.schema_name}.{table.name} {on_cluster}("
        fields_sql, primary_keys = self._get_fields_and_pks(fields, True, without_date)
        create_sql += (
            fields_sql[:-1]
            + f""") PRIMARY KEY {",".join(primary_keys)} SOURCE(CLICKHOUSE(NAME 'self_localhost' """
            + f"""DB '{original_table_schema}' TABLE '{original_table_name}')) LIFETIME(MIN 0 MAX 1000) """
        )
        if without_date:
            create_sql += "LAYOUT(COMPLEX_KEY_HASHED())"
        else:
            create_sql += 'LAYOUT(RANGE_HASHED(range_lookup_strategy \'max\')) RANGE(MIN "datefrom" MAX "dateto");'
        return create_sql

    def _generate_sharding_key_by_fields(self, fields: list[DataStorageField]) -> str:
        """Создать ключ шардирования по полям."""
        sharding_fields = ""
        for field in fields:
            if field.is_sharding_key and not sharding_fields:
                sharding_fields += field.sql_name if field.sql_name else field.name
            elif field.is_sharding_key:
                sharding_fields += f", {field.sql_name}" if field.sql_name else field.name
        if sharding_fields:
            sharding_key = f"cityHash64({sharding_fields})"
        else:
            sharding_key = "rand()"
        return sharding_key

    def _get_create_distributed_table_sql(
        self,
        table: DbObject,
        fields: list[DataStorageField],
        original_table_schema: str,
        original_table_name: str,
        not_exists: bool = False,
    ) -> str:
        """Формирование запроса на создание дистрибутивной таблицы."""
        if self.database.default_cluster_name is None:
            raise ValueError("cluster_name not found!")
        sharding_key = self._generate_sharding_key_by_fields(fields)
        if_not_exists = " IF NOT EXISTS " if not_exists else " "
        fields_sql, _ = self._get_fields_and_pks(fields)
        return (
            f"CREATE TABLE{if_not_exists}{table.schema_name}.{table.name} ON CLUSTER {self.database.default_cluster_name} ({fields_sql[:-1]})"
            + f" ENGINE = Distributed({self.database.default_cluster_name}, {original_table_schema}, {original_table_name}, {sharding_key})"
        )

    def get_create_tables_sql_by_tables_and_fields(
        self,
        tables: list[DbObject],
        fields: list[DataStorageField],
        not_exists: bool = False,
    ) -> tuple[list[str], list[DbObject]]:
        """
        Создать запрос создание таблицы.
        """
        create_table_sqls: tuple[list[str], list[DbObject]] = ([], [])
        cluster_name = self.database.default_cluster_name
        database_object_names = get_database_object_names(tables)
        for table in tables:
            if table.object_type in (DbObjectTypeEnum.TABLE, DbObjectTypeEnum.REPLICATED_TABLE):
                create_table_sql = self._get_create_usual_table_sql(
                    table,
                    fields,
                    not_exists,
                )
                create_table_sqls[1].append(table)
                create_table_sqls[0].append(create_table_sql)
            elif (
                table.object_type == DbObjectTypeEnum.DISTRIBUTED_TABLE
                and cluster_name
                and database_object_names.table_schema
                and database_object_names.table_name
            ):
                distributed_sql = self._get_create_distributed_table_sql(
                    table,
                    fields,
                    database_object_names.table_schema,
                    database_object_names.table_name,
                    not_exists,
                )
                create_table_sqls[1].append(table)
                create_table_sqls[0].append(distributed_sql)
            elif table.object_type == DbObjectTypeEnum.DISTRIBUTED_TABLE and not cluster_name:
                raise ValueError("cluster_name not found!")
            elif (
                table.object_type == DbObjectTypeEnum.DICTIONARY
                and database_object_names.table_schema
                and database_object_names.table_name
            ):
                dictionary_sql = self._create_dictionary_sql(
                    table,
                    database_object_names.table_schema,
                    database_object_names.table_name,
                    fields,
                    not_exists,
                )
                create_table_sqls[1].append(table)
                create_table_sqls[0].append(dictionary_sql)
        return create_table_sqls

    async def _is_exist_table(
        self,
        table: DbObject,
    ) -> bool:
        try:
            if (
                table.object_type == DbObjectTypeEnum.DISTRIBUTED_TABLE is not None
                and self.database.default_cluster_name
            ):
                select_sql = f"SELECT * FROM {table.schema_name}.{table.name} LIMIT 1;"
                _ = not bool(await self._get_data_query(select_sql))
                return True
            if not self.tenant_id:
                raise ValueError("The Table model has no tenant. Are you reading it from the cache?")
            if (
                table.object_type in (DbObjectTypeEnum.TABLE, DbObjectTypeEnum.REPLICATED_TABLE)
                and not self.database.default_cluster_name
            ):
                select_sql = f"SELECT * FROM {table.schema_name}.{table.name} LIMIT 1"
                _ = not bool(await self._get_data_query(select_sql))
            elif table.object_type in (DbObjectTypeEnum.TABLE, DbObjectTypeEnum.REPLICATED_TABLE):
                select_sql = f"SELECT * FROM cluster({self.database.default_cluster_name}, {table.schema_name}.{table.name}) LIMIT 1;"
                _ = not bool(await self._get_data_query(select_sql))
        except Exception as exc:
            if "(UNKNOWN_TABLE)" in str(exc):
                return False
        return True

    def _get_drop_table_sql(self, table: DbObject, exists: bool = True) -> str:
        """Получить sql запрос для удаления таблицы."""
        on_cluster = f"ON CLUSTER {self.database.default_cluster_name}" if self.database.default_cluster_name else ""
        if_exists = "IF EXISTS" if exists else ""
        is_sync = " SYNC" if settings.SYNC_CLICKHOUSE_DROP else ""
        return f"DROP TABLE {if_exists} {table.schema_name}.{table.name} {on_cluster}{is_sync}"

    def _get_drop_dictionary_sql(self, table: DbObject, exists: bool = True) -> str:
        """Получить sql запрос для удаления словаря."""
        on_cluster = f"ON CLUSTER {self.database.default_cluster_name}" if self.database.default_cluster_name else ""
        if_exists = " IF EXISTS " if exists else " "
        is_sync = " SYNC" if settings.SYNC_CLICKHOUSE_DROP else ""
        return f"DROP DICTIONARY{if_exists}{table.schema_name}.{table.name} {on_cluster}{is_sync}"

    def _get_delete_tables_sql(self, tables: list[DbObject], exists: bool = True) -> tuple[list[str], list[DbObject]]:
        """
        Создать запрос на удаление таблицы.
        """
        sql_expressions = []
        for table in tables:
            if table.object_type == DbObjectTypeEnum.DISTRIBUTED_TABLE and not self.database.default_cluster_name:
                raise ValueError("cluster_name not found!")
            if table.object_type in (DbObjectTypeEnum.TABLE, DbObjectTypeEnum.REPLICATED_TABLE):
                sql_expressions.append(self._get_drop_table_sql(table, exists))
            elif table.object_type == DbObjectTypeEnum.DISTRIBUTED_TABLE:
                sql_expressions.insert(0, self._get_drop_table_sql(table, exists))
            elif table.object_type == DbObjectTypeEnum.DICTIONARY:
                sql_expressions.insert(0, self._get_drop_dictionary_sql(table))
        return sql_expressions, tables

    async def _is_exist_tables(
        self,
        tables: list[DbObject],
    ) -> bool:
        result = True
        for table in tables:
            result &= await self._is_exist_table(table)
        return result

    async def _is_possible_to_drop(self, tables: list[DbObject], allow_non_exist_tables: bool = False) -> bool:
        """
        Проверка можно ли дропнуть таблицу (пустая ли она)
        """
        try:
            possible_to_drop = True
            database_object_names = get_database_object_names(tables)
            if database_object_names.distributed_name is not None and self.database.default_cluster_name:
                select_sql = f"SELECT * FROM {database_object_names.distributed_schema}.{database_object_names.distributed_name} LIMIT 1;"
                possible_to_drop &= not bool(await self._get_data_query(select_sql))
                return possible_to_drop
            for table in tables:
                if (
                    table.object_type in (DbObjectTypeEnum.TABLE, DbObjectTypeEnum.REPLICATED_TABLE)
                    and not self.database.default_cluster_name
                ):
                    select_sql = f"SELECT * FROM {table.schema_name}.{table.name} LIMIT 1"
                    possible_to_drop &= not bool(await self._get_data_query(select_sql))
                elif table.object_type in (DbObjectTypeEnum.TABLE, DbObjectTypeEnum.REPLICATED_TABLE):
                    select_sql = f"SELECT * FROM cluster({self.database.default_cluster_name}, {table.schema_name}.{table.name}) LIMIT 1;"
                    possible_to_drop &= not bool(await self._get_data_query(select_sql))
        except Exception as exc:
            if "(UNKNOWN_TABLE)" in str(exc) and allow_non_exist_tables:
                return True
            raise Exception(exc)
        return possible_to_drop

    async def _execute_DDL(self, queries: str | list[str]) -> None:
        """
        Выполнение DDL запроса.
        Например: "CREATE", "ALTER TABLE", "DROP TABLE" и т.д
        """
        return await execute_raw_DDL(self.tenant_id, self.database, queries)

    @retry(RetryConfig(logger=None))
    async def _get_data_query(self, query: str, params: Optional[dict[Any, Any]] = None) -> list[Sequence[Any]]:
        """
        Выполнение запроса, который возвращает строки.
        Например: "SELECT", "UPDATE ... RETURNING ..." и т.д.
        """
        return await execute_raw_DQL_or_DML(self.tenant_id, self.database, query, params)

    async def _get_table_field_description(self, table_schema: str, table_name: str) -> dict[str, dict[str, Any]]:
        query = (
            "SELECT "
            "name AS column_name, "
            "type AS internal_data_type, "
            "CASE WHEN is_in_primary_key = 1 THEN true ELSE false END AS is_primary_key "
            "FROM system.columns "
            f"WHERE database = '{table_schema}' AND table = '{table_name}' "
            "ORDER BY position"
        )
        fields_description = await self._get_data_query(
            query,
        )
        result: dict[str, dict[str, Any]] = defaultdict(dict)
        for field_description in fields_description:
            result[field_description[0]].update({
                "data_type": (
                    remove_parentheses_content(field_description[1])
                    if DATA_TYPES[DatabaseTypeEnum.CLICKHOUSE][SemanticDataTypeEnum.TIMESTAMP] in field_description[1]
                    else field_description[1]
                ),
                "is_primary_key": field_description[2],
            })
        return result

    def extract_sharding_key(self, create_table_sql: str) -> Optional[str]:
        """
        Извлекает ключ шардирования из SQL определения Distributed таблицы ClickHouse.
        Возвращает строку с выражением ключа шардирования или None, если ключ не задан.
        """
        # Ищем ENGINE = Distributed(...) с параметрами в скобках
        match = re.search(DISTRIBUTED_SHARDING_KEY_PATTERN, create_table_sql)
        if not match:
            return None

        params_str = match.group(1).strip()

        # Разбиваем параметры по запятым, учитывая, что внутри параметров могут быть скобки
        params = []
        bracket_level = 0
        current_param = ""
        for ch in params_str:
            if ch == "(":
                bracket_level += 1
            elif ch == ")":
                bracket_level -= 1
            if ch == "," and bracket_level == 0:
                params.append(current_param)
                current_param = ""
            else:
                current_param += ch
        # Добавляем последний параметр
        if current_param:
            params.append(current_param)

        # Ключ шардирования - 4-й параметр (индекс 3), если есть
        if len(params) >= 4:
            sharding_key = params[3]
            if sharding_key:
                return sharding_key.strip()
        return None

    async def get_sharding_key_from_table(self, table_schema: str, table_name: str) -> Optional[str]:
        query = f"SHOW CREATE TABLE `{table_schema}`.`{table_name}`"
        sharding_result = await self._get_data_query(query)
        if sharding_result:
            sharding_key = self.extract_sharding_key(sharding_result[0][0])
            return sharding_key
        return None

    async def get_alter_tables_sql_expressions(
        self, tables: list[DbObject], fields: list[DataStorageField], sql_expressions: list[str]
    ) -> tuple[list[str], list[DbObject]]:
        """
        Собрать из сырых sql выражений (alter) запросы для clickhouse.

        Args:
            datastorage (DataStorage): datastorage, который хотим обновить
            model (Model): модель, к которой привязан datastorage
            sql_expressions (list[str]): сырые запросы

        Returns:
            list[str]: готовые запросы
        """
        result_tables: list[DbObject] = []
        database_objects_names = get_database_object_names(tables)
        if not database_objects_names.table_schema or not database_objects_names.table_name:
            raise ValueError(f"It is not possible to update a {tables} that does not have a table")
        on_cluster = (
            f" ON CLUSTER {self.database.default_cluster_name} "
            if self.database.default_cluster_name is not None
            else " "
        )
        alter_table_distr_query = ""
        recreate_distr = False
        distr_object = None
        for table in tables:
            if table.object_type == DbObjectTypeEnum.DISTRIBUTED_TABLE:
                distr_object = table
                break
        dictionary_object = None
        for table in tables:
            if table.object_type == DbObjectTypeEnum.DICTIONARY:
                dictionary_object = table
                break
        table_object = None
        for table in tables:
            if table.object_type == DbObjectTypeEnum.TABLE:
                table_object = table
                break
        alter_table_query = (
            f"ALTER TABLE {database_objects_names.table_schema}.{database_objects_names.table_name}{on_cluster}"
        )
        if distr_object:
            alter_table_distr_query = f"ALTER TABLE {distr_object.schema_name}.{distr_object.name}{on_cluster}"
        execute_distr = False
        execute_table = False
        sql_expressions_for_execute = []
        if distr_object and database_objects_names.distributed_schema and database_objects_names.distributed_name:
            sharding_key = await self.get_sharding_key_from_table(
                database_objects_names.distributed_schema, database_objects_names.distributed_name
            )
            current_sharding_key = self._generate_sharding_key_by_fields(fields)
            if sharding_key != current_sharding_key:
                recreate_distr = True
                sql_expressions_for_execute.append(self._get_drop_table_sql(distr_object, True))
                result_tables.append(distr_object)
        for sql_expression in sql_expressions:
            alter_table_query += f"{sql_expression},"
            execute_table = True
            if distr_object and not recreate_distr:
                alter_table_distr_query += f"{sql_expression},"
                execute_distr = True
        if execute_table and table_object:
            sql_expressions_for_execute.append(alter_table_query[:-1])
            result_tables.append(table_object)
        if execute_distr and distr_object:
            sql_expressions_for_execute.append(alter_table_distr_query[:-1])
            result_tables.append(distr_object)
        if dictionary_object and sql_expressions:
            sql_expressions_for_execute.append(
                self._create_dictionary_sql(
                    dictionary_object,
                    database_objects_names.table_schema,
                    database_objects_names.table_name,
                    fields,
                    replace=True,
                )
            )
            result_tables.append(dictionary_object)
        if distr_object and recreate_distr and self.database.default_cluster_name:
            sql_expressions_for_execute.append(
                self._get_create_distributed_table_sql(
                    distr_object,
                    fields,
                    database_objects_names.table_schema,
                    database_objects_names.table_name,
                    True,
                )
            )
            result_tables.append(distr_object)
        return sql_expressions_for_execute, result_tables

    async def recreate_dictionary(
        self,
        tables: list[DbObject],
        fields: list[DataStorageField],
    ) -> Optional[DatabaseObjectGenerationResult]:
        """
        Пересоздаёт словарь в базе данных при наличии соответствующих условий.

        Функция ищет объект базы данных типа 'DICTIONARY' в переданном хранилище данных.
        Если найден подходящий объект и модель с совпадающим именем, генерируется SQL-запрос
        для создания или замены словаря с параметрами NOT EXISTS и REPLACE. Затем запрос
        выполняется асинхронно через метод _execute_DDL.

        Args:
            data_storage (DataStorage): Хранилище данных, содержащее объекты базы данных.
            model_model (ModelModel): Модель, для которой необходимо пересоздать словарь.

        Returns:
            bool: True, если словарь успешно пересоздан; False, если условия не выполнены.
        """
        original_table = None
        dictionary_table = None
        for table in tables:
            if table.object_type in {DbObjectTypeEnum.TABLE, DbObjectTypeEnum.REPLICATED_TABLE}:
                original_table = table
            if table.object_type == DbObjectTypeEnum.DICTIONARY:
                dictionary_table = table
        if not dictionary_table or not original_table:
            return None
        result = DatabaseObjectGenerationResult(
            table=dictionary_table,
        )
        try:
            dictionary_sql = self._create_dictionary_sql(
                dictionary_table,
                original_table.schema_name,
                original_table.name,
                fields,
                not_exists=True,
                replace=True,
            )
            result.sql_expression = dictionary_sql
            result.executed = True
            await self._execute_DDL(dictionary_sql)
            logger.debug("Recreated dictionary: %s.%s", dictionary_table.schema_name, dictionary_table.name)
        except Exception as exc:
            logger.exception(
                "Error: recreating dictionary: %s.%s.", dictionary_table.schema_name, dictionary_table.name
            )
            result.error = str(exc)
        return result
