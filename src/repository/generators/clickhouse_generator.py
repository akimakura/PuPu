"""
Репозиторий, который генерирует объекты в базе данных ClickHouse
"""

import re
from collections import defaultdict
from typing import Any, Optional, Sequence

from clickhouse_connect.driver.exceptions import OperationalError
from py_common_lib.logger import EPMPYLogger
from py_common_lib.logger.audit_types import F9

from src.clickhouse.engine import get_client
from src.config import settings
from src.db.data_storage import DataStorage, DataStorageField
from src.db.database_object import DatabaseObject
from src.db.model import Model
from src.models.consts import DATA_TYPES, DATEFROM, DATETO
from src.models.database import Database as DatabaseModel, DatabaseTypeEnum
from src.models.database_object import DatabaseObject as DatabaseObjectModel, DbObjectTypeEnum
from src.models.dimension import DimensionTypeEnum
from src.models.measure import MeasureTypeEnum
from src.models.model import Model as ModelModel
from src.repository.generators.base_generator import GeneratorRepository
from src.repository.utils import (
    get_database_object_names,
    get_field_type_with_length,
    get_filtred_database_object_by_data_storage,
    get_ip_address_by_dns_name,
    get_object_filtred_by_model_name,
)
from src.utils.backoff import RetryConfig, retry
from src.utils.validators import get_bool_from_str_or_bool, remove_parentheses_content

logger = EPMPYLogger(__name__)

DISTRIBUTED_SHARDING_KEY_PATTERN = re.compile(r"ENGINE\s*=\s*Distributed\s*\((.+)\)", re.IGNORECASE | re.DOTALL)


class GeneratorClickhouseRepository(GeneratorRepository):
    """Генератор табличек в ClickHouse или совместимых баз"""

    @classmethod
    def _get_create_column_sql(cls, field_name: str, field_type: str, default_value: Optional[str] = None) -> str:
        """
        Создать запрос на добавление колонки.
        """
        default_value = f" DEFAULT {default_value}" if default_value is not None else ""
        return f"ADD COLUMN IF NOT EXISTS `{field_name}` {field_type}{default_value}"

    @classmethod
    def extract_sharding_key(cls, create_table_sql: str) -> Optional[str]:
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

    @classmethod
    async def get_sharding_key_from_table(
        cls, database: DatabaseModel, table_schema: str, table_name: str
    ) -> Optional[str]:
        query = f"SHOW CREATE TABLE `{table_schema}`.`{table_name}`"
        sharding_result = await cls._get_data_query(query, database)
        if sharding_result:
            sharding_key = cls.extract_sharding_key(sharding_result[0][0])
            return sharding_key
        return None

    @classmethod
    async def _is_exist_database_objects(
        cls,
        database_objects: list[DatabaseObjectModel] | list[DatabaseObjectModel],
        database: DatabaseModel,
    ) -> bool:
        result = True
        for database_object in database_objects:
            result &= await cls._is_exist_database_object(database_object, database)
        return result

    @classmethod
    async def _is_exist_database_object(
        cls,
        database_object: DatabaseObject | DatabaseObjectModel,
        database: DatabaseModel,
    ) -> bool:
        try:
            if database_object.type == DbObjectTypeEnum.DISTRIBUTED_TABLE is not None and database.default_cluster_name:
                select_sql = f"SELECT * FROM {database_object.schema_name}.{database_object.name} LIMIT 1;"
                _ = not bool(await cls._get_data_query(select_sql, database))
                return True
            if not database_object.tenant_id:
                raise ValueError("The Table model has no tenant. Are you reading it from the cache?")
            if (
                database_object.type in (DbObjectTypeEnum.TABLE, DbObjectTypeEnum.REPLICATED_TABLE)
                and not database.default_cluster_name
            ):
                select_sql = f"SELECT * FROM {database_object.schema_name}.{database_object.name} LIMIT 1"
                _ = not bool(await cls._get_data_query(select_sql, database))
            elif database_object.type in (DbObjectTypeEnum.TABLE, DbObjectTypeEnum.REPLICATED_TABLE):
                select_sql = f"SELECT * FROM cluster({database.default_cluster_name}, {database_object.schema_name}.{database_object.name}) LIMIT 1;"
                _ = not bool(await cls._get_data_query(select_sql, database))
        except Exception as exc:
            if "(UNKNOWN_TABLE)" in str(exc):
                return False
        return True

    @classmethod
    def _get_create_view_sql(
        cls, schema_name: str, name: str, sql_expression: str, cluster_name: Optional[str] = None, replace: bool = False
    ) -> list[str]:
        """Генерация DDL, который создает или пересоздает VIEW."""
        replace_sql = "OR REPLACE" if replace else ""
        on_cluster = f"ON CLUSTER {cluster_name}" if cluster_name is not None else ""
        result = f"CREATE {replace_sql} VIEW {schema_name}.{name} {on_cluster} AS ({sql_expression})"
        return [result]

    @classmethod
    def _get_delete_view_sql(cls, schema_name: str, name: str, cluster_name: Optional[str] = None) -> str:
        """Генерация DDL, который удаляет VIEW."""
        on_cluster = f"ON CLUSTER {cluster_name}" if cluster_name is not None else ""
        is_sync = " SYNC" if settings.SYNC_CLICKHOUSE_DROP else ""
        return f"DROP VIEW IF EXISTS {schema_name}.{name} {on_cluster}{is_sync}"

    @classmethod
    def _get_table_field_type(
        cls, field: DataStorageField, db_type: DatabaseTypeEnum, without_null: bool = False, with_precision: bool = True
    ) -> str:
        """
        Конвертация семантического типа в технический.
        Например INTEGER -> Int64 DEFAULT 0
        """
        precision, scale, field_type = get_field_type_with_length(field)
        if field_type == MeasureTypeEnum.DECIMAL:
            result_field_type = DATA_TYPES[db_type][field_type] + (f"({precision},{scale})" if with_precision else "")
        else:
            result_field_type = DATA_TYPES[db_type][field_type]
        if without_null:
            return result_field_type
        default_value = cls.get_default_value_by_field(field, db_type)
        if default_value is not None:
            return f"{result_field_type} DEFAULT {default_value}"
        else:
            return result_field_type

    @classmethod
    def _get_drop_table_sql(
        cls, database_object: DatabaseObjectModel, cluster_name: Optional[str] = None, exists: bool = True
    ) -> str:
        """Получить sql запрос для удаления таблицы."""
        on_cluster = f"ON CLUSTER {cluster_name}" if cluster_name else ""
        if_exists = "IF EXISTS" if exists else ""
        is_sync = " SYNC" if settings.SYNC_CLICKHOUSE_DROP else ""
        return f"DROP TABLE {if_exists} {database_object.schema_name}.{database_object.name} {on_cluster}{is_sync}"

    @classmethod
    def _get_drop_dictionary_sql(
        cls, database_object: DatabaseObjectModel, cluster_name: Optional[str] = None, exists: bool = True
    ) -> str:
        """Получить sql запрос для удаления словаря."""
        on_cluster = f"ON CLUSTER {cluster_name}" if cluster_name else ""
        if_exists = " IF EXISTS " if exists else " "
        is_sync = " SYNC" if settings.SYNC_CLICKHOUSE_DROP else ""
        return f"DROP DICTIONARY{if_exists}{database_object.schema_name}.{database_object.name} {on_cluster}{is_sync}"

    @classmethod
    def _get_delete_db_objects_sql(
        cls, database_objects: list[DatabaseObjectModel], cluster_name: Optional[str] = None, exists: bool = True
    ) -> list[str]:
        """
        Создать запрос на удаление таблицы.
        """
        sql_expressions = []
        for database_object in database_objects:
            if database_object.type == DbObjectTypeEnum.DISTRIBUTED_TABLE and not cluster_name:
                raise ValueError("cluster_name not found!")
            if database_object.type in (DbObjectTypeEnum.TABLE, DbObjectTypeEnum.REPLICATED_TABLE):
                sql_expressions.append(cls._get_drop_table_sql(database_object, cluster_name, True))
            elif database_object.type == DbObjectTypeEnum.DISTRIBUTED_TABLE:
                sql_expressions.insert(0, cls._get_drop_table_sql(database_object, cluster_name, True))
            elif database_object.type == DbObjectTypeEnum.DICTIONARY:
                sql_expressions.insert(0, cls._get_drop_dictionary_sql(database_object, cluster_name, True))
        return sql_expressions

    @classmethod
    @retry(RetryConfig())
    async def _execute_DDL(cls, queries: str | list[str], database: DatabaseModel) -> None:
        """
        Выполнение DDL запроса.
        Например: "CREATE", "ALTER TABLE", "DROP TABLE" и т.д
        """
        try:
            client = await get_client(database)
            if isinstance(queries, list):
                for query in queries:
                    logger.debug("""EXECUTE QUERY: "%s";""", query)
                    await client.query(query)
            else:
                logger.debug("""EXECUTE QUERY: "%s";""", queries)
                await client.query(queries)
            return None
        except OperationalError as ext:
            ip_address = get_ip_address_by_dns_name(database.connections[0].host)
            logger.audit(F9, host=ip_address, dns_name=database.connections[0].host)
            raise OperationalError(str(ext))
        except ConnectionRefusedError as ext:
            ip_address = get_ip_address_by_dns_name(database.connections[0].host)
            logger.audit(F9, host=ip_address, dns_name=database.connections[0].host)
            raise ConnectionRefusedError(str(ext))
        except Exception as ext:
            logger.error("Error executing DDL: %s", query)
            raise Exception(str(ext))

    @classmethod
    @retry(RetryConfig(logger=None))
    async def _get_data_query(
        cls, query: str, database: DatabaseModel, params: Optional[dict] = None
    ) -> list[Sequence[Any]]:
        """
        Выполнение запроса, который возвращает строки.
        Например: "SELECT", "UPDATE ... RETURNING ..." и т.д.
        """
        try:
            client = await get_client(database)
            logger.debug("""EXECUTE QUERY: "%s";""", query)
            result_ch = await client.query(query)
            return list(result_ch.result_rows)
        except OperationalError as ext:
            ip_address = get_ip_address_by_dns_name(database.connections[0].host)
            logger.audit(F9, host=ip_address, dns_name=database.connections[0].host)
            raise OperationalError(str(ext))
        except ConnectionRefusedError as ext:
            ip_address = get_ip_address_by_dns_name(database.connections[0].host)
            logger.audit(F9, host=ip_address, dns_name=database.connections[0].host)
            raise ConnectionRefusedError(str(ext))
        except Exception as exc:
            if "(UNKNOWN_TABLE)" in str(exc):
                logger.warning("Unknown table")
            else:
                logger.exception("Error execute query: %s", query)
            raise Exception(exc)

    @classmethod
    async def find_views_by_table(
        cls, database: DatabaseModel, schema_name: str, table_names: list[str]
    ) -> list[dict[str, str]]:
        """Ищет представления по списку таблиц через system.tables."""
        if not table_names:
            return []
        safe_schema = schema_name.replace("'", "''")
        safe_tables = [table_name.replace("'", "''") for table_name in table_names]
        like_conditions = " OR ".join([f"create_table_query LIKE '%{table}%'" for table in safe_tables])
        query = (
            "SELECT database AS view_schema, name AS view_name, create_table_query AS view_definition "
            "FROM system.tables "
            "WHERE engine = 'View' "
            f"AND ({like_conditions}) "
            f"AND database = '{safe_schema}'"
        )
        rows = await cls._get_data_query(query, database)
        return [
            {
                "view_schema": row[0],
                "view_name": row[1],
                "view_definition": row[2],
            }
            for row in rows
        ]

    @classmethod
    async def _is_possible_to_drop_column(
        cls,
        database_objects: list[DatabaseObjectModel],
        database: DatabaseModel,
        field_name: str,
    ) -> bool:
        """
        Проверка можно ли дропнуть поле (пустое ли оно)
        """
        possible_to_drop = True
        database_object_names = get_database_object_names(database_objects)
        if database_object_names.distributed_name is not None and database.default_cluster_name:
            select_sql = f'SELECT "{field_name}" FROM {database_object_names.distributed_schema}.{database_object_names.distributed_name} where isNotNull("{field_name}") LIMIT 1'
            possible_to_drop &= not bool(await cls._get_data_query(select_sql, database))
            return possible_to_drop
        for database_object in database_objects:
            if (
                database_object.type in (DbObjectTypeEnum.TABLE, DbObjectTypeEnum.REPLICATED_TABLE)
                and not database.default_cluster_name
            ):
                select_sql = f'SELECT "{field_name}" FROM {database_object.schema_name}.{database_object.name} where isNotNull("{field_name}") LIMIT 1'
                possible_to_drop &= not bool(await cls._get_data_query(select_sql, database))
            elif database_object.type in (DbObjectTypeEnum.TABLE, DbObjectTypeEnum.REPLICATED_TABLE):
                select_sql = f"SELECT * FROM cluster({database.default_cluster_name}, {database_object.schema_name}.{database_object.name}) LIMIT 1;"
                possible_to_drop &= not bool(await cls._get_data_query(select_sql, database))
        return possible_to_drop

    @classmethod
    def _get_modify_column_sql(cls, field_name: str, field_type: str, default_value: Optional[str] = None) -> list[str]:
        """
        Создать запрос на изменение колонки.
        """
        default_value = f" DEFAULT {default_value}" if default_value is not None else default_value
        return [f'MODIFY COLUMN "{field_name}" {field_type}{default_value}']

    @classmethod
    def _get_create_table_prefix(
        cls, database_object: DatabaseObjectModel, cluster_name: Optional[str] = None, not_exists: bool = False
    ) -> str:
        """
        Создать первую часть запроса на создание таблицы.
        Возвращает: CREATE TABLE {table.schema_name}.{table.name} {on_cluster} (
        """
        on_cluster = f"ON CLUSTER {cluster_name} " if cluster_name else ""
        if_not_exists = " IF NOT EXISTS " if not_exists else " "
        create_table_sql = (
            f"CREATE TABLE{if_not_exists}{database_object.schema_name}.{database_object.name} {on_cluster}( "
        )
        return create_table_sql

    @classmethod
    def _create_dictionary_sql(
        cls,
        data_storage: DataStorage,
        database_object: DatabaseObjectModel,
        model_name: str,
        cluster_name: Optional[str] = None,
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
        for data_storage_field in data_storage.fields:
            if data_storage_field.name in (DATEFROM, DATETO):
                without_date = False
        database_objects = get_object_filtred_by_model_name(data_storage.database_objects, model_name, True)
        database_objects_names = get_database_object_names(database_objects)
        table_name = database_objects_names.table_name
        table_schema = database_objects_names.table_schema
        replace_sql = " OR REPLACE " if replace else " "
        on_cluster = f"ON CLUSTER {cluster_name} " if cluster_name else ""
        if_not_exists = " IF NOT EXISTS " if not_exists else " "
        create_sql = f"CREATE{replace_sql}DICTIONARY{if_not_exists}{database_object.schema_name}.{database_object.name} {on_cluster}("
        fields_sql, primary_keys = cls.get_fields_and_pks(data_storage, DatabaseTypeEnum.CLICKHOUSE, True, without_date)
        create_sql += (
            fields_sql[:-1]
            + f""") PRIMARY KEY {','.join(primary_keys)} SOURCE(CLICKHOUSE(NAME 'self_localhost' """
            + f"""DB '{table_schema}' TABLE '{table_name}')) LIFETIME(MIN 0 MAX 1000) """
        )
        if without_date:
            create_sql += "LAYOUT(COMPLEX_KEY_HASHED())"
        else:
            create_sql += "LAYOUT(RANGE_HASHED(range_lookup_strategy 'max')) RANGE(MIN \"datefrom\" MAX \"dateto\");"
        return create_sql

    @classmethod
    async def recreate_dictionary(
        cls,
        data_storage: DataStorage,
        model_model: ModelModel,
    ) -> bool:
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
        dictionary_sql = None
        if model_model.database is None:
            raise ValueError("Database is not found!")
        for database_object in data_storage.database_objects:
            model_names = {database_object_model.name for database_object_model in database_object.models}
            if database_object.type == DbObjectTypeEnum.DICTIONARY and model_model.name in model_names:
                database_object_model = DatabaseObjectModel.model_validate(database_object)
                dictionary_sql = cls._create_dictionary_sql(
                    data_storage,
                    database_object_model,
                    model_model.name,
                    model_model.database.default_cluster_name,
                    not_exists=True,
                    replace=True,
                )
                break
        if dictionary_sql:
            await cls._execute_DDL(dictionary_sql, model_model.database)
            logger.debug("Recreated dictionary for: %s", data_storage.name)
            return True
        return False

    @classmethod
    def _get_create_distributed_table_sql(
        cls,
        data_storage: DataStorage,
        database_object: DatabaseObject | DatabaseObjectModel,
        table_schema: str,
        table_name: str,
        cluster_name: str,
        not_exists: bool = False,
    ) -> str:
        """Формирование запроса на создание дистрибутивной таблицы."""
        if not data_storage.sharding_key:
            raise ValueError("Sharding key may not be NULL")
        if_not_exists = " IF NOT EXISTS " if not_exists else " "
        fields_sql, primary_keys = cls.get_fields_and_pks(data_storage, DatabaseTypeEnum.CLICKHOUSE)
        return (
            f"CREATE TABLE{if_not_exists}{database_object.schema_name}.{database_object.name} ON CLUSTER {cluster_name} ({fields_sql[:-1]})"
            + f" ENGINE = Distributed({cluster_name}, {table_schema}, {table_name}, {data_storage.sharding_key})"
        )

    @classmethod
    def _get_create_usual_table_sql(
        cls,
        data_storage: DataStorage,
        database_object: DatabaseObjectModel,
        database_model: DatabaseModel,
        not_exists: bool = False,
        dimension_tech_fields: bool = False,
    ) -> str:
        """Получить sql запрос на создание обычной таблицы."""
        create_table_sql, primary_keys = cls._get_create_table_request_sql_wihout_pk(
            data_storage, database_object, database_model, not_exists, dimension_tech_fields
        )
        enable_replicated = get_bool_from_str_or_bool(
            getattr(settings, f"ENABLE_REPLICATED_TABLES_{database_model.name}".upper(), "true")
        )
        if database_object.type == DbObjectTypeEnum.REPLICATED_TABLE and enable_replicated:
            engine_type = (
                f"ReplicatedMergeTree('/clickhouse/store/{database_object.schema_name}/{database_object.name}'"
                + ", '{shard}')"
            )
        else:
            engine_type = "MergeTree()"
        if primary_keys:
            create_table_sql = create_table_sql[:-1] + f") ENGINE = {engine_type} ORDER BY ({','.join(primary_keys)})"
        else:
            create_table_sql = create_table_sql[:-1] + ") ENGINE = Log()"
        return create_table_sql

    @classmethod
    def _get_create_table_sql(
        cls,
        data_storage: DataStorage,
        database_model: DatabaseModel,
        model_name: str,
        not_exists: bool = False,
        dimension_tech_fields: bool = False,
    ) -> list[str]:
        """
        Создать запрос создание таблицы.
        """
        create_table_sqls = []
        cluster_name = database_model.default_cluster_name
        database_objects = get_object_filtred_by_model_name(data_storage.database_objects, model_name, True)
        database_objects_model = [
            DatabaseObjectModel.model_validate(database_object) for database_object in database_objects
        ]
        database_object_names = get_database_object_names(database_objects_model)
        for database_object in database_objects_model:
            if database_object.type in (DbObjectTypeEnum.TABLE, DbObjectTypeEnum.REPLICATED_TABLE):
                create_table_sql = cls._get_create_usual_table_sql(
                    data_storage, database_object, database_model, not_exists, dimension_tech_fields
                )
                create_table_sqls.append(create_table_sql)
            elif (
                database_object.type == DbObjectTypeEnum.DISTRIBUTED_TABLE
                and cluster_name
                and database_object_names.table_schema
                and database_object_names.table_name
            ):
                distributed_sql = cls._get_create_distributed_table_sql(
                    data_storage,
                    database_object,
                    database_object_names.table_schema,
                    database_object_names.table_name,
                    cluster_name,
                    not_exists,
                )
                create_table_sqls.append(distributed_sql)
            elif database_object.type == DbObjectTypeEnum.DISTRIBUTED_TABLE and not cluster_name:
                raise ValueError("cluster_name not found!")
            elif database_object.type == DbObjectTypeEnum.DICTIONARY:
                dictionary_sql = cls._create_dictionary_sql(
                    data_storage, database_object, model_name, cluster_name, not_exists
                )
                create_table_sqls.append(dictionary_sql)
        return create_table_sqls

    @classmethod
    async def _is_possible_to_drop(
        cls, database_objects: list[DatabaseObjectModel], database: DatabaseModel, allow_non_exist_tables: bool = False
    ) -> bool:
        """
        Проверка можно ли дропнуть таблицу (пустая ли она)
        """
        try:
            possible_to_drop = True
            database_object_names = get_database_object_names(database_objects)
            if database_object_names.distributed_name is not None and database.default_cluster_name:
                select_sql = f"SELECT * FROM {database_object_names.distributed_schema}.{database_object_names.distributed_name} LIMIT 1;"
                possible_to_drop &= not bool(await cls._get_data_query(select_sql, database))
                return possible_to_drop
            for database_object in database_objects:
                if not database_object.tenant_id:
                    raise ValueError("The Table model has no tenant. Are you reading it from the cache?")
                if (
                    database_object.type in (DbObjectTypeEnum.TABLE, DbObjectTypeEnum.REPLICATED_TABLE)
                    and not database.default_cluster_name
                ):
                    select_sql = f"SELECT * FROM {database_object.schema_name}.{database_object.name} LIMIT 1"
                    possible_to_drop &= not bool(await cls._get_data_query(select_sql, database))
                elif database_object.type in (DbObjectTypeEnum.TABLE, DbObjectTypeEnum.REPLICATED_TABLE):
                    select_sql = f"SELECT * FROM cluster({database.default_cluster_name}, {database_object.schema_name}.{database_object.name}) LIMIT 1;"
                    possible_to_drop &= not bool(await cls._get_data_query(select_sql, database))
        except Exception as exc:
            if "(UNKNOWN_TABLE)" in str(exc) and allow_non_exist_tables:
                return True
            raise Exception(exc)
        return possible_to_drop

    @classmethod
    async def _get_table_field_description(
        cls, database: DatabaseModel, schema_name: str, table_name: str
    ) -> dict[str, dict[str, Any]]:
        query = (
            "SELECT "
            "name AS column_name, "
            "type AS internal_data_type, "
            "CASE WHEN is_in_primary_key = 1 THEN true ELSE false END AS is_primary_key "
            "FROM system.columns "
            f"WHERE database = '{schema_name}' AND table = '{table_name}' "
            "ORDER BY position"
        )
        fields_description = await cls._get_data_query(
            query,
            database,
        )
        result: dict[str, dict[str, Any]] = defaultdict(dict)
        for field_description in fields_description:
            result[field_description[0]].update(
                {
                    "data_type": (
                        remove_parentheses_content(field_description[1])
                        if DATA_TYPES[DatabaseTypeEnum.CLICKHOUSE][DimensionTypeEnum.TIMESTAMP] in field_description[1]
                        else field_description[1]
                    ),
                    "is_primary_key": field_description[2],
                }
            )
        return result

    @classmethod
    async def get_alter_database_objects_sql_expressions(
        cls, datastorage: DataStorage, model: Model, sql_expressions: list[str]
    ) -> list[str]:
        """
        Собрать из сырых sql выражений (alter) запросы для clickhouse.

        Args:
            datastorage (DataStorage): datastorage, который хотим обновить
            model (Model): модель, к которой привязан datastorage
            sql_expressions (list[str]): сырые запросы

        Returns:
            list[str]: готовые запросы
        """
        database = DatabaseModel.model_validate(model.database)
        database_objects = get_filtred_database_object_by_data_storage(datastorage, model.name)
        database_objects_names = get_database_object_names(database_objects)
        if not database_objects_names.table_schema or not database_objects_names.table_name:
            raise ValueError(
                f"It is not possible to update a datastorage {datastorage.name} that does not have a table"
            )
        on_cluster = (
            f" ON CLUSTER {database.default_cluster_name} " if database.default_cluster_name is not None else " "
        )
        recreate_distr = False
        distr_object = None
        for database_object in database_objects:
            if database_object.type == DbObjectTypeEnum.DISTRIBUTED_TABLE:
                distr_object = database_object
                break
        dictionary_object = None
        for database_object in database_objects:
            if database_object.type == DbObjectTypeEnum.DICTIONARY:
                dictionary_object = database_object
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
            sharding_key = await cls.get_sharding_key_from_table(
                database, database_objects_names.distributed_schema, database_objects_names.distributed_name
            )
            if sharding_key != datastorage.sharding_key:
                recreate_distr = True
                sql_expressions_for_execute.append(
                    cls._get_drop_table_sql(distr_object, database.default_cluster_name, True)
                )
        for sql_expression in sql_expressions:
            alter_table_query += f"{sql_expression},"
            execute_table = True
            if distr_object and not recreate_distr:
                alter_table_distr_query += f"{sql_expression},"
                execute_distr = True
        if execute_table:
            sql_expressions_for_execute.append(alter_table_query[:-1])
        if execute_distr:
            sql_expressions_for_execute.append(alter_table_distr_query[:-1])
        if dictionary_object and sql_expressions:
            sql_expressions_for_execute.append(
                cls._create_dictionary_sql(
                    datastorage, dictionary_object, model.name, database.default_cluster_name, replace=True
                )
            )
        if distr_object and recreate_distr and database.default_cluster_name:
            sql_expressions_for_execute.append(
                cls._get_create_distributed_table_sql(
                    datastorage,
                    distr_object,
                    database_objects_names.table_schema,
                    database_objects_names.table_name,
                    database.default_cluster_name,
                    True,
                )
            )
        return sql_expressions_for_execute
