from typing import Any, Optional, Sequence

from py_common_lib.logger import EPMPYLogger

from src.config import settings
from src.integrations.modelling_tools_api.codegen import Database, DataStorageField, DbObject
from src.models.consts import (
    DATA_TYPES,
    DATEFROM,
    DATETO,
    DEFAULT_DATE_FROM,
    DEFAULT_DATE_TO,
    DEFAULT_TYPE_VALUES,
    TIMESTAMP,
)
from src.models.database_object import DatabaseObjectGenerationResult, DbObjectTypeEnum
from src.models.dimension import TechDimensionEnum
from src.models.types import SemanticDataTypeEnum
from src.repository.utils import get_database_object_names
from src.utils.backoff import RetryConfig, retry
from src.utils.validators import remove_parentheses_content

logger = EPMPYLogger(__name__)


class TableRepository:
    def __init__(self, tenant_id: str, database: Database):
        self.tenant_id = tenant_id
        self.database = database

    async def _is_possible_to_drop(self, tables: list[DbObject], allow_non_exist_tables: bool = False) -> bool:
        """
        Проверка можно ли дропнуть таблицы (пустая ли она)
        """
        raise NotImplementedError

    async def get_alter_tables_sql_expressions(
        self, tables: list[DbObject], fields: list[DataStorageField], sql_expressions: list[str]
    ) -> tuple[list[str], list[DbObject]]:
        """
        Собрать из сырых sql выражений (alter) запросы для конкретной базы данных.

        Args:
            datastorage (DataStorage): datastorage, который хотим обновить
            model (Model): модель, к которой привязан datastorage
            sql_expressions (list[str]): сырые запросы

        Returns:
            list[str]: готовые запросы
        """
        raise NotImplementedError

    async def recreate_dictionary(
        self,
        tables: list[DbObject],
        fields: list[DataStorageField],
    ) -> Optional[DatabaseObjectGenerationResult]:
        raise NotImplementedError

    @classmethod
    def _get_drop_column_sql(cls, field_name: str) -> str:
        """
        Создать запрос на удаление колонки.
        """
        return f"DROP COLUMN IF EXISTS `{field_name}`"

    def _get_create_column_sql(self, field_name: str, field_type: str, default_value: Optional[str] = None) -> str:
        """
        Создать запрос на добавление колонки.
        """
        raise NotImplementedError

    async def _get_table_field_description(self, table_schema: str, table_name: str) -> dict[str, dict[str, Any]]:
        raise NotImplementedError

    def _get_modify_column_sql(
        self, field_name: str, field_type: str, default_value: Optional[str] = None
    ) -> list[str]:
        """
        Создать запрос на изменение колонки.
        """
        raise NotImplementedError

    async def _is_possible_to_drop_column(
        self,
        tables: list[DbObject],
        field_name: str,
    ) -> bool:
        """
        Проверка можно ли дропнуть поле (пустое ли оно)
        """
        raise NotImplementedError

    def _get_delete_tables_sql(self, tables: list[DbObject], exists: bool = True) -> tuple[list[str], list[DbObject]]:
        """
        Создать запрос на удаление таблицы.
        """
        raise NotImplementedError

    async def _get_delete_tables_sql_and_check_possible_delete(
        self,
        tables: list[DbObject],
        exists: bool = False,
        check_possible_delete: bool = True,
    ) -> tuple[list[str], list[DbObject]]:
        """
        Получить запрос на удаление таблицы и проверить возможность её удаления.
        """
        sql_expressions, tables = self._get_delete_tables_sql(tables, exists=exists)
        logger.debug("SQL_EXPRESSIONS created. sql_expressions='%s'", sql_expressions)
        if check_possible_delete and not await self._is_possible_to_drop(tables, True):
            raise ValueError("It is not possible to delete a non-empty table.")
        return sql_expressions, tables

    async def _is_exist_tables(
        self,
        tables: list[DbObject],
    ) -> bool:
        raise NotImplementedError

    async def _execute_DDL(self, queries: str | list[str]) -> None:
        """
        Выполнение DDL запроса.
        Например: "CREATE", "ALTER TABLE", "DROP TABLE" и т.д
        """
        raise NotImplementedError

    def get_default_value_by_field(self, field: DataStorageField) -> Optional[str]:
        """Возвращает дефолтное значение для поля."""
        if field.sql_column_type is None:
            raise ValueError("Field has no sql_column_type. ")
        field_type = remove_parentheses_content(field.sql_column_type)
        if field.name == DATEFROM and field_type == DATA_TYPES[self.database.type][SemanticDataTypeEnum.DATE]:
            return f"'{DEFAULT_DATE_FROM}'"
        elif (
            field.name == TechDimensionEnum.IS_ACTIVE_DIMENSION
            and field_type == DATA_TYPES[self.database.type][SemanticDataTypeEnum.BOOLEAN]
        ):
            return "true"
        elif field_type in (
            DATA_TYPES[self.database.type][SemanticDataTypeEnum.UUID],
            DATA_TYPES[self.database.type][SemanticDataTypeEnum.TIMESTAMP],
        ):
            return f"{DEFAULT_TYPE_VALUES[self.database.type][field_type]}"
        elif field.name == DATETO and field_type == DATA_TYPES[self.database.type][SemanticDataTypeEnum.DATE]:
            return f"'{DEFAULT_DATE_TO}'"
        elif not field.is_key:
            return f"{DEFAULT_TYPE_VALUES[self.database.type][field_type]}"
        else:
            return None

    async def is_possible_to_drop_tables(self, tables: list[DbObject]) -> bool:
        """
        Проверяет, возможно ли удалить хранилище данных.

        Args:
            tables (list[DbObject]): таблицы для удаления.
        Returns:
            bool: True, если удаление возможно, иначе False.
        """
        possible_to_drop = await self._is_possible_to_drop(tables, True)
        if possible_to_drop:
            logger.debug("Tables %s is possible to drop", tables)
        return possible_to_drop

    @retry(RetryConfig())
    async def _get_data_query(self, query: str, params: Optional[dict[Any, Any]] = None) -> list[Sequence[Any]]:
        """
        Выполнение запроса, который возвращает строки.
        Например: "SELECT", "UPDATE ... RETURNING ..." и т.д.
        """
        raise NotImplementedError

    def sort_tables(self, arr: list[DbObject], reverse: bool = False) -> list[DbObject]:
        if not reverse:
            return sorted(arr, key=lambda db_object: 0 if db_object.object_type == DbObjectTypeEnum.TABLE else 1)
        return sorted(arr, key=lambda db_object: 1 if db_object.object_type == DbObjectTypeEnum.TABLE else 0)

    def get_create_tables_sql_by_tables_and_fields(
        self,
        tables: list[DbObject],
        fields: list[DataStorageField],
        not_exists: bool = False,
    ) -> tuple[list[str], list[DbObject]]:
        """
        Создать запрос создание таблицы.
        """
        raise NotImplementedError

    def _get_create_table_prefix(
        self, schema_name: str, table_name: str, cluster_name: Optional[str] = None, not_exists: bool = False
    ) -> str:
        """
        Создать первую часть запроса на создание таблицы.
        Возвращает строку вида "CREATE TABLE {table.schema_name}.{table.name} {on_cluster} (".
        """
        raise NotImplementedError

    def set_default_field_value(
        self,
        field: DataStorageField,
    ) -> str:
        raise NotImplementedError

    def _get_fields_and_pks(
        self,
        fields: list[DataStorageField],
        ignore_functions: bool = False,
        without_date: bool = True,
    ) -> tuple[str, list[str]]:
        """
        Возвращает поля вместе с типами и все первичные ключи
        """
        primary_keys = []
        fields_str = ""
        if not fields:
            raise ValueError("You cannot create a table without fields.")
        for field in fields:
            if not field.sql_column_type:
                raise ValueError(f"Field {field.name} has no sql_column_type")
            field_type = remove_parentheses_content(field.sql_column_type)
            if not ignore_functions or field_type not in (
                DATA_TYPES[self.database.type][SemanticDataTypeEnum.UUID],
                DATA_TYPES[self.database.type][SemanticDataTypeEnum.TIMESTAMP],
            ):
                field_type = self.set_default_field_value(field)
            if field.sql_name:
                field_name = field.sql_name
            else:
                field_name = field.name
            if field.is_key and ((not without_date and field.name not in (DATEFROM, DATETO)) or without_date):
                primary_keys.append(f'"{field_name}"')
            fields_str += f'"{field_name}" {field_type},'
        return fields_str, primary_keys

    def _get_create_table_request_sql_wihout_pk(
        self,
        table: DbObject,
        fields: list[DataStorageField],
        not_exists: bool = False,
    ) -> tuple[str, list[str]]:
        """
        Создать запрос на создание таблицы без primary_keys.
        """
        create_table_sql = self._get_create_table_prefix(
            table.schema_name, table.name, self.database.default_cluster_name, not_exists
        )
        fields_str, primary_keys = self._get_fields_and_pks(
            fields,
        )
        return create_table_sql + fields_str, primary_keys

    async def create_tables(
        self,
        tables: list[DbObject],
        fields: list[DataStorageField],
        not_exist: bool = False,
        execute_query: bool = True,
        delete_if_failder: bool = True,
    ) -> list[DatabaseObjectGenerationResult]:
        """
        Создать таблицу для DataStorage в clickhouse, GreenPlum или PostgreSQL.
        """
        result: list[DatabaseObjectGenerationResult] = []
        if not settings.ENABLE_GENERATE_OBJECTS:
            return result
        tables = self.sort_tables(tables)
        sql_expressions, tables = self.get_create_tables_sql_by_tables_and_fields(tables, fields, not_exist)
        logger.debug("SQL_EXPRESSION created. sql_expression='%s'", sql_expressions)
        if execute_query and not sql_expressions:
            logger.debug("There are no tables to create")
            return result
        delete_flag = False
        for sql_expression, table in zip(sql_expressions, tables):
            result.append(
                DatabaseObjectGenerationResult(
                    table=table,
                    sql_expression=sql_expression,
                )
            )
            if execute_query and sql_expressions:
                try:
                    result[-1].executed = True
                    await self._execute_DDL(sql_expression)
                    logger.debug(
                        "Table %s.%s created in database %s.", table.schema_name, table.name, self.database.name
                    )
                except Exception as exc:
                    logger.exception(
                        "Table %s.%s not created in database %s.", table.schema_name, table.name, self.database.name
                    )
                    result[-1].error = str(exc)
                    delete_flag = True
        if delete_flag and delete_if_failder:
            logger.debug("Try deleting tables %s", tables)
            await self.delete_tables(tables, fields, True, False)
        return result

    async def delete_tables(
        self,
        tables: list[DbObject],
        fields: list[DataStorageField],
        exists: bool = False,
        recreate_if_failed: bool = True,
        check_possible_delete: bool = True,
        execute_query: bool = True,
    ) -> list[DatabaseObjectGenerationResult]:
        """
        Удалить таблицу для DataStorage в Clickhouse, GreenPlum или PostgreSQL.
        """
        result: list[DatabaseObjectGenerationResult] = []
        if not settings.ENABLE_GENERATE_OBJECTS:
            return result
        tables = self.sort_tables(tables, reverse=True)
        sql_expressions, tables = await self._get_delete_tables_sql_and_check_possible_delete(
            tables, exists, check_possible_delete
        )
        if execute_query and not sql_expressions:
            logger.debug("There are no tables to create")
            return result
        create_flag = False
        for sql_expression, table in zip(sql_expressions, tables):
            result.append(
                DatabaseObjectGenerationResult(
                    table=table,
                    sql_expression=sql_expression,
                )
            )
            try:
                if sql_expressions and execute_query:
                    result[-1].executed = True
                    await self._execute_DDL(sql_expression)
                    logger.debug("Tables %s dropped from database.", tables)
            except Exception as ext:
                logger.exception(
                    "Error: deleting database_objects: %s.",
                    tables,
                )
                result[-1].error = str(ext)
                create_flag = True
        if create_flag and recreate_if_failed:
            logger.debug("Try recreate database_objects: %s.", tables)
            _ = await self.create_tables(tables, fields, True, False)
        return result

    async def recreate_tables(
        self,
        tables: list[DbObject],
        fields: list[DataStorageField],
        сheck_possible_to_drop_tables: bool = True,
        execute_query: bool = True,
    ) -> list[DatabaseObjectGenerationResult]:
        """Пересоздание модели таблицы для dso."""
        db_objects_strs = [f"{table.schema_name}.{table.name}" for table in tables]
        if сheck_possible_to_drop_tables:
            is_possible_drop = await self.is_possible_to_drop_tables(tables)
            is_exists_fields = bool(fields)
        else:
            is_possible_drop = True
            is_exists_fields = True
        if is_exists_fields and is_possible_drop:
            result = []
            sql_expressions_delete = await self.delete_tables(
                tables,
                fields,
                True,
                recreate_if_failed=False,
                check_possible_delete=сheck_possible_to_drop_tables,
                execute_query=execute_query,
            )
            result.extend(sql_expressions_delete)
            sql_expressions_create = await self.create_tables(
                tables, fields, True, delete_if_failder=False, execute_query=execute_query
            )
            result.extend(sql_expressions_create)
            return result
        elif not is_exists_fields:
            raise ValueError(f"Fields is empty for {db_objects_strs}.")
        raise ValueError(f"It is impossible to recreate a tables {db_objects_strs}.")

    async def alter_tables_by_comparing_with_meta(
        self,
        tables: list[DbObject],
        fields: list[DataStorageField],
        enable_delete_column: bool = False,
        enable_delete_not_empty: bool = False,
        execute_query: bool = True,
    ) -> tuple[bool, bool, list[DatabaseObjectGenerationResult]]:
        """
        Обновление таблицы в базе данных по сравнению с метаинформацией.

        Args:
            datastorage (DataStorage): datastorage, который необходимо обновить.
            model (Model): модель, в которой находится datastorage.
            enable_delete_column (bool): Включить удаление столбца из таблицы в случае, если он отсутствует в метаинформации.
            execute_query (bool): Выполнить запросы к базе данных или только вернуть SQL выражения.
            enable_delete_not_empty (bool): Флаг, который указывает, можно ли удалить столбец, если он не пустой.
        Returns:
            tuple[bool, bool, list[str]]: tuple, состоящий из трех значений в следующем порядке:
            - флаг обновилась ли таблица
            - возможно ли ее изменить без пересоздания
            - список sql выражений для обновления таблицы
        """
        database_objects_names = get_database_object_names(tables)
        if not database_objects_names.table_schema or not database_objects_names.table_name:
            raise ValueError(f"It is not possible to update {tables} that does not have a table")
        phis_table_fields = await self._get_table_field_description(
            database_objects_names.table_schema, database_objects_names.table_name
        )
        sql_expressions = []
        dso_fields = {}
        for dso_field in fields:
            if dso_field.sql_name is None or dso_field.sql_column_type is None:
                raise ValueError(f"it is not possible to use a field {dso_field.name} without sqlName or sqlColumnType")
            dso_fields[dso_field.sql_name] = dso_field
            with_precision = dso_field.name != TIMESTAMP
            dso_field_type = (
                remove_parentheses_content(dso_field.sql_column_type) if with_precision else dso_field.sql_column_type
            )
            if dso_field.sql_name in phis_table_fields:
                if (
                    dso_field_type == phis_table_fields[dso_field.sql_name]["data_type"]
                    and dso_field.is_key == phis_table_fields[dso_field.sql_name]["is_primary_key"]
                ):
                    continue

                if dso_field.is_key != phis_table_fields[dso_field.sql_name]["is_primary_key"]:
                    logger.debug(
                        "You cannot update the primary key (Meta is_key=%s, Db is_key=%s for field=%s). The table needs to be recreated.",
                        dso_field.is_key,
                        phis_table_fields[dso_field.sql_name]["is_primary_key"],
                        dso_field.sql_name,
                    )
                    return True, False, []

                if (
                    dso_field_type != phis_table_fields[dso_field.sql_name]["data_type"]
                    and phis_table_fields[dso_field.sql_name]["is_primary_key"]
                ):
                    logger.debug(
                        "You cannot update the primary key type (Meta type=%s, Db type=%s for field=%s). The table needs to be recreated.",
                        dso_field_type,
                        phis_table_fields[dso_field.sql_name]["data_type"],
                        dso_field.sql_name,
                    )
                    return True, False, []

                if dso_field_type != phis_table_fields[dso_field.sql_name]["data_type"]:
                    sql_expressions.extend(
                        self._get_modify_column_sql(
                            dso_field.sql_name,
                            dso_field_type,
                            self.get_default_value_by_field(dso_field),
                        )
                    )
            elif dso_field.is_key:
                logger.debug(
                    "You cannot create primary key column (%s). The table needs to be recreated.", dso_field.sql_name
                )
                return True, False, []
            else:
                sql_expressions.append(
                    self._get_create_column_sql(
                        dso_field.sql_name,
                        dso_field_type,
                        self.get_default_value_by_field(dso_field),
                    )
                )

        for phis_field_name, phis_field_value in phis_table_fields.items():
            if phis_field_name in dso_fields:
                continue
            if enable_delete_column:
                if phis_field_value["is_primary_key"]:
                    logger.debug(
                        "You cannot drop the primary key (%s). The table needs to be recreated.", phis_field_name
                    )
                    return True, False, []
                if enable_delete_not_empty or await self._is_possible_to_drop_column(tables, phis_field_name):
                    sql_expressions.append(self._get_drop_column_sql(phis_field_name))
                else:
                    return True, False, []
            else:
                logger.warning("You cannot drop column %s. Column drop is disabled.", phis_field_name)
        sql_expressions, tables = await self.get_alter_tables_sql_expressions(tables, fields, sql_expressions)
        result: list[DatabaseObjectGenerationResult] = []
        for sql_expression, table in zip(sql_expressions, tables):
            result.append(
                DatabaseObjectGenerationResult(
                    table=table,
                    sql_expression=sql_expression,
                )
            )
            if sql_expressions and execute_query:
                try:
                    result[-1].executed = True
                    await self._execute_DDL(sql_expressions)
                except Exception as exc:
                    logger.exception(
                        "Error: updating tables: %s.",
                        tables,
                    )
                    result[-1].error = str(exc)
        if sql_expressions and execute_query:
            logger.debug("Tables %s success updated.", tables)
            return True, True, result
        elif sql_expressions:
            logger.debug("Tables sql update generated but not executed")
            return True, False, result
        return False, False, []

    async def update_tables(
        self,
        tables: list[DbObject],
        fields: list[DataStorageField],
        enable_delete_column: bool = True,
        enable_delete_not_empty: bool = False,
        execute_query: bool = True,
    ) -> list[DatabaseObjectGenerationResult]:
        """
        Обновляет таблицу для объекта DataStorage в ClickHouse, GreenPlum или PostgreSQL.

        Проверяет существование таблицы и при необходимости изменяет её структуру на основе метаданных модели.
        При невозможности применения изменений пытается удалить и заново создать таблицу.

        Args:
            data_storage (DataStorage): Хранилище данных, представляющее целевую таблицу.
            model (Model): Модель.
            enable_delete_column (bool): Разрешает удаление колонок из таблицы. По умолчанию True.
            enable_delete_not_empty (bool): Разрешает удаление/пересоздание не пустой таблицы. По умолчанию False.
            execute_query (bool): Выполнять ли SQL-запросы или только возвращать их. По умолчанию True.

        Returns:
            list[str]: Список сгенерированных SQL-запросов для изменения таблицы.

        Raises:
            ValueError: Если требуется пересоздать таблицу, но она содержит данные и
                    `enable_delete_not_empty` установлен в False.
        """
        if not settings.ENABLE_GENERATE_OBJECTS:
            return []
        is_exist = await self._is_exist_tables(tables)
        if is_exist:
            is_changed, is_sql_expressions_completed, sql_expressions = await self.alter_tables_by_comparing_with_meta(
                tables, fields, enable_delete_column, enable_delete_not_empty, execute_query
            )
            if (is_changed and is_sql_expressions_completed) or (
                is_changed and not is_sql_expressions_completed and sql_expressions
            ):
                if execute_query:
                    logger.info("Tables %s updated.", tables)
                return sql_expressions
            elif is_changed:
                logger.debug("Changes to %s cannot be applied. Check possible to drop.", tables)
                is_possible_to_drop = (
                    await self.is_possible_to_drop_tables(tables) if not enable_delete_not_empty else True
                )
                if is_possible_to_drop:
                    logger.debug("Tables %s can be dropped. Let's try to recreate.", tables)
                    sql_expressions = await self.recreate_tables(
                        tables, fields, сheck_possible_to_drop_tables=False, execute_query=execute_query
                    )
                    logger.info("Tables %s recreated.", tables)
                    return sql_expressions
                else:
                    raise ValueError("A change that requires the table to be recreated, but the table is not empty.")
        else:
            logger.debug("Tables %s does not exist. Let's try to recreate", tables)
            return await self.recreate_tables(tables, fields, execute_query=execute_query)
        logger.info("Tables %s already updated.", tables)
        return []
