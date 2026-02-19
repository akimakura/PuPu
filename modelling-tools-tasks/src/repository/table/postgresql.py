from collections import defaultdict
from typing import Any, Optional, Sequence

from py_common_lib.logger import EPMPYLogger
from sqlalchemy.exc import ProgrammingError

from src.db.engines.postgresql.engine import execute_raw_DDL, execute_raw_DQL_or_DML
from src.integrations.modelling_tools_api.codegen import DataStorageField, DbObject
from src.models.database_object import DbObjectTypeEnum
from src.repository.table.core import TableRepository
from src.utils.backoff import RetryConfig, retry

logger = EPMPYLogger(__name__)


class TablePostgreSqlRepository(TableRepository):
    def _get_create_table_prefix(
        self, schema_name: str, table_name: str, cluster_name: Optional[str] = None, not_exists: bool = False
    ) -> str:
        """
        Создать первую часть запроса на создание таблицы.
        Возвращает: "CREATE TABLE {table.schema_name}.{table.name} {on_cluster} (".
        """
        if_not_exists = " IF NOT EXISTS " if not_exists else " "
        create_table_sql = f"CREATE TABLE{if_not_exists}{schema_name}.{table_name} ( "
        return create_table_sql

    def set_default_field_value(self, field: DataStorageField) -> str:
        """
        Конвертация семантического типа в технический.
        Например INTEGER -> int4 DEFAULT '0'
        """
        if field.sql_column_type is None:
            raise ValueError("Field has no sql_column_type. ")
        default_value = self.get_default_value_by_field(field)
        if default_value is not None:
            return f"{field.sql_column_type} NOT NULL DEFAULT {default_value}"
        else:
            return f"{field.sql_column_type} NOT NULL"

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
        for table in tables:
            if table.object_type not in (DbObjectTypeEnum.TABLE, DbObjectTypeEnum.REPLICATED_TABLE):
                continue
            create_table_sql, primary_keys = self._get_create_table_request_sql_wihout_pk(
                table,
                fields,
                not_exists,
            )
            if primary_keys:
                create_table_sql += f"CONSTRAINT {self.tenant_id}_{table.schema_name}_{table.name}_pkey PRIMARY KEY ({','.join(primary_keys)}))"
            else:
                create_table_sql = create_table_sql[:-1] + ")"
            create_table_sqls[0].append(create_table_sql)
            create_table_sqls[1].append(table)
        return create_table_sqls

    async def _execute_DDL(self, queries: str | list[str]) -> None:
        """
        Выполнение DDL запроса.
        Например: "CREATE", "ALTER TABLE", "DROP TABLE" и т.д
        """
        return await execute_raw_DDL(self.tenant_id, self.database, queries)

    async def _is_exist_tables(
        self,
        tables: list[DbObject],
    ) -> bool:
        try:
            for table in tables:
                select_sql = f"SELECT * FROM {table.schema_name}.{table.name} LIMIT 1"
                _ = await self._get_data_query(select_sql)
        except ProgrammingError as exc:
            if "UndefinedTableError" in str(exc):
                return False
        return True

    def _get_delete_tables_sql(self, tables: list[DbObject], exists: bool = True) -> tuple[list[str], list[DbObject]]:
        """
        Создать запрос на удаление таблицы.
        """
        if_exists = "IF EXISTS" if exists else ""
        sql_expressions = []
        for table in tables:
            sql_expressions.append(f"DROP TABLE {if_exists} {table.schema_name}.{table.name}")
        return sql_expressions, tables

    async def _is_possible_to_drop(self, tables: list[DbObject], allow_non_exist_tables: bool = False) -> bool:
        """
        Проверка можно ли дропнуть таблицу (пустая ли она)
        """
        try:
            possible_to_drop = True
            for table in tables:
                select_sql = f"SELECT * FROM {table.schema_name}.{table.name} LIMIT 1"
                result = await self._get_data_query(select_sql)
                possible_to_drop &= not bool(result)
                logger.debug("Possible_drop=%s", possible_to_drop)
            return possible_to_drop
        except ProgrammingError as exc:
            if "UndefinedTableError" in str(exc) and allow_non_exist_tables:
                return True
            raise ProgrammingError(str(exc), orig=exc.orig if exc.orig is not None else exc, params=exc.params)

    @retry(RetryConfig(logger=None))
    async def _get_data_query(self, query: str, params: Optional[dict[Any, Any]] = None) -> Sequence[Any]:
        """
        Выполнение запроса, который возвращает строки.
        Например: "SELECT", "UPDATE ... RETURNING ..." и т.д.
        """
        return await execute_raw_DQL_or_DML(self.tenant_id, self.database, query, params)

    async def _get_table_field_description(self, table_schema: str, table_name: str) -> dict[str, dict[str, Any]]:
        query = (
            "SELECT a.attname AS column_name, t.typname AS internal_data_type, "
            "CASE WHEN i.indisprimary AND a.attnum = ANY(i.indkey) THEN true ELSE false END AS is_primary_key "
            "FROM pg_attribute a "
            "JOIN pg_class c ON a.attrelid = c.oid "
            "JOIN pg_type t ON a.atttypid = t.oid "
            "JOIN pg_namespace n ON c.relnamespace = n.oid "
            "LEFT JOIN pg_index i ON c.oid = i.indrelid "
            "WHERE n.nspname = :table_schema AND c.relname = :table_name AND a.attnum > 0 AND NOT a.attisdropped "
            "ORDER BY a.attnum;"
        )
        fields_description = await self._get_data_query(query, {"table_name": table_name, "table_schema": table_schema})
        result: dict[str, dict[str, Any]] = defaultdict(dict)
        for field_description in fields_description:
            result[field_description[0]].update({
                "data_type": field_description[1],
                "is_primary_key": field_description[2],
            })
        return result

    def _get_modify_column_sql(
        self, field_name: str, field_type: str, default_value: Optional[str] = None
    ) -> list[str]:
        """
        Создать запрос на изменение колонки.
        """
        sql_expressions = [
            f'ALTER COLUMN "{field_name}" DROP DEFAULT;',
            f'ALTER COLUMN "{field_name}" TYPE {field_type} USING "{field_name}"::{field_type}',
        ]
        if default_value is not None:
            sql_expressions.append(f'ALTER COLUMN "{field_name}" SET DEFAULT {default_value}')
        return sql_expressions

    def _get_create_column_sql(self, field_name: str, field_type: str, default_value: Optional[str] = None) -> str:
        """
        Создать запрос на добавление колонки.
        """
        default_value = f" DEFAULT {default_value}" if default_value is not None else ""
        return f'ADD COLUMN IF NOT EXISTS "{field_name}" {field_type} NOT NULL{default_value}'

    async def _is_possible_to_drop_column(
        self,
        tables: list[DbObject],
        field_name: str,
    ) -> bool:
        """
        Проверка можно ли дропнуть поле (пустое ли оно)
        """
        possible_to_drop = True
        for table in tables:
            select_sql = (
                f'SELECT "{field_name}" FROM {table.schema_name}.{table.name} where "{field_name}" is not NULL LIMIT 1'
            )
            result = await self._get_data_query(select_sql)
            possible_to_drop &= not bool(result)
        return possible_to_drop

    async def get_alter_tables_sql_expressions(
        self,
        tables: list[DbObject],
        fields: list[DataStorageField],
        sql_expressions: list[str],
    ) -> tuple[list[str], list[DbObject]]:
        """
        Собрать из сырых sql выражений (alter) запросы для postgres.

        Args:
            datastorage (DataStorage): datastorage, который хотим обновить
            model (Model): модель, к которой привязан datastorage
            sql_expressions (list[str]): сырые запросы

        Returns:
            list[str]: готовые запросы
        """
        sql_expressions_for_execute = []
        for table in tables:
            for sql_expression in sql_expressions:
                sql_expressions_for_execute.append(f"ALTER TABLE {table.schema_name}.{table.name} {sql_expression}")
        return sql_expressions_for_execute, tables
