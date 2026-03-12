"""
Репозиторий, который генерирует объекты в базе данных PostgreSQL
"""

from collections import defaultdict
from typing import Any, Optional, Sequence

from py_common_lib.logger import EPMPYLogger
from py_common_lib.logger.audit_types import F9
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError

from src.db.composite import Composite
from src.db.data_storage import DataStorage, DataStorageField
from src.db.engine import DatabaseConnector
from src.db.model import Model
from src.models.consts import DATA_TYPES
from src.models.database import Database as DatabaseModel, DatabaseTypeEnum
from src.models.database_object import DatabaseObject as DatabaseObjectModel, DbObjectTypeEnum
from src.models.dimension import DimensionTypeEnum
from src.models.measure import MeasureTypeEnum
from src.repository.generators.base_generator import GeneratorRepository
from src.repository.utils import (
    get_field_type_with_length,
    get_filtred_database_object_by_data_storage,
    get_ip_address_by_dns_name,
    is_nullable_measure_field,
    get_object_filtred_by_model_name,
)
from src.utils.backoff import RetryConfig, retry
from src.utils.view_parser import contains_sql_identifier

logger = EPMPYLogger(__name__)


class GeneratorPostgreSQLRepository(GeneratorRepository):
    """Генератор табличек в PostgreSQL или совместимых баз"""

    @classmethod
    def _get_create_view_sql(
        cls, schema_name: str, name: str, sql_expression: str, cluster_name: Optional[str] = None, replace: bool = False
    ) -> list[str]:
        """Генерация DDL, который создает или пересоздает VIEW."""
        result = []
        if replace:
            drop_view_sql = cls._get_delete_view_sql(schema_name=schema_name, name=name, cluster_name=cluster_name)
            result.append(drop_view_sql)
        create_view_sql = f"CREATE VIEW {schema_name}.{name} AS ({sql_expression})"
        result.append(create_view_sql)
        return result

    @classmethod
    def _get_create_column_sql(
        cls,
        field_name: str,
        field_type: str,
        default_value: Optional[str] = None,
        is_nullable: bool = False,
    ) -> str:
        """
        Создать запрос на добавление колонки.
        """
        default_value = f" DEFAULT {default_value}" if default_value is not None else ""
        not_null = "" if is_nullable else " NOT NULL"
        return f'ADD COLUMN IF NOT EXISTS "{field_name}" {field_type}{not_null}{default_value}'

    @classmethod
    def _get_delete_view_sql(cls, schema_name: str, name: str, cluster_name: Optional[str] = None) -> str:
        """Генерация DDL, который удаляет VIEW."""
        return f"DROP VIEW IF EXISTS {schema_name}.{name}"

    @classmethod
    def _get_delete_db_objects_sql(
        cls, database_objects: list[DatabaseObjectModel], cluster_name: Optional[str] = None, exists: bool = True
    ) -> list[str]:
        """
        Создать запрос на удаление таблицы.
        """
        if_exists = "IF EXISTS"
        sql_expressions = []
        for database_object in database_objects:
            sql_expressions.append(f"DROP TABLE {if_exists} {database_object.schema_name}.{database_object.name}")
        return sql_expressions

    @classmethod
    def _get_table_field_type(
        cls, field: DataStorageField, db_type: DatabaseTypeEnum, without_null: bool = False, with_precision: bool = True
    ) -> str:
        """
        Конвертация семантического типа в технический.
        Например INTEGER -> int4 DEFAULT '0'
        """
        precision, scale, field_type = get_field_type_with_length(field)
        if field_type == MeasureTypeEnum.DECIMAL:
            total_precision = None
            if precision is not None and scale is not None:
                total_precision = precision + scale
            else:
                total_precision = precision if precision is not None else scale
            result_field_type = DATA_TYPES[db_type][field_type] + (
                f"({total_precision},{scale})" if with_precision else ""
            )
        elif field_type in (
            DimensionTypeEnum.TIME,
            DimensionTypeEnum.DATETIME,
            DimensionTypeEnum.TIMESTAMP,
        ):
            result_field_type = DATA_TYPES[db_type][field_type] + (
                f"({precision})" if with_precision and precision is not None else ""
            )
        else:
            result_field_type = DATA_TYPES[db_type][field_type]
        if without_null:
            return result_field_type
        if is_nullable_measure_field(field):
            return result_field_type
        default_value = cls.get_default_value_by_field(field, db_type)
        if default_value is not None:
            return f"{result_field_type} NOT NULL DEFAULT {default_value}"
        else:
            result_field_type = f"{result_field_type} NOT NULL"
        return result_field_type

    @classmethod
    @retry(RetryConfig())
    async def _execute_DDL(cls, queries: str | list[str], database: DatabaseModel) -> None:
        """
        Выполнение DDL запроса.
        Например: "CREATE", "ALTER TABLE", "DROP TABLE" и т.д
        """
        try:
            db = DatabaseConnector(database=database)
            _, async_session_maker = await db.get_not_pg_is_in_recovery()
            async with async_session_maker() as session:
                if isinstance(queries, list):
                    for query in queries:
                        query = query.replace("`", '"')
                        logger.debug("""EXECUTE QUERY: "%s";""", query)
                        await session.execute(text(query))
                else:
                    logger.debug("""EXECUTE QUERY: "%s";""", queries)
                    await session.execute(text(queries))
                await session.commit()
                return None
        except ConnectionRefusedError as ext:
            ip_address = get_ip_address_by_dns_name(database.connections[0].host)
            logger.audit(F9, host=ip_address, dns_name=database.connections[0].host)
            raise ConnectionRefusedError(str(ext))

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
            db = DatabaseConnector(database=database)
            _, async_session_maker = await db.get_not_pg_is_in_recovery()
            async with async_session_maker() as session:
                query = query.replace("`", '"')
                logger.debug("""EXECUTE QUERY: "%s"; with params=%s""", query, params)
                result_gp = await session.execute(text(query), params)
                if result_gp.rowcount == 0:  # type: ignore
                    return []
                rows = result_gp.fetchall()
                result_rows: list[Sequence[Any]] = []
                for row in rows:
                    result_rows.append(tuple(row))
                return result_rows
        except ConnectionRefusedError as exc:
            ip_address = get_ip_address_by_dns_name(database.connections[0].host)
            logger.audit(F9, host=ip_address, dns_name=database.connections[0].host)
            raise ConnectionRefusedError(str(exc))
        except ProgrammingError as exc:
            if "UndefinedTableError" in str(exc):
                logger.warning("Unknown table")
            else:
                logger.exception("Error execute query: %s", query)
            raise ProgrammingError(str(exc), orig=exc.orig if exc.orig is not None else exc, params=exc.params)
        except Exception as exc:
            logger.exception("Error execute query: %s", query)
            raise Exception(str(exc))

    @classmethod
    async def find_views_by_table(
        cls, database: DatabaseModel, schema_name: str, table_names: list[str]
    ) -> list[dict[str, str]]:
        """Ищет представления по списку таблиц через information_schema.views."""
        if not table_names:
            return []
        query = (
            "SELECT table_schema AS view_schema, table_name AS view_name, view_definition "
            "FROM information_schema.views "
            "WHERE table_schema = :schema_name"
        )
        rows = await cls._get_data_query(query, database, {"schema_name": schema_name})
        return [
            {
                "view_schema": row[0],
                "view_name": row[1],
                "view_definition": row[2],
            }
            for row in rows
            if row[2] and any(contains_sql_identifier(row[2], table_name) for table_name in table_names)
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
        for database_object in database_objects:
            select_sql = f'SELECT "{field_name}" FROM {database_object.schema_name}.{database_object.name} where "{field_name}" is not NULL LIMIT 1'
            result = await cls._get_data_query(select_sql, database)
            possible_to_drop &= not bool(result)
        return possible_to_drop

    @classmethod
    def _get_modify_column_sql(
        cls,
        field_name: str,
        field_type: str,
        default_value: Optional[str] = None,
        is_nullable: bool = False,
        current_field_type: Optional[str] = None,
        current_is_nullable: bool = False,
    ) -> list[str]:
        """
        Создать запрос на изменение колонки.
        """
        sql_expressions = [f'ALTER COLUMN "{field_name}" DROP DEFAULT;']
        if current_field_type != field_type:
            sql_expressions.append(f'ALTER COLUMN "{field_name}" TYPE {field_type} USING "{field_name}"::{field_type}')
        if is_nullable:
            if not current_is_nullable:
                sql_expressions.append(f'ALTER COLUMN "{field_name}" DROP NOT NULL')
            return sql_expressions
        if current_is_nullable and default_value is not None:
            sql_expressions.append(f'__UPDATE_NULLS__|{field_name}|{default_value}')
            sql_expressions.append(f'ALTER COLUMN "{field_name}" SET NOT NULL')
        if default_value is not None:
            sql_expressions.append(f'ALTER COLUMN "{field_name}" SET DEFAULT {default_value}')
        return sql_expressions

    @classmethod
    def _get_create_table_prefix(
        cls, database_object: DatabaseObjectModel, cluster_name: Optional[str] = None, not_exists: bool = False
    ) -> str:
        """
        Создать первую часть запроса на создание таблицы.
        Возвращает: "CREATE TABLE {table.schema_name}.{table.name} {on_cluster} (".
        """
        if_not_exists = " IF NOT EXISTS " if not_exists else " "
        create_table_sql = f"CREATE TABLE{if_not_exists}{database_object.schema_name}.{database_object.name} ( "
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
        database_objects = get_object_filtred_by_model_name(data_storage.database_objects, model_name, True)
        database_objects_model = [
            DatabaseObjectModel.model_validate(database_object) for database_object in database_objects
        ]
        create_table_sqls = []
        for database_object in database_objects_model:
            if database_object.type not in (DbObjectTypeEnum.TABLE, DbObjectTypeEnum.REPLICATED_TABLE):
                continue
            create_table_sql, primary_keys = cls._get_create_table_request_sql_wihout_pk(
                data_storage, database_object, database_model, not_exists, dimension_tech_fields
            )
            if primary_keys:
                create_table_sql += f"CONSTRAINT {database_object.tenant_id}_{database_object.schema_name}_{database_object.name}_pkey PRIMARY KEY ({','.join(primary_keys)}))"
            else:
                create_table_sql = create_table_sql[:-1] + ")"
            create_table_sqls.append(create_table_sql)
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
            for database_object in database_objects:
                if not database_object.tenant_id:
                    raise ValueError("The Table model has no tenant. Are you reading it from the cache?")
                select_sql = f"SELECT * FROM {database_object.schema_name}.{database_object.name} LIMIT 1"
                result = await cls._get_data_query(select_sql, database)
                possible_to_drop &= not bool(result)
                logger.debug("Possible_drop=%s", possible_to_drop)
            return possible_to_drop
        except ProgrammingError as exc:
            if "UndefinedTableError" in str(exc) and allow_non_exist_tables:
                return True
            raise ProgrammingError(str(exc), orig=exc.orig if exc.orig is not None else exc, params=exc.params)

    @classmethod
    async def _is_exist_database_objects(
        cls,
        database_objects: list[DatabaseObjectModel],
        database: DatabaseModel,
    ) -> bool:
        try:
            for database_object in database_objects:
                if not database_object.tenant_id:
                    raise ValueError("The Table model has no tenant. Are you reading it from the cache?")
                select_sql = f"SELECT * FROM {database_object.schema_name}.{database_object.name} LIMIT 1"
                _ = await cls._get_data_query(select_sql, database)
        except ProgrammingError as exc:
            if "UndefinedTableError" in str(exc):
                return False
        return True

    @classmethod
    async def _get_table_field_description(
        cls, database: DatabaseModel, schema_name: str, table_name: str
    ) -> dict[str, dict[str, Any]]:
        query = (
            "SELECT a.attname AS column_name, t.typname AS internal_data_type, "
            "CASE WHEN i.indisprimary AND a.attnum = ANY(i.indkey) THEN true ELSE false END AS is_primary_key, "
            "NOT a.attnotnull AS is_nullable "
            "FROM pg_attribute a "
            "JOIN pg_class c ON a.attrelid = c.oid "
            "JOIN pg_type t ON a.atttypid = t.oid "
            "JOIN pg_namespace n ON c.relnamespace = n.oid "
            "LEFT JOIN pg_index i ON c.oid = i.indrelid "
            "WHERE n.nspname = :table_schema AND c.relname = :table_name AND a.attnum > 0 AND NOT a.attisdropped "
            "ORDER BY a.attnum;"
        )
        fields_description = await cls._get_data_query(
            query, database, {"table_name": table_name, "table_schema": schema_name}
        )
        result: dict[str, dict[str, Any]] = defaultdict(dict)
        for field_description in fields_description:
            result[field_description[0]].update(
                {
                    "data_type": field_description[1],
                    "is_primary_key": field_description[2],
                    "is_nullable": field_description[3],
                }
            )
        return result

    @classmethod
    async def get_alter_database_objects_sql_expressions(
        cls,
        datastorage: DataStorage,
        model: Model,
        sql_expressions: list[str],
    ) -> list[str]:
        """
        Собрать из сырых sql выражений (alter) запросы для postgres.
        Если есть зависимые VIEW (например, композиты), они будут
        удалены перед ALTER TABLE и пересозданы после.

        Args:
            datastorage (DataStorage): datastorage, который хотим обновить
            model (Model): модель, к которой привязан datastorage
            sql_expressions (list[str]): сырые запросы

        Returns:
            list[str]: готовые запросы
        """
        database_objects = get_filtred_database_object_by_data_storage(datastorage, model.name)

        sql_expressions_for_execute = []
        for database_object in database_objects:
            for sql_expression in sql_expressions:
                if sql_expression.startswith("__UPDATE_NULLS__|"):
                    _, field_name, default_value = sql_expression.split("|", maxsplit=2)
                    sql_expressions_for_execute.append(
                        f'UPDATE {database_object.schema_name}.{database_object.name} SET "{field_name}" = {default_value} WHERE "{field_name}" IS NULL'
                    )
                else:
                    sql_expressions_for_execute.append(
                        f"ALTER TABLE {database_object.schema_name}.{database_object.name} {sql_expression}"
                    )

        return sql_expressions_for_execute

