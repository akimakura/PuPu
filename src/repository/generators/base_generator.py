"""
Репозиторий, который генерирует объекты в базе данных
"""

from typing import Any, Optional, Sequence

from py_common_lib.logger import EPMPYLogger

from src.config import settings
from src.db.composite import Composite
from src.db.data_storage import DataStorage, DataStorageField
from src.db.dimension import Dimension
from src.db.model import Model
from src.models.composite import CompositeFieldRefObjectEnum
from src.models.consts import DATEFROM, DATETO, DEFAULT_DATE_FROM, DEFAULT_DATE_TO, DEFAULT_TYPE_VALUES
from src.models.data_storage import DataStorage as DataStorageModel, DataStorageEditRequest, DataStorageLogsFieldEnum
from src.models.database import Database as DatabaseModel, DatabaseTypeEnum
from src.models.database_object import DatabaseObject as DatabaseObjectModel
from src.models.dimension import DimensionTypeEnum, TechDimensionEnum
from src.models.model import Model as ModelModel
from src.repository.utils import (
    get_database_object_names,
    get_field_type_with_length,
    get_filtred_database_object_by_data_storage,
    is_nullable_measure_field,
    get_object_filtred_by_model_name,
)
from src.utils.backoff import RetryConfig, retry

logger = EPMPYLogger(__name__)


class GeneratorRepository:

    @classmethod
    async def get_alter_database_objects_sql_expressions(
        cls,
        datastorage: DataStorage,
        model: Model,
        sql_expressions: list[str],
    ) -> list[str]:
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

    @classmethod
    def _get_table_field_type(
        cls,
        field: DataStorageField,
        db_type: DatabaseTypeEnum,
        without_null: bool = False,
        with_precision: bool = False,
    ) -> str:
        """
        Возвращает технический тип данных поля
        Например Nullable(Int64)
        """
        raise NotImplementedError

    @classmethod
    @retry(RetryConfig())
    async def _execute_DDL(cls, queries: str | list[str], database: DatabaseModel) -> None:
        """
        Выполнение DDL запроса.
        Например: "CREATE", "ALTER TABLE", "DROP TABLE" и т.д
        """
        raise NotImplementedError

    @classmethod
    @retry(RetryConfig())
    async def _get_data_query(cls, query: str, database: DatabaseModel) -> list[Sequence[Any]]:
        """
        Выполнение запроса, который возвращает строки.
        Например: "SELECT", "UPDATE ... RETURNING ..." и т.д.
        """
        raise NotImplementedError

    @classmethod
    async def find_views_by_table(
        cls, database: DatabaseModel, schema_name: str, table_names: list[str]
    ) -> list[dict[str, str]]:
        """Возвращает DDL представлений, зависящих от списка таблиц."""
        raise NotImplementedError

    @classmethod
    async def _is_possible_to_drop(
        cls, database_objects: list[DatabaseObjectModel], database: DatabaseModel, allow_non_exist_tables: bool = False
    ) -> bool:
        """
        Проверка можно ли дропнуть таблицу (пустая ли она)
        """
        raise NotImplementedError

    @classmethod
    async def recreate_dictionary(
        cls,
        data_storage: DataStorage,
        model_model: ModelModel,
    ) -> bool:
        raise NotImplementedError

    @classmethod
    async def _get_table_field_description(
        cls, database: DatabaseModel, schema_name: str, table_name: str
    ) -> dict[str, dict[str, Any]]:
        raise NotImplementedError

    @classmethod
    async def is_exist_datastorage(cls, data_storage: DataStorage, model: Model) -> bool:
        database = DatabaseModel.model_validate(model.database)
        database_objects_model = get_filtred_database_object_by_data_storage(data_storage, model.name)
        return await cls._is_exist_database_objects(database_objects_model, database)

    @classmethod
    async def _is_exist_database_objects(
        cls,
        database_objects: list[DatabaseObjectModel],
        database: DatabaseModel,
    ) -> bool:
        raise NotImplementedError

    @classmethod
    async def is_possible_to_drop_data_storage(cls, data_storage: DataStorage, model: Model) -> bool:
        """
        Проверяет, возможно ли удалить хранилище данных.

        Args:
            data_storage (DataStorage): Хранилище данных.
            model (Model): Модель.

        Returns:
            bool: True, если удаление возможно, иначе False.
        """
        database = DatabaseModel.model_validate(model.database)
        database_objects_model = get_filtred_database_object_by_data_storage(data_storage, model.name)
        possible_to_drop = await cls._is_possible_to_drop(database_objects_model, database, True)
        if possible_to_drop:
            logger.debug("Datastorage %s is possible to drop", data_storage.name)
        return possible_to_drop

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
        raise NotImplementedError

    @classmethod
    def get_default_value_by_field(cls, field: DataStorageField, db_type: DatabaseTypeEnum) -> Optional[str]:
        """Возвращает дефолтное значение для поля."""
        if is_nullable_measure_field(field):
            return None
        _, _, field_type = get_field_type_with_length(field)
        if field.name == DATEFROM and field_type == DimensionTypeEnum.DATE:
            return f"'{DEFAULT_DATE_FROM}'"
        elif field.name == TechDimensionEnum.IS_ACTIVE_DIMENSION and field_type == DimensionTypeEnum.BOOLEAN:
            return "true"
        elif field_type in (DimensionTypeEnum.UUID, DimensionTypeEnum.TIMESTAMP):
            return f"{DEFAULT_TYPE_VALUES[db_type][field_type]}"
        elif field.name == DATETO and field_type == DimensionTypeEnum.DATE:
            return f"'{DEFAULT_DATE_TO}'"
        elif not field.is_key:
            return f"{DEFAULT_TYPE_VALUES[db_type][field_type]}"
        else:
            return None

    @classmethod
    def _get_drop_column_sql(cls, field_name: str) -> str:
        """
        Создать запрос на удаление колонки.
        """
        return f"DROP COLUMN IF EXISTS `{field_name}`"

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
        raise NotImplementedError

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
        raise NotImplementedError

    @classmethod
    def _get_create_table_prefix(
        cls, database_object: DatabaseObjectModel, cluster_name: Optional[str] = None, not_exists: bool = False
    ) -> str:
        """
        Создать первую часть запроса на создание таблицы.
        Возвращает строку вида "CREATE TABLE {table.schema_name}.{table.name} {on_cluster} (".
        """
        raise NotImplementedError

    @classmethod
    def get_fields_and_pks(
        cls,
        data_storage: DataStorage,
        db_type: DatabaseTypeEnum,
        ignore_functions: bool = False,
        without_date: bool = True,
        dimension_tech_fields: bool = False,
    ) -> tuple[str, list]:
        """
        Возвращает поля вместе с типами и все первичные ключи
        """
        fields = data_storage.fields
        primary_keys = []
        fields_str = ""
        if not fields:
            raise ValueError("You cannot create a table without fields.")
        for field in fields:
            if not dimension_tech_fields and field.is_tech_field:
                continue
            _, _, semantic_field_type = get_field_type_with_length(field)
            if ignore_functions and semantic_field_type in (DimensionTypeEnum.UUID, DimensionTypeEnum.TIMESTAMP):
                field_type = cls._get_table_field_type(field, db_type, True)
            else:
                field_type = cls._get_table_field_type(field, db_type)
            if field.sql_name:
                field_name = field.sql_name
            else:
                field_name = field.name
            if field.is_key and ((not without_date and field.name not in (DATEFROM, DATETO)) or without_date):
                primary_keys.append(f'"{field_name}"')
            fields_str += f'"{field_name}" {field_type},'
        return fields_str, primary_keys

    @classmethod
    def _get_create_table_request_sql_wihout_pk(
        cls,
        data_storage: DataStorage,
        database_object: DatabaseObjectModel,
        database: DatabaseModel,
        not_exists: bool = False,
        dimension_tech_fields: bool = False,
    ) -> tuple[str, list]:
        """
        Создать запрос на создание таблицы без primary_keys.
        """
        create_table_sql = cls._get_create_table_prefix(database_object, database.default_cluster_name, not_exists)
        fields_str, primary_keys = cls.get_fields_and_pks(
            data_storage, database.type, dimension_tech_fields=dimension_tech_fields
        )
        return create_table_sql + fields_str, primary_keys

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
        raise NotImplementedError

    @classmethod
    def _get_delete_db_objects_sql(
        cls, database_objects: list[DatabaseObjectModel], cluster_name: Optional[str] = None, exists: bool = True
    ) -> list[str]:
        """
        Создать запрос на удаление таблицы.
        """
        raise NotImplementedError

    @classmethod
    async def create_datastorage(
        cls,
        data_storage: DataStorage,
        model: Model,
        not_exist: bool = False,
        delete_if_failder: bool = True,
        execute_query: bool = True,
        check_possible_delete: bool = True,
    ) -> list[str]:
        """
        Создать таблицу для DataStorage в clickhouse, GreenPlum или PostgreSQL.
        """
        if not settings.ENABLE_GENERATE_OBJECTS:
            return []
        database = DatabaseModel.model_validate(model.database)
        sql_expressions = cls._get_create_table_sql(
            data_storage, database, model.name, not_exist, dimension_tech_fields=model.dimension_tech_fields
        )
        logger.debug("SQL_EXPRESSION created. sql_expression='%s'", sql_expressions)
        try:
            if execute_query and sql_expressions:
                await cls._execute_DDL(sql_expressions, database)
                logger.debug(
                    "Table for data_storage with name=%s created in database.",
                    data_storage.name,
                )
                return sql_expressions
            elif execute_query:
                logger.debug("There are no tables to create")
                return sql_expressions
            else:
                return sql_expressions
        except Exception as ext:
            logger.exception(
                "Error: creating datastorage name=%s, model=%s, tenant=%s.",
                data_storage.name,
                model.name,
                data_storage.tenant_id,
            )
            if delete_if_failder:
                logger.debug(
                    "Try deleting datastorage name=%s, model=%s, tenant=%s.",
                    data_storage.name,
                    model.name,
                    data_storage.tenant_id,
                )
                await cls.delete_datastorage(
                    data_storage, model, True, False, check_possible_delete=check_possible_delete
                )
            raise Exception(str(ext))

    @classmethod
    async def recreate_data_storage(
        cls,
        data_storage: DataStorage,
        model: Model,
        сheck_possible_to_drop_data_storage: bool = True,
        execute_query: bool = True,
    ) -> list[str]:
        """Пересоздание модели таблицы для dso."""
        db_objects = get_filtred_database_object_by_data_storage(data_storage, model.name)
        db_objects_strs = [f"{db_object.schema_name}.{db_object.name}" for db_object in db_objects]
        if сheck_possible_to_drop_data_storage:
            is_possible_drop = await cls.is_possible_to_drop_data_storage(data_storage, model)
            is_exists_fields = bool(data_storage.fields)
        else:
            is_possible_drop = True
            is_exists_fields = True
        if is_exists_fields and is_possible_drop:
            result = []
            sql_expressions_delete = await cls.delete_datastorage(
                data_storage,
                model,
                True,
                recreate_if_failed=False,
                check_possible_delete=сheck_possible_to_drop_data_storage,
                execute_query=execute_query,
            )
            result.extend(sql_expressions_delete)
            sql_expressions_create = await cls.create_datastorage(
                data_storage,
                model,
                True,
                delete_if_failder=False,
                execute_query=execute_query,
                check_possible_delete=сheck_possible_to_drop_data_storage,
            )
            result.extend(sql_expressions_create)
            return result
        elif not is_exists_fields:
            raise ValueError(f"Fields is empty for {data_storage.name}.")
        raise ValueError(f"It is impossible to recreate a tables {db_objects_strs}.")

    @classmethod
    async def update_datastorage(
        cls,
        data_storage: DataStorage,
        model: Model,
        enable_delete_column: bool = True,
        enable_delete_not_empty: bool = False,
        execute_query: bool = True,
    ) -> list[str]:
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
        is_exist = await cls.is_exist_datastorage(data_storage, model)
        if is_exist:
            is_changed, is_sql_expressions_completed, sql_expressions = (
                await cls.alter_datastorage_by_comparing_with_meta(
                    data_storage, model, enable_delete_column, enable_delete_not_empty, execute_query
                )
            )
            if (is_changed and is_sql_expressions_completed) or (
                is_changed and not is_sql_expressions_completed and sql_expressions
            ):
                if execute_query:
                    logger.info("DataStorage %s updated.", data_storage.name)
                return sql_expressions
            elif is_changed:
                logger.debug("Changes to %s datastorage cannot be applied. Check possible to drop.", data_storage.name)
                is_possible_to_drop = (
                    await cls.is_possible_to_drop_data_storage(data_storage, model)
                    if not enable_delete_not_empty
                    else True
                )
                if is_possible_to_drop:
                    logger.debug("DataStorage %s can be dropped. Let's try to recreate.", data_storage.name)
                    sql_expressions = await cls.recreate_data_storage(
                        data_storage, model, сheck_possible_to_drop_data_storage=False, execute_query=execute_query
                    )
                    logger.info("DataStorage %s recreated.", data_storage.name)
                    return sql_expressions
                else:
                    raise ValueError(f"The table for model '{model.name}' must be recreated, but it is not empty.")
        else:
            logger.debug("DataStorage %s does not exist. Let's try to recreate", data_storage.name)
            return await cls.recreate_data_storage(
                data_storage,
                model,
                execute_query=execute_query,
                сheck_possible_to_drop_data_storage=not enable_delete_not_empty,
            )
        logger.info("DataStorage %s already updated.", data_storage.name)
        return []

    @classmethod
    async def _get_delete_db_objects_sql_and_check_possible_delete(
        cls,
        database_objects: list[DatabaseObjectModel],
        database: DatabaseModel,
        exists: bool = False,
        check_possible_delete: bool = True,
    ) -> list[str]:
        """
        Получить запрос на удаление таблицы и проверить возможность её удаления.
        """
        sql_expressions = cls._get_delete_db_objects_sql(database_objects, database.default_cluster_name, exists=exists)
        logger.debug("SQL_EXPRESSIONS created. sql_expressions='%s'", sql_expressions)
        if check_possible_delete and not await cls._is_possible_to_drop(database_objects, database, True):
            raise ValueError("It is not possible to delete a non-empty table.")
        return sql_expressions

    @classmethod
    async def delete_datastorage(
        cls,
        data_storage: Optional[DataStorage],
        model: Model,
        exists: bool = False,
        recreate_if_failed: bool = True,
        database_objects_model: Optional[list[DatabaseObjectModel]] = None,
        check_possible_delete: bool = True,
        execute_query: bool = True,
    ) -> list[str]:
        """
        Удалить таблицу для DataStorage в Clickhouse, GreenPlum или PostgreSQL.
        """
        if not settings.ENABLE_GENERATE_OBJECTS:
            return []
        database = DatabaseModel.model_validate(model.database)
        if database_objects_model is None and data_storage is not None:
            database_objects_model = get_filtred_database_object_by_data_storage(data_storage, model.name)
        if database_objects_model is None:
            raise ValueError("DatabaseObjects is empty.")
        sql_expressions = await cls._get_delete_db_objects_sql_and_check_possible_delete(
            database_objects_model, database, exists, check_possible_delete
        )
        try:
            if sql_expressions and execute_query:
                await cls._execute_DDL(sql_expressions, database)
                logger.debug("database objects %s dropped from database.", database_objects_model)
                return sql_expressions
            elif execute_query:
                logger.debug("There are no tables to delete")
                return sql_expressions
            else:
                return sql_expressions
        except Exception as ext:
            logger.exception(
                "Error: deleting database_objects: %s.",
                database_objects_model,
            )
            if recreate_if_failed and data_storage:
                logger.debug("Try recreate database_objects: %s.", database_objects_model)
                _ = await cls.create_datastorage(
                    data_storage,
                    model,
                    True,
                    False,
                    check_possible_delete=check_possible_delete,
                )
            raise Exception(str(ext))

    @classmethod
    async def create_dimension(cls, dimension: Dimension, model: Model, if_not_exists: bool = False) -> None:
        """
        Создать таблицы для DataStorage'ей Dimension в Clickhouse, GreenPlum или PostgreSQL.
        """
        if not settings.ENABLE_GENERATE_OBJECTS:
            return None
        database = DatabaseModel.model_validate(model.database)
        values_data_storage = dimension.values_table
        texts_data_storage = dimension.text_table
        attributes_data_storage = dimension.attributes_table
        sql_expressions = []
        if values_data_storage:
            sql_expressions.extend(
                cls._get_create_table_sql(
                    values_data_storage,
                    database,
                    model.name,
                    if_not_exists,
                    dimension_tech_fields=model.dimension_tech_fields,
                )
            )
        if texts_data_storage:
            sql_expressions.extend(
                cls._get_create_table_sql(
                    texts_data_storage,
                    database,
                    model.name,
                    if_not_exists,
                    dimension_tech_fields=model.dimension_tech_fields,
                )
            )
        if attributes_data_storage:
            sql_expressions.extend(
                cls._get_create_table_sql(
                    attributes_data_storage,
                    database,
                    model.name,
                    if_not_exists,
                    dimension_tech_fields=model.dimension_tech_fields,
                )
            )
        logger.debug("SQL_EXPRESSIONS created. sql_expressions='%s'", sql_expressions)
        if not sql_expressions:
            return None
        try:
            await cls._execute_DDL(sql_expressions, database)
            if values_data_storage:
                logger.debug(
                    "Values table for data_storage with name=%s created in database.",
                    values_data_storage.name,
                )
            if texts_data_storage:
                logger.debug(
                    "Texts table for data_storage with name=%s created in database.",
                    texts_data_storage.name,
                )
            if attributes_data_storage:
                logger.debug(
                    "Attributes table for data_storage with name=%s created in database.",
                    attributes_data_storage.name,
                )
        except Exception as ext:
            logger.error("The query could not be completed")
            delete_expressions = []
            if values_data_storage:
                values_db_objects_model = get_filtred_database_object_by_data_storage(values_data_storage, model.name)
                delete_expressions.extend(
                    await cls._get_delete_db_objects_sql_and_check_possible_delete(
                        values_db_objects_model, database, True
                    )
                )
            if texts_data_storage:
                texts_db_objects_model = get_filtred_database_object_by_data_storage(texts_data_storage, model.name)
                delete_expressions.extend(
                    await cls._get_delete_db_objects_sql_and_check_possible_delete(
                        texts_db_objects_model, database, True
                    )
                )
            if attributes_data_storage:
                attributes_db_objects_model = get_filtred_database_object_by_data_storage(
                    attributes_data_storage, model.name
                )
                delete_expressions.extend(
                    await cls._get_delete_db_objects_sql_and_check_possible_delete(
                        attributes_db_objects_model, database, True
                    )
                )
            if delete_expressions:
                await cls._execute_DDL(delete_expressions, database)
            raise Exception(str(ext))
        return None

    @classmethod
    async def update_dimension(
        cls,
        dimension: Dimension,
        model: Model,
        prev_attributes_model: Optional[DataStorageModel],
        attributes_edit_model: Optional[DataStorageEditRequest],
        prev_texts_model: Optional[DataStorageModel],
        texts_edit_model: Optional[DataStorageEditRequest],
        prev_values_model: Optional[DataStorageModel],
        enable_delete_not_empty: bool = False,
    ) -> None:
        """
        Обновить таблицы для DataStorage'ей Dimension в Clickhouse, GreenPlum или PostgreSQL.
        """
        if not settings.ENABLE_GENERATE_OBJECTS:
            return None
        database = DatabaseModel.model_validate(model.database)
        sql_expressions = []
        if prev_attributes_model is None and dimension.attributes_table is not None:
            sql_expressions.extend(
                cls._get_create_table_sql(
                    dimension.attributes_table, database, model.name, dimension_tech_fields=model.dimension_tech_fields
                )
            )
        elif prev_attributes_model is not None and dimension.attributes_table is None:
            attributes_database_objects = get_object_filtred_by_model_name(
                prev_attributes_model.database_objects, model.name, True
            )
            sql_expressions.extend(
                await cls._get_delete_db_objects_sql_and_check_possible_delete(
                    attributes_database_objects, database, check_possible_delete=not enable_delete_not_empty
                )
            )
        elif prev_attributes_model is not None and dimension.attributes_table is not None and attributes_edit_model:
            sql_expressions.extend(
                await cls.update_datastorage(
                    dimension.attributes_table,
                    model,
                    execute_query=False,
                    enable_delete_not_empty=enable_delete_not_empty,
                )
            )

        if prev_texts_model is None and dimension.text_table is not None:
            sql_expressions.extend(
                cls._get_create_table_sql(
                    dimension.text_table, database, model.name, dimension_tech_fields=model.dimension_tech_fields
                )
            )
        elif prev_texts_model is not None and dimension.text_table is None:
            text_database_objects = get_object_filtred_by_model_name(
                prev_texts_model.database_objects, model.name, True
            )
            sql_expressions.extend(
                await cls._get_delete_db_objects_sql_and_check_possible_delete(
                    text_database_objects, database, check_possible_delete=not enable_delete_not_empty
                )
            )
        elif prev_texts_model is not None and dimension.text_table is not None and texts_edit_model:
            sql_expressions.extend(
                await cls.update_datastorage(
                    dimension.text_table,
                    model,
                    execute_query=False,
                    enable_delete_not_empty=enable_delete_not_empty,
                )
            )

        if prev_values_model is None and dimension.values_table is not None:
            sql_expressions.extend(
                cls._get_create_table_sql(
                    dimension.values_table, database, model.name, dimension_tech_fields=model.dimension_tech_fields
                )
            )
        elif prev_values_model is not None and dimension.values_table is None:
            values_database_objects = get_object_filtred_by_model_name(
                prev_values_model.database_objects, model.name, True
            )
            sql_expressions.extend(
                await cls._get_delete_db_objects_sql_and_check_possible_delete(
                    values_database_objects, database, check_possible_delete=not enable_delete_not_empty
                )
            )
        elif prev_values_model is not None and dimension.values_table is not None:
            sql_expressions.extend(
                await cls.update_datastorage(
                    dimension.values_table,
                    model,
                    execute_query=False,
                    enable_delete_not_empty=enable_delete_not_empty,
                )
            )
        logger.debug("SQL_EXPRESSIONS created. sql_expressions='%s'", sql_expressions)
        if not sql_expressions:
            return None
        await cls._execute_DDL(sql_expressions, database)
        return None

    @classmethod
    async def delete_dimension(
        cls,
        dimension: Dimension,
        model: Model,
        values_db_objects_model: Optional[list[DatabaseObjectModel]] = None,
        texts_db_objects_model: Optional[list[DatabaseObjectModel]] = None,
        attributes_db_objects_model: Optional[list[DatabaseObjectModel]] = None,
        if_exists: bool = False,
        check_possible_delete: bool = True,
    ) -> None:
        if not settings.ENABLE_GENERATE_OBJECTS:
            return None
        """
        Удалить таблицы для DataStorage'ей Dimension в Clickhouse, GreenPlum или PostgreSQL.
        """
        database = DatabaseModel.model_validate(model.database)
        values_data_storage = dimension.values_table
        texts_data_storage = dimension.text_table
        attributes_data_storage = dimension.attributes_table
        sql_expressions = []
        if values_data_storage:
            if not values_db_objects_model:
                values_db_objects_model = get_filtred_database_object_by_data_storage(values_data_storage, model.name)
            sql_expressions.extend(
                await cls._get_delete_db_objects_sql_and_check_possible_delete(
                    values_db_objects_model, database, if_exists, check_possible_delete=check_possible_delete
                )
            )
        if texts_data_storage:
            if not texts_db_objects_model:
                texts_db_objects_model = get_filtred_database_object_by_data_storage(texts_data_storage, model.name)
            sql_expressions.extend(
                await cls._get_delete_db_objects_sql_and_check_possible_delete(
                    texts_db_objects_model, database, if_exists, check_possible_delete=check_possible_delete
                )
            )
        if attributes_data_storage:
            if not attributes_db_objects_model:
                attributes_db_objects_model = get_filtred_database_object_by_data_storage(
                    attributes_data_storage, model.name
                )
            sql_expressions.extend(
                await cls._get_delete_db_objects_sql_and_check_possible_delete(
                    attributes_db_objects_model, database, if_exists, check_possible_delete=check_possible_delete
                )
            )
        logger.debug("SQL_EXPRESSIONS created. sql_expressions='%s'", sql_expressions)
        if not sql_expressions:
            return None

        try:
            await cls._execute_DDL(sql_expressions, database)
            if values_data_storage:
                logger.debug(
                    "Values table for data_storage with name=%s created in database.", values_data_storage.name
                )
            if texts_data_storage:
                logger.debug("Texts table for data_storage with name=%s created in database.", texts_data_storage.name)
            if attributes_data_storage:
                logger.debug(
                    "Attributes table for data_storage with name=%s created in database.", attributes_data_storage.name
                )
        except Exception as ext:
            logger.error("The query could not be completed")
            delete_expressions = []
            if values_data_storage:
                delete_expressions.extend(
                    cls._get_create_table_sql(
                        values_data_storage,
                        database,
                        model.name,
                        not_exists=True,
                        dimension_tech_fields=model.dimension_tech_fields,
                    )
                )
            if texts_data_storage:
                delete_expressions.extend(
                    cls._get_create_table_sql(
                        texts_data_storage,
                        database,
                        model.name,
                        not_exists=True,
                        dimension_tech_fields=model.dimension_tech_fields,
                    )
                )
            if attributes_data_storage:
                delete_expressions.extend(
                    cls._get_create_table_sql(
                        attributes_data_storage,
                        database,
                        model.name,
                        not_exists=True,
                        dimension_tech_fields=model.dimension_tech_fields,
                    )
                )
            if delete_expressions:
                await cls._execute_DDL(delete_expressions, database)
            raise Exception(str(ext))
        return None

    @classmethod
    def _get_create_view_sql(
        cls, schema_name: str, name: str, sql_expression: str, cluster_name: Optional[str] = None, replace: bool = False
    ) -> list[str]:
        """Генерация DDL, который создает или пересоздает VIEW."""
        raise NotImplementedError

    @classmethod
    def _get_delete_view_sql(cls, schema_name: str, name: str, cluster_name: Optional[str] = None) -> str:
        """Генерация DDL, который удаляет VIEW."""
        raise NotImplementedError

    @classmethod
    async def create_composite(
        cls, composite: Composite, model: Model, sql_expression: str, replace: bool = False
    ) -> Optional[list[str]]:
        """Создание представления (View) для композита в базе данных.

        Метод проверяет корректность входных данных, формирует SQL-запрос для создания
        или замены представления и выполняет его в указанной базе данных.

        Логика работы:
        1. Проверяет наличие источников данных типа CE_SCENARIO в композите.
        2. Учитывает глобальный флаг ENABLE_GENERATE_OBJECTS.
        3. Ищет объект базы данных, связанный с указанной моделью.
        4. Формирует SQL-запрос для создания/замены представления.
        5. Выполняет DDL-операцию в базе данных.
        6. Логгирует результаты выполнения операции.

        Args:
            composite (Composite): Объект композита, содержащий конфигурацию представления.
            model (Model): Модель, для которой создается представление.
            sql_expression (str): SQL-запрос, определяющий логику представления.
            replace (bool, optional): Флаг, указывающий на необходимость замены
                существующего представления. Defaults to False.

        Returns:
            Optional[str]: Сформированный SQL-запрос для создания представления,
                если операция выполнена успешно. None, если создание невозможно.

        Raises:
            ValueError: Если для модели не найден соответствующий объект базы данных
                в составе композита.


        """
        for datasource in composite.datasources:
            if datasource.type == CompositeFieldRefObjectEnum.CE_SCENARIO:
                return None
        if not settings.ENABLE_GENERATE_OBJECTS:
            return None
        database = DatabaseModel.model_validate(model.database)
        cluster_name = database.default_cluster_name
        view_name = None
        schema_name = None
        for database_object in composite.database_objects:
            model_names = {model_db_object.name for model_db_object in database_object.models}
            if model.name in model_names:
                view_name = database_object.name
                schema_name = database_object.schema_name
                break
        if not view_name or not schema_name:
            raise ValueError(f"There is no dbobject for this model ({model.name}) in the {composite.name} composite.")
        sql_expression_create = cls._get_create_view_sql(schema_name, view_name, sql_expression, cluster_name, replace)
        logger.debug("SQL_EXPRESSION created. sql_expression='%s'", sql_expression_create)
        await cls._execute_DDL(sql_expression_create, database)
        logger.debug(
            "View for composite with name=%s, schema_name=%s created in database.",
            composite.name,
            schema_name,
        )
        return sql_expression_create

    @classmethod
    async def update_composite(cls, composite: Composite, model: Model, sql_expression: str) -> Optional[list[str]]:
        """
        Пересоздаёт представление (View) для композита в базе данных.

        Проверяет источники данных композита на наличие типа CE_SCENARIO,
        пропускает выполнение при отключённой генерации объектов (ENABLE_GENERATE_OBJECTS).
        Для каждого объекта из фильтрованного списка формирует SQL-запрос на обновление View
        и выполняет его через DDL-команду.

        Args:
            composite (Composite): Объект композита, содержащий источники данных.
            model (Model): Модель с данными о базе данных.
            sql_expression (str): SQL-выражение для создания представления.

        Returns:
            Optional[list[str]]: Возвращает последний сформированный SQL-запрос или None.

        Raises:
            ValueError: Если объект базы данных не содержит schema_name.
        """
        for datasource in composite.datasources:
            if datasource.type == CompositeFieldRefObjectEnum.CE_SCENARIO:
                return None
        if not settings.ENABLE_GENERATE_OBJECTS:
            return None
        database = DatabaseModel.model_validate(model.database)
        database_objects = get_filtred_database_object_by_data_storage(composite, model.name)
        cluster_name = database.default_cluster_name
        sql_expression_update = None
        for database_object in database_objects:
            if database_object.schema_name is None:
                raise ValueError(f"database_object {database_object.name} does not contain schema_name.")
            sql_expression_update = cls._get_create_view_sql(
                database_object.schema_name, database_object.name, sql_expression, cluster_name, replace=True
            )
            logger.debug("SQL_EXPRESSION created. sql_expression='%s'", sql_expression_update)
            await cls._execute_DDL(sql_expression_update, database)
            logger.debug(
                "View for composite with name=%s, schema_name=%s updated in database.",
                database_object.name,
                database_object.schema_name,
            )
        return sql_expression_update

    @classmethod
    async def delete_composite(
        cls,
        composite: Composite,
        model: Model,
        database_objects: Optional[list[DatabaseObjectModel]] = None,
    ) -> Optional[str]:
        """
        Удаление View для композита в базе данных.

        Метод проверяет источники данных композита и выполняет удаление связанных представлений (views),
        если это разрешено настройками. Возвращает последний выполненный SQL-запрос или None.

        Args:
            composite (Composite): Объект композита, содержащий источники данных.
            model (Model): Модель базы данных, связанная с композитом.
            database_objects (Optional[list[DatabaseObjectModel]]): Список объектов базы данных для удаления.
                Если не указан, будет выполнен фильтр по хранилищу данных композита.

        Returns:
            Optional[str]: SQL-запрос, использованный для удаления последнего view, или None, если операция не выполнена.

        Raises:
            ValueError: Если объект базы данных не содержит схему (schema_name).

        """
        for datasource in composite.datasources:
            if datasource.type == CompositeFieldRefObjectEnum.CE_SCENARIO:
                return None
        if not settings.ENABLE_GENERATE_OBJECTS:
            return None
        if database_objects is None:
            database_objects = get_filtred_database_object_by_data_storage(composite, model.name)
        database = DatabaseModel.model_validate(model.database)
        cluster_name = database.default_cluster_name
        sql_expression = None
        for database_object in database_objects:
            if not database_object.schema_name:
                raise ValueError(f"database_object {database_object.name} does not contain schema_name.")
            sql_expression = cls._get_delete_view_sql(database_object.schema_name, database_object.name, cluster_name)
            logger.debug("SQL_EXPRESSION created. sql_expression='%s'", sql_expression)
            await cls._execute_DDL(sql_expression, database)
            logger.debug(
                "View for composite with name=%s, schema_name=%s deleted from database.",
                database_object.name,
                database_object.schema_name,
            )
        return sql_expression

    @classmethod
    async def alter_datastorage_by_comparing_with_meta(
        cls,
        datastorage: DataStorage,
        model: Model,
        enable_delete_column: bool = False,
        enable_delete_not_empty: bool = False,
        execute_query: bool = True,
    ) -> tuple[bool, bool, list[str]]:
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
        database = DatabaseModel.model_validate(model.database)
        database_objects = get_filtred_database_object_by_data_storage(datastorage, model.name)
        database_objects_names = get_database_object_names(database_objects)
        if not database_objects_names.table_schema or not database_objects_names.table_name:
            raise ValueError(
                f"It is not possible to update a datastorage {datastorage.name} that does not have a table"
            )
        phis_table_fields = await cls._get_table_field_description(
            database, database_objects_names.table_schema, database_objects_names.table_name
        )
        sql_expressions = []
        dso_fields = {}
        for dso_field in datastorage.fields:
            if not model.dimension_tech_fields and dso_field.is_tech_field:
                continue
            if dso_field.sql_name is None:
                raise ValueError(
                    f"it is not possible to use a field {datastorage.name}.{dso_field.name} without sqlName"
                )
            dso_fields[dso_field.sql_name] = dso_field
            with_precision = dso_field.name != DataStorageLogsFieldEnum.TIMESTAMP
            dso_field_type = cls._get_table_field_type(dso_field, database.type, True, with_precision=with_precision)
            dso_field_is_nullable = is_nullable_measure_field(dso_field)
            db_field = phis_table_fields.get(dso_field.sql_name)
            if db_field is not None:
                db_field_type = db_field["data_type"]
                db_field_is_key = db_field["is_primary_key"]
                db_field_is_nullable = db_field.get("is_nullable", False)

                if (
                    dso_field_type == db_field_type
                    and dso_field.is_key == db_field_is_key
                    and dso_field_is_nullable == db_field_is_nullable
                ):
                    continue

                if dso_field.is_key != db_field_is_key:
                    logger.debug(
                        "You cannot update the primary key (Meta is_key=%s, Db is_key=%s for field=%s). The table needs to be recreated.",
                        dso_field.is_key,
                        db_field_is_key,
                        dso_field.sql_name,
                    )
                    return True, False, []

                is_field_definition_changed = (
                    dso_field_type != db_field_type or dso_field_is_nullable != db_field_is_nullable
                )

                if is_field_definition_changed and db_field_is_key:
                    logger.debug(
                        "You cannot update the primary key type/nullability (Meta type=%s, Db type=%s, Meta nullable=%s, Db nullable=%s for field=%s). The table needs to be recreated.",
                        dso_field_type,
                        db_field_type,
                        dso_field_is_nullable,
                        db_field_is_nullable,
                        dso_field.sql_name,
                    )
                    return True, False, []

                if is_field_definition_changed:
                    sql_expressions.extend(
                        cls._get_modify_column_sql(
                            dso_field.sql_name,
                            dso_field_type,
                            cls.get_default_value_by_field(dso_field, database.type),
                            is_nullable=dso_field_is_nullable,
                            current_field_type=db_field_type,
                            current_is_nullable=db_field_is_nullable,
                        )
                    )
            elif dso_field.is_key:
                logger.debug(
                    "You cannot create primary key column (%s). The table needs to be recreated.", dso_field.sql_name
                )
                return True, False, []
            else:
                sql_expressions.append(
                    cls._get_create_column_sql(
                        dso_field.sql_name,
                        dso_field_type,
                        cls.get_default_value_by_field(dso_field, database.type),
                        is_nullable=dso_field_is_nullable,
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
                if enable_delete_not_empty or await cls._is_possible_to_drop_column(
                    database_objects, database, phis_field_name
                ):
                    sql_expressions.append(cls._get_drop_column_sql(phis_field_name))
                else:
                    return True, False, []
            else:
                logger.warning("You cannot drop column %s. Column drop is disabled.", phis_field_name)
        sql_expressions = await cls.get_alter_database_objects_sql_expressions(datastorage, model, sql_expressions)
        if sql_expressions and execute_query:
            await cls._execute_DDL(sql_expressions, database)
            return True, True, sql_expressions
        elif sql_expressions:
            return True, False, sql_expressions
        return False, False, []
