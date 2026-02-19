"""
Методы для настройки окружения в контейнере на запуске.
"""

import asyncio
import sys
from typing import Optional

from alembic import command
from alembic.config import Config
from py_common_lib.logger import EPMPYLogger
from redis import asyncio as aioredis

from src.cache import FastAPICache
from src.cache.backends.redis import RedisBackend
from src.config import GeneratorConnectionTypeEnum, settings
from src.db.engine import database_connector
from src.events.kafka import kafka_connector
from src.repository.generators.utils import get_generator
from src.repository.meta_synchronizer import GeneratorResult, MetaSynchronizerRepository
from src.repository.model import ModelRepository
from src.utils.backoff import sync_retry
from src.utils.logging.setup import setup_logging

logger: EPMPYLogger = EPMPYLogger(__name__)


async def setup_logging_configs() -> None:
    """
    Инициализация Логгера.
    """
    setup_logging(filename=settings.LOGGING_PATH)


async def setup_fastapi_cache() -> None:
    """
    Инициализация подключения к redis для кэширования данных
    """
    enable = True
    if settings.REDIS_URL in {"", None, "redis://", "rediss://", " "}:
        enable = False
        settings.REDIS_URL = "redis://"
    redis = aioredis.from_url(settings.REDIS_URL)
    FastAPICache.init(
        RedisBackend(redis),
        prefix=settings.CACHE_PREFIX,
        enable=enable,
    )
    if enable:
        await FastAPICache.clear()


async def recreate_model(
    tenant: str,
    model_name: str,
    datastorage_migration_update: bool = False,
    datastorage_migration_create: bool = False,
    composite_create_migration: bool = False,
    composite_delete_migration: bool = False,
    enable_delete_column: bool = False,
    dictionary_migration: bool = False,
    enable_delete_not_empty: bool = False,
    enable_raw_sql: Optional[bool] = False,
    ignore_data_storages: Optional[list[str]] = None,
    ignore_composites: Optional[list[str]] = None,
) -> None:
    """
    Запуск пересоздания таблиц и вьюх, привязанных к модели model_name.

    Выполняет операции по обновлению/созданию объектов базы данных (data storages, composites, словари)
    в соответствии с флагами и параметрами, переданными в вызове.

    Args:
        tenant (str): Идентификатор тенанта, в котором находится модель.
        model_name (str): Название модели, для которой выполняются операции.
        datastorage_migration_update (bool): Если True, обновляет существующие data storages.
        datastorage_migration_create (bool): Если True, создает новые data storages.
        composite_create_migration (bool): Если True, создает новые composites.
        composite_delete_migration (bool): Если True, удаляет composites.
        enable_delete_column (bool): Флаг для включения логики удаления столбцов при обновлении.
        dictionary_migration (bool): Если True, обновляет словари модели.
        ignore_data_storages (Optional[list[str]]): Список имен data storages, которые нужно игнорировать.
        ignore_composites (Optional[list[str]]): Список имен composites, которые нужно игнорировать.
        enable_delete_not_empty (bool): Если True, удаляет не пустые data storages.

    """
    _, async_session_maker = await database_connector.get_not_pg_is_in_recovery()
    if ignore_data_storages is None:
        ignore_data_storages = []
    if ignore_composites is None:
        ignore_composites = []
    composites_with_errors = []
    dictionary_with_errors = []
    data_storages_with_errors = []
    async with async_session_maker() as session:
        model_repository = ModelRepository.get_by_session(session)
        meta_sync_repository = MetaSynchronizerRepository.get_by_session(session)
        model = await model_repository.get_by_name(tenant, model_name)
        database_name = model.database_name
        setattr(settings, f"DB_{tenant}_{database_name}_TYPE".upper(), GeneratorConnectionTypeEnum.PHYSICAL)
        if enable_raw_sql:
            with open(f"{settings.PATH_TO_SCHEMA_MIGRATIONS}/static/{model_name}.sql", "r") as file:
                sqls = file.readlines()
                generator = get_generator(model)
                for sql_num, sql in enumerate(sqls):
                    logger.info("Executing sql query for %s, %s/%s", model_name, sql_num + 1, len(sqls))
                    sql = sql.strip()
                    await generator._execute_DDL(sql, model.database)
                    sql_num += 1
        if composite_delete_migration:
            deleted_composites = await meta_sync_repository.delete_composites_in_database_from_meta(
                tenant, model_name, ignore_composites, False
            )
            for composite in deleted_composites:
                if composite.result == GeneratorResult.FAILURE:
                    logger.error("Error creating composite '%s'. Reason: %s", composite.object_name, composite.msg)
                    composites_with_errors.append(composite.object_name)
        if datastorage_migration_update:
            data_storages = await meta_sync_repository.upate_datastorages_in_database_from_meta(
                tenant, model_name, ignore_data_storages, enable_delete_column, enable_delete_not_empty, False
            )
            for data_storage in data_storages:

                if data_storage.result == GeneratorResult.FAILURE:
                    logger.error(
                        "Error creating dataStorage '%s'. Reason: %s", data_storage.object_name, data_storage.msg
                    )
                    data_storages_with_errors.append(data_storage.object_name)
        elif datastorage_migration_create:
            data_storages = await meta_sync_repository.create_data_storages_in_database_from_meta(
                tenant, model_name, ignore_data_storages, False, False
            )
            for data_storage in data_storages:
                if data_storage.result == GeneratorResult.FAILURE:
                    logger.error(
                        "Error creating dataStorage '%s'. Reason: %s", data_storage.object_name, data_storage.msg
                    )
                    data_storages_with_errors.append(data_storage.object_name)
        if dictionary_migration:
            dictionaries = await meta_sync_repository.upate_dictionary_in_model(
                tenant, model_name, ignore_data_storages
            )
            for dictionary in dictionaries:
                if dictionary.result == GeneratorResult.FAILURE:
                    logger.error(
                        "Error recreating dictionary for datastorage '%s'. Reason: %s",
                        dictionary.object_name,
                        dictionary.msg,
                    )
                    dictionary_with_errors.append(dictionary.object_name)
        if composite_create_migration:
            composites = await meta_sync_repository.create_composites_in_database_from_meta(
                tenant, model_name, ignore_composites, False
            )
            for composite in composites:
                if composite.result == GeneratorResult.FAILURE:
                    logger.error("Error creating composite '%s'. Reason: %s", composite.object_name, composite.msg)
                    composites_with_errors.append(composite.object_name)
        logger.info("DataStorages with errors = %s", data_storages_with_errors)
        logger.info("Composites with errors = %s", composites_with_errors)
        logger.info("Recreate dictionary with errors = %s", dictionary_with_errors)
        return None


@sync_retry(delay=3)
def run_alembic_upgrade(alembic_cfg: Config) -> None:
    """Запускает upgrade миграции."""
    command.upgrade(alembic_cfg, settings.ALEMBIC_TARGET_REVISION)


@sync_retry(delay=3)
def run_alembic_downgrade(alembic_cfg: Config) -> None:
    """Запускает downgrade миграции."""
    command.downgrade(alembic_cfg, settings.ALEMBIC_TARGET_REVISION)


def run_migrations(
    models: Optional[list[str]] = None,
    datastorage_migration_update: bool = False,
    datastorage_migration_create: bool = False,
    enable_delete_column: bool = False,
    composite_create_migration: bool = False,
    composite_delete_migration: bool = False,
    dictionary_migration: bool = False,
    enable_delete_not_empty: bool = False,
    enable_raw_sql: bool = False,
) -> None:
    """
    Запуск миграций базы данных и объектов модели.

    Args:
        models (Optional[list[str]]): Список моделей, для которых необходимо выполнить миграции.
            Если не указано, используется пустой список.
        datastorage_migration_update (bool): Флаг для обновления существующих хранилищ данных.
        datastorage_migration_create (bool): Флаг для пересоздания хранилищ данных.
        enable_delete_column (bool): Флаг для включения удаления колонок при миграции.
        composite_create_migration (bool): Флаг для создания композитов.
        composite_delete_migration (bool): Флаг для удаления композитов.
        dictionary_migration (bool): Флаг для пересоздания словарей.
        enable_delete_not_empty (bool): Флаг для удаления объектов даже если они не пустые.
    """
    if models is None:
        models = []
    if settings.ENABLE_SCHEMA_MIGRATIONS or "schema" in models:
        alembic_cfg = Config(settings.PATH_TO_ALEMBIC_INI_SCHEMA)
        alembic_cfg.set_main_option("script_location", settings.PATH_TO_SCHEMA_MIGRATIONS)
        if settings.ALEMBIC_ACTION == "upgrade":
            run_alembic_upgrade(alembic_cfg)
        elif settings.ALEMBIC_ACTION == "downgrade":
            run_alembic_downgrade(alembic_cfg)
        else:
            logger.error("ALEMBIC_ACTION setting has wrong definition, please define one of: downgrade, upgrade")
            sys.exit(1)
    for model in models:
        tenant = getattr(settings, f"{model}_MIGRATIONS_TENANT_NAME".upper(), "tenant1")
        if model == "schema":
            continue
        if (
            not datastorage_migration_update
            and not composite_create_migration
            and not datastorage_migration_create
            and not composite_delete_migration
            and not dictionary_migration
            and not enable_raw_sql
        ):
            logger.error(
                "Please specify at least one of the types of objects to migrate %s",
                "(-cd - delete composites, -cc - recreate composites, -du - update dataStorages, -dc - create dataStorages, -rd - recreate dictionary -rs - raw sql commands)",
            )
            sys.exit(1)
        if datastorage_migration_update and datastorage_migration_create:
            logger.error(
                "Only one operation with datastorage can be performed at a time (Select only one item: -du or -dc)"
            )
            sys.exit(1)
        asyncio.run(
            recreate_model(
                tenant,
                model,
                datastorage_migration_update,
                datastorage_migration_create,
                composite_create_migration,
                composite_delete_migration,
                enable_delete_column,
                dictionary_migration,
                enable_delete_not_empty,
                enable_raw_sql,
            )
        )
    return None


async def start_kafka_producer() -> None:
    """Запускает kafka producer."""
    if settings.ENABLE_KAFKA:
        logger.info("Launching the kafka producer.")
        kafka_connector.init_producer()
        producer = kafka_connector.get_producer()
        await producer.start()
        logger.info("The kafka producer has been launched.")


async def stop_kafka_producer() -> None:
    """Останавливает kafka producer."""
    if settings.ENABLE_KAFKA:
        logger.info("Stopping the kafka producer.")
        producer = kafka_connector.get_producer()
        await producer.stop()
        logger.info("The kafka producer has been stopped.")
