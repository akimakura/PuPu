from typing import Annotated

from py_common_lib.starlette_context_plugins import AuthorizationPlugin
from py_common_lib.utils.headers import get_standard_headers
from starlette_context import context
from taskiq import TaskiqDepends

from src.dependencies import get_mt_api_client
from src.integrations.modelling_tools_api.codegen import (
    ApiClient as MTApiClient,
    Database,
    InternalApi,
    V1Api,
)
from src.models.database import DatabaseTypeEnum
from src.repository.table import TableClickhouseRepository, TablePostgreSqlRepository, TableRepository
from src.repository.view import ViewClickhouseRepository, ViewPostgreSQLRepository, ViewRepository
from src.service.composite.composite import CompositeService
from src.service.composite.sql_generator import CompositeSQLGenerator
from src.service.database import DatabaseService
from src.service.datastorage import DataStorageService
from src.service.dimension import DimensionService
from src.service.meta_sync import MetaSyncService


def get_database_service(
    mt_api_client: Annotated[MTApiClient, TaskiqDepends(get_mt_api_client)],
) -> DatabaseService:
    """
    Получение сервиса для работы с базами данных

    Args:
        mt_api_client (ApiClient): клиент для работы с API Modelling Tools

    Returns:
        DatabaseService: сервис для работы с базами данных
    """
    mt_api_v1_client = V1Api(mt_api_client)
    return DatabaseService(mt_api_v1_client)


def get_datastorage_service(
    mt_api_client: Annotated[MTApiClient, TaskiqDepends(get_mt_api_client)],
) -> DataStorageService:
    """
    Получение сервиса для работы с хранилищами данных

    Args:
        mt_api_client (ApiClient): клиент для работы с API Modelling Tools

    Returns:
        DatabaseService: сервис для работы с хранилищами данных
    """
    mt_api_v1_client = V1Api(mt_api_client)
    mt_api_internal_client = InternalApi(mt_api_client)
    return DataStorageService(
        mt_api_v1_client=mt_api_v1_client,
        mt_internal_api_client=mt_api_internal_client,
    )


def get_composite_service(
    mt_api_client: Annotated[MTApiClient, TaskiqDepends(get_mt_api_client)],
) -> CompositeService:
    """
    Получение сервиса для работы с композитными объектами

    Args:
        mt_api_client (ApiClient): клиент для работы с API Modelling Tools

    Returns:
        DatabaseService: сервис для работы с композитными объектами
    """
    mt_api_v1_client = V1Api(mt_api_client)
    mt_api_internal_client = InternalApi(mt_api_client)
    return CompositeService(
        mt_api_v1_client=mt_api_v1_client,
        mt_internal_api_client=mt_api_internal_client,
    )


def get_table_repository(tenant_id: str, database: Database) -> TableRepository:
    """
    Получение репозитория для работы с таблицами в зависимости от типа БД.

    Args:
        tenant_id (str): идентификатор тенанта
        database (Database): объект базы данных

    Returns:
        TableRepository: репозиторий для работы с таблицами
    """
    if database.type == DatabaseTypeEnum.CLICKHOUSE:
        return TableClickhouseRepository(tenant_id, database)
    if database.type in {DatabaseTypeEnum.POSTGRESQL, DatabaseTypeEnum.GREENPLUM}:
        return TablePostgreSqlRepository(tenant_id, database)
    raise ValueError("Unsupported database type: {}".format(database.type))


def get_view_repository(tenant_id: str, database: Database) -> ViewRepository:
    """
    Получение репозитория для работы с представлениями в зависимости от типа БД.

    Args:
        tenant_id (str): идентификатор тенанта
        database (Database): объект базы данных

    Returns:
        ViewRepository: репозиторий для работы с представлениями
    """
    if database.type == DatabaseTypeEnum.CLICKHOUSE:
        return ViewClickhouseRepository(tenant_id, database)
    if database.type in {DatabaseTypeEnum.POSTGRESQL, DatabaseTypeEnum.GREENPLUM}:
        return ViewPostgreSQLRepository(tenant_id, database)
    raise ValueError("Unsupported database type: {}".format(database.type))


async def set_datastorage_service_dependency(
    datastorage_service: DataStorageService, database_service: DatabaseService, tenant_id: str, model_name: str
) -> None:
    """
    Установка зависимостей для сервиса DataStorageService.

    Args:
        datastorage_service (DataStorageService): сервис для работы с хранилищами данных
        database_service (DatabaseService): сервис для работы с базами данных
        tenant_id (str): идентификатор тенанта
        model_name (str): имя модели
    """
    set_headers(datastorage_service.mt_internal_api_client)
    set_headers(datastorage_service.mt_api_v1_client)
    set_headers(database_service.mt_api_v1_client)
    database = await database_service.get_database(tenant_id=tenant_id, model_name=model_name)
    datastorage_service.table_repository = get_table_repository(tenant_id, database)
    datastorage_service.tenant_id = tenant_id
    datastorage_service.model_name = model_name


async def set_composite_service_dependency(
    composite_service: CompositeService, database_service: DatabaseService, tenant_id: str, model_name: str
) -> None:
    """
    Установка зависимостей для сервиса CompositeService.

    Args:
        composite_service (CompositeService): сервис для работы с композитами
        database_service (DatabaseService): сервис для работы с базами данных
        tenant_id (str): идентификатор тенанта
        model_name (str): имя модели
    """
    set_headers(database_service.mt_api_v1_client)
    set_headers(composite_service.mt_internal_api_client)
    set_headers(composite_service.mt_api_v1_client)
    database = await database_service.get_database(tenant_id=tenant_id, model_name=model_name)
    composite_service.view_repository = get_view_repository(tenant_id, database)
    composite_service.tenant_id = tenant_id
    composite_service.model_name = model_name
    composite_service.sql_generator = CompositeSQLGenerator(
        model_name, tenant_id, composite_service.mt_api_v1_client, DatabaseTypeEnum(database.type)
    )


async def get_dimension_service(
    mt_api_client: Annotated[MTApiClient, TaskiqDepends(get_mt_api_client)],
) -> DimensionService:
    """
    Получение сервиса для работы с измерениями

    Args:
        mt_api_client (ApiClient): клиент для работы с API Modelling Tools
    Returns:
        DimensionService: сервис для работы с измерениями
    """
    mt_api_v1_client = V1Api(mt_api_client)
    mt_api_internal_client = InternalApi(mt_api_client)
    return DimensionService(
        mt_api_v1_client=mt_api_v1_client,
        mt_internal_api_client=mt_api_internal_client,
    )


def set_headers(client: V1Api | InternalApi) -> None:
    """
    Установиить заголовки для Rest клиента

    Args:
        client (V1Api | InternalApi): клиент для работы с API Modelling Tools
    """
    headers = get_standard_headers()
    headers.update({"Authorization": context.get(AuthorizationPlugin.key, "")})
    client.api_client.default_headers.update(headers)


def set_dimension_service_dependency(dimension_service: DimensionService, tenant_id: str, model_name: str) -> None:
    """
    Установка зависимостей для сервиса DimensionService.

    Args:
        dimension_service (DimensionService): сервис для работы с измерениями
        tenant_id (str): идентификатор тенанта
        model_name (str): имя модели
    """
    set_headers(dimension_service.mt_api_v1_client)
    set_headers(dimension_service.mt_internal_api_client)
    dimension_service.tenant_id = tenant_id
    dimension_service.model_name = model_name


def get_meta_sync_service() -> MetaSyncService:
    """
    Получение сервиса для синхронизации моделей

    Returns:
        MetaSyncService: сервис для синхронизации моделей
    """
    return MetaSyncService()


async def set_meta_sync_service_dependency(
    meta_sync_service: MetaSyncService,
    database_service: DatabaseService,
    datastorage_service: DataStorageService,
    dimension_service: DimensionService,
    composite_service: CompositeService,
    tenant_id: str,
    model_name: str,
) -> None:
    """
    Установка зависимостей для сервиса MetaSyncService.

    Args:
        meta_sync_service (MetaSyncService): сервис для синхронизации моделей
        database_service (DatabaseService): сервис для работы с базами данных
        datastorage_service (DataStorageService): сервис для работы с хранилищами данных
        dimension_service (DimensionService): сервис для работы с измерениями
        composite_service (CompositeService): сервис для работы с композитами
        tenant_id (str): идентификатор тенанта
        model_name (str): имя модели
    """
    set_dimension_service_dependency(dimension_service, tenant_id, model_name)
    await set_datastorage_service_dependency(datastorage_service, database_service, tenant_id, model_name)
    await set_composite_service_dependency(composite_service, database_service, tenant_id, model_name)
    meta_sync_service.model_name = model_name
    meta_sync_service.tenant_id = tenant_id
    meta_sync_service.composite_service = composite_service
    meta_sync_service.datastorage_service = datastorage_service
    meta_sync_service.dimension_service = dimension_service
