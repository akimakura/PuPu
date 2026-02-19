from contextlib import nullcontext
from typing import Annotated, Any

from py_common_lib.logger import EPMPYLogger
from starlette_context import context, request_cycle_context
from taskiq import TaskiqDepends

from src.broker import broker
from src.integrations.modelling_tools_api.codegen import RespObjectStatus
from src.service.composite.composite import CompositeService
from src.service.database import DatabaseService
from src.service.datastorage import DataStorageService
from src.service.dimension import DimensionService
from src.service.meta_sync import MetaSyncService
from src.tasks.dependencies import (
    get_composite_service,
    get_database_service,
    get_datastorage_service,
    get_dimension_service,
    get_meta_sync_service,
    set_meta_sync_service_dependency,
)

logger = EPMPYLogger(__name__)


@broker.task
async def sync_model(
    data_storage_service: Annotated[DataStorageService, TaskiqDepends(get_datastorage_service)],
    dimension_service: Annotated[DimensionService, TaskiqDepends(get_dimension_service)],
    database_service: Annotated[DatabaseService, TaskiqDepends(get_database_service)],
    composite_service: Annotated[CompositeService, TaskiqDepends(get_composite_service)],
    meta_sync_service: Annotated[MetaSyncService, TaskiqDepends(get_meta_sync_service)],
    tenant_id: str,
    model_name: str,
    call_context: dict[Any, Any],
    composite_delete: bool = False,
    composite_create: bool = False,
    datastorage_update: bool = False,
    datastorage_create: bool = False,
    with_delete_columns: bool = False,
    with_delete_not_empty: bool = False,
    recreate_dictionry: bool = False,
    force_composites: bool = False,
    force_datastorages: bool = False,
    force_dimensions: bool = False,
) -> list[RespObjectStatus]:
    """
    Асинхронная задача синхронизации модели.

    Задача получает различные сервисы через зависимости (TaskiqDepends), выполняет предварительные проверки контекста запроса,
    устанавливает зависимость `meta_sync_service` и запускает процесс синхронизации метаданных модели.

    Args:
        data_storage_service (DataStorageService): Сервис работы с хранилищем данных.
        dimension_service (DimensionService): Сервис работы с измерениями.
        database_service (DatabaseService): Сервис взаимодействия с базой данных.
        composite_service (CompositeService): Сервис композитных объектов.
        meta_sync_service (MetaSyncService): Основной сервис синхронизации метаданных.
        tenant_id (str): Идентификатор тенанта.
        model_name (str): Название модели для синхронизации.
        call_context (Dict[Any, Any]): Контекст вызова задачи.
        composite_delete (bool, optional): Флаг удаления композитных объектов. Defaults to False.
        composite_create (bool, optional): Флаг создания композитных объектов. Defaults to False.
        datastorage_update (bool, optional): Флаг включает режим обновления хранилища данных. Defaults to False.
        datastorage_create (bool, optional): Флаг включает режим пересоздания хранилищ данных. Defaults to False.
        with_delete_columns (bool, optional): Флаг разрешает удалять столбцы при обновлении моделей. Defaults to False.
        with_delete_not_empty (bool, optional): Флаг разрешает удаление непустых сущностей. Defaults to False.
        recreate_dictionry (bool, optional): Флаг пересоздания справочников. Defaults to False.
        force_composites (bool, optional): Принудительное обновление композитных объектов. Defaults to False.
        force_datastorages (bool, optional): Принудительное обновление хранилищ данных. Defaults to False.
        force_dimensions (bool, optional): Принудительное обновление измерений. Defaults to False.

    Returns:
        List[RespObjectStatus]: Список статусов обработки объектов после завершения операции синхронизации.
    """
    context_manager: Any

    if context.exists():
        context_manager = nullcontext()
    else:
        context_manager = request_cycle_context(call_context)
    with context_manager:
        await set_meta_sync_service_dependency(
            meta_sync_service,
            database_service,
            data_storage_service,
            dimension_service,
            composite_service,
            tenant_id,
            model_name,
        )
        logger.info("Run sync model task. Model namae %s", model_name)
        await meta_sync_service.sync_meta(
            composite_delete_flag=composite_delete,
            composite_create_flag=composite_create,
            datastorage_update_flag=datastorage_update,
            datastorage_create_flag=datastorage_create,
            with_delete_columns_flag=with_delete_columns,
            with_delete_not_empty_flag=with_delete_not_empty,
            recreate_dictionry_flag=recreate_dictionry,
            force_composites=force_composites,
            force_datastorages=force_datastorages,
            force_dimensions=force_dimensions,
        )
    return []
