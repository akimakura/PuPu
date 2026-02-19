from contextlib import nullcontext
from typing import Annotated, Any

from py_common_lib.logger import EPMPYLogger
from starlette_context import context, request_cycle_context
from taskiq import TaskiqDepends

from src.broker import broker
from src.integrations.modelling_tools_api.codegen import RespObjectStatus
from src.service.database import DatabaseService
from src.service.datastorage import DataStorageService
from src.service.dimension import DimensionService
from src.tasks.dependencies import (
    get_database_service,
    get_datastorage_service,
    get_dimension_service,
    set_datastorage_service_dependency,
    set_dimension_service_dependency,
)

logger = EPMPYLogger(__name__)


@broker.task
async def create_dimension_task(
    data_storage_service: Annotated[DataStorageService, TaskiqDepends(get_datastorage_service)],
    dimension_service: Annotated[DimensionService, TaskiqDepends(get_dimension_service)],
    database_service: Annotated[DatabaseService, TaskiqDepends(get_database_service)],
    call_context: dict[Any, Any],
    tenant_id: str,
    model_name: str,
    dimension_name: str,
    if_not_exists: bool = False,
    delete_if_failder: bool = False,
) -> list[RespObjectStatus]:
    """
    Асинхронная задача создания измерения (dimension).

    Создает новое измерение и связанное с ним хранилища данных (data storage),
    обрабатывая статус создания объектов через сервисы зависимостей.

    Args:
        data_storage_service (DataStorageService): Сервис работы с хранилищами данных
        dimension_service (DimensionService): Сервис работы с измерениями
        database_service (DatabaseService): Сервис базы данных
        call_context (dict): Контекст вызова (опционально)
        tenant_id (str): Идентификатор тенанта
        model_name (str): Имя модели
        dimension_name (str): Название создаваемого измерения
        if_not_exists (bool): Флаг проверки существования объекта перед созданием (по умолчанию `False`)
        delete_if_failed (bool): Удалять объект в случае ошибки (по умолчанию `False`)

    Returns:
        List[RespObjectStatus]: Список состояний созданных объектов после обработки статуса измерения
    """
    context_manager: Any
    if context.exists():
        context_manager = nullcontext()
    else:
        context_manager = request_cycle_context(call_context)
    with context_manager:
        set_dimension_service_dependency(dimension_service, tenant_id, model_name)
        await set_datastorage_service_dependency(data_storage_service, database_service, tenant_id, model_name)
        logger.info("Create dimension %s.%s.%s task", tenant_id, model_name, dimension_name)
        data_storage_names = await dimension_service.get_all_related_datastorage_names_by_dimension_name(dimension_name)
        object_statuses = []
        for data_storage_name in data_storage_names:
            object_statuses.extend(
                await data_storage_service.create_datastorage_by_name(
                    data_storage_name, if_not_exists, delete_if_failder
                )
            )
        result = await dimension_service.change_dimension_status_by_datastorage_status(dimension_name, object_statuses)
        return result


@broker.task
async def update_dimension_task(
    data_storage_service: Annotated[DataStorageService, TaskiqDepends(get_datastorage_service)],
    dimension_service: Annotated[DimensionService, TaskiqDepends(get_dimension_service)],
    database_service: Annotated[DatabaseService, TaskiqDepends(get_database_service)],
    call_context: dict[Any, Any],
    tenant_id: str,
    model_name: str,
    dimension_name: str,
    enable_delete_column: bool = True,
    enable_delete_not_empty: bool = False,
) -> list[RespObjectStatus]:
    """
    Асинхронная задача обновления измерения (dimension).

    Обновляет измерение и связанные с ним хранилища данных (data storage),
    обрабатывая статус создания объектов через сервисы зависимостей.

    Args:
        data_storage_service (DataStorageService): Сервис работы с хранилищами данных
        dimension_service (DimensionService): Сервис работы с измерениями
        database_service (DatabaseService): Сервис базы данных
        call_context (dict): Контекст вызова (опционально)
        tenant_id (str): Идентификатор тенанта
        model_name (str): Имя модели
        dimension_name (str): Название создаваемого измерения
        enable_delete_column (bool): Флаг разрешения удаления столбцов при обновлении (по умолчанию `True`)
        enable_delete_not_empty (bool): Флаг разрешения удаление непустых сущностей (по умолчанию `False`)

    Returns:
        List[RespObjectStatus]: Список состояний созданных объектов после обработки статуса измерения
    """
    context_manager: Any
    if context.exists():
        context_manager = nullcontext()
    else:
        context_manager = request_cycle_context(call_context)
    with context_manager:
        set_dimension_service_dependency(dimension_service, tenant_id, model_name)
        await set_datastorage_service_dependency(data_storage_service, database_service, tenant_id, model_name)
        logger.info("Create dimension %s.%s.%s task", tenant_id, model_name, dimension_name)
        data_storage_names = await dimension_service.get_all_related_datastorage_names_by_dimension_name(dimension_name)
        object_statuses = []
        for data_storage_name in data_storage_names:
            object_statuses.extend(
                await data_storage_service.update_datastorage_by_name(
                    data_storage_name, enable_delete_column, enable_delete_not_empty
                )
            )
        result = await dimension_service.change_dimension_status_by_datastorage_status(dimension_name, object_statuses)
        return result
