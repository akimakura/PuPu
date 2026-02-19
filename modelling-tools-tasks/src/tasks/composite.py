from contextlib import nullcontext
from typing import Annotated, Any

from py_common_lib.logger import EPMPYLogger
from starlette_context import context, request_cycle_context
from taskiq import TaskiqDepends

from src.broker import broker
from src.integrations.modelling_tools_api.codegen import RespObjectStatus
from src.service.composite.composite import CompositeService
from src.service.database import DatabaseService
from src.tasks.dependencies import get_composite_service, get_database_service, set_composite_service_dependency

logger = EPMPYLogger(__name__)


@broker.task
async def create_composite_task(
    tenant_id: str,
    model_name: str,
    database_service: Annotated[DatabaseService, TaskiqDepends(get_database_service)],
    composite_service: Annotated[CompositeService, TaskiqDepends(get_composite_service)],
    call_context: dict[Any, Any],
    composite_name: str,
    replace: bool = False,
) -> list[RespObjectStatus]:
    """
    Асинхронная задача создания композитного объекта.

    Args:
        tenant_id (str): Идентификатор арендатора (тенанта).
        model_name (str): Название модели, связанной с композитным объектом.
        database_service (DatabaseService): Сервис взаимодействия с базой данных.
        composite_service (CompositeService): Сервис работы с композитными объектами.
        call_context (dict[Any, Any]): Контекст вызова задачи.
        composite_name (str): Имя создаваемого композитного объекта.
        replace (bool, optional): Флаг замены существующего объекта (по умолчанию False).

    Returns:
        List[RespObjectStatus]: Список статусов созданных объектов.
    """
    context_manager: Any
    if context.exists():
        context_manager = nullcontext()
    else:
        context_manager = request_cycle_context(call_context)
    with context_manager:
        await set_composite_service_dependency(composite_service, database_service, tenant_id, model_name)
        logger.info("Create composite %s.%s.%s task", tenant_id, model_name, composite_name)
        object_status = await composite_service.create_composite_by_name_and_send_status(composite_name, replace)
        return object_status


@broker.task
async def update_composite_task(
    tenant_id: str,
    model_name: str,
    database_service: Annotated[DatabaseService, TaskiqDepends(get_database_service)],
    composite_service: Annotated[CompositeService, TaskiqDepends(get_composite_service)],
    call_context: dict[Any, Any],
    composite_name: str,
) -> list[RespObjectStatus]:
    """
    Асинхронная задача обновления композитного объекта.

    Args:
        tenant_id (str): Идентификатор арендатора (тенанта).
        model_name (str): Название модели, связанной с композитным объектом.
        database_service (DatabaseService): Сервис взаимодействия с базой данных.
        composite_service (CompositeService): Сервис работы с композитными объектами.
        call_context (dict[Any, Any]): Контекст вызова задачи.
        composite_name (str): Имя создаваемого композитного объекта.

    Returns:
        List[RespObjectStatus]: Список статусов созданных объектов.
    """
    context_manager: Any
    if context.exists():
        context_manager = nullcontext()
    else:
        context_manager = request_cycle_context(call_context)
    with context_manager:
        await set_composite_service_dependency(composite_service, database_service, tenant_id, model_name)
        logger.info("Update composite %s.%s.%s task", tenant_id, model_name, composite_name)
        object_status = await composite_service.update_composite_by_name_and_send_status(
            composite_name,
        )
        return object_status
