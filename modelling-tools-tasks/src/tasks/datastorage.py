from contextlib import nullcontext
from typing import Annotated, Any

from py_common_lib.logger import EPMPYLogger
from starlette_context import context, request_cycle_context
from taskiq import TaskiqDepends

from src.broker import broker
from src.integrations.modelling_tools_api.codegen import RespObjectStatus
from src.service.database import DatabaseService
from src.service.datastorage import DataStorageService
from src.tasks.dependencies import get_database_service, get_datastorage_service, set_datastorage_service_dependency

logger = EPMPYLogger(__name__)


@broker.task
async def create_datastorage_task(
    tenant_id: str,
    model_name: str,
    database_service: Annotated[DatabaseService, TaskiqDepends(get_database_service)],
    datastorage_service: Annotated[DataStorageService, TaskiqDepends(get_datastorage_service)],
    call_context: dict[Any, Any],
    datastorage_name: str,
    if_not_exists: bool = False,
    delete_if_failder: bool = False,
) -> list[RespObjectStatus]:
    """
    Асинхронная задача создания хранилища данных (data storage).

    Задача асинхронно создает объект хранилища данных (`data storage`), используя предоставленные сервисы
    баз данных и управления хранилищами данных. В зависимости от контекста выполнения (наличие текущего
    контекста запроса), используется контекстный менеджер, позволяющий корректно установить необходимые
    зависимости сервисов перед созданием объекта хранилища.

    Args:
        tenant_id (str): идентификатор тенанта.
        model_name (str): имя модели данных.
        database_service (DatabaseService): сервис работы с базой данных.
        datastorage_service (DataStorageService): сервис управления объектами хранилищ данных.
        call_context (dict[Any, Any]): контекст вызова задачи.
        datastorage_name (str): название создаваемого хранилища данных.
        if_not_exists (bool): флаг игнорирования ошибки, если хранилище уже существует (по умолчанию `False`).
        delete_if_failed (bool): флаг удаления неудачно созданного хранилища после сбоя операции (по умолчанию `False`)
    Returns:
        list[RespObjectStatus]: список объектов статуса выполненных операций над хранилищем данных.

    """
    context_manager: Any
    if context.exists():
        context_manager = nullcontext()
    else:
        context_manager = request_cycle_context(call_context)
    with context_manager:
        await set_datastorage_service_dependency(datastorage_service, database_service, tenant_id, model_name)
        logger.info("Create datastorage %s.%s.%s task", tenant_id, model_name, datastorage_name)
        object_status = await datastorage_service.create_datastorage_by_name_and_send_status(
            datastorage_name, if_not_exists, delete_if_failder
        )
        return object_status


@broker.task
async def update_datastorage_task(
    tenant_id: str,
    model_name: str,
    database_service: Annotated[DatabaseService, TaskiqDepends(get_database_service)],
    datastorage_service: Annotated[DataStorageService, TaskiqDepends(get_datastorage_service)],
    call_context: dict[Any, Any],
    datastorage_name: str,
    enable_delete_column: bool = True,
    enable_delete_not_empty: bool = False,
) -> list[RespObjectStatus]:
    """
    Задача асинхронной обработки обновления хранилища данных.

    Args:
        tenant_id (str): Идентификатор тенанта.
        model_name (str): Название модели, связанной с хранилищем данных.
        database_service (DatabaseService): Сервис работы с базой данных (получается через зависимость).
        datastorage_service (DataStorageService): Сервис управления хранилищами данных (получается через зависимость).
        call_context (Dict[Any, Any]): Контекст вызова задачи.
        datastorage_name (str): Имя хранилища данных для обновления.
        enable_delete_column (bool): Флаг разрешения удаления колонок при обновлении (по умолчанию True).
        enable_delete_not_empty (bool): Флаг разрешения удаления непустых объектов (по умолчанию False).

    Returns:
        List[RespObjectStatus]: Список объектов статуса выполнения операции.
    """
    context_manager: Any
    if context.exists():
        context_manager = nullcontext()
    else:
        context_manager = request_cycle_context(call_context)
    with context_manager:
        await set_datastorage_service_dependency(datastorage_service, database_service, tenant_id, model_name)
        logger.info("Update datastorage %s.%s.%s task", tenant_id, model_name, datastorage_name)
        object_status = await datastorage_service.update_datastorage_by_name_and_send_status(
            datastorage_name, enable_delete_column, enable_delete_not_empty
        )
        return object_status
