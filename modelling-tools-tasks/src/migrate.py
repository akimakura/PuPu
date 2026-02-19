import asyncio
import uuid
from contextlib import nullcontext
from typing import Any

from py_common_lib.logger import EPMPYLogger
from starlette_context import context, request_cycle_context

from src.broker import with_task_broker
from src.config import settings
from src.dependencies.dependencies import mt_api_client_context
from src.integrations.modelling_tools_api.codegen import V1Api
from src.models.args import ArgsModel
from src.models.responses import TaskResponse
from src.tasks.meta_sync import sync_model
from src.utils.validators import get_not_empty_or_raise

logger = EPMPYLogger(__name__)


def get_migration_call_context() -> dict[str, str]:
    """Генерирует контекст вызова для миграции, включающий trace id и span id."""
    return {
        "x-b3-traceid": uuid.uuid4().hex,
        "x-b3-spanid": uuid.uuid4().hex[:16],
    }


async def get_model_names_to_migrate_or_raise(tenant: str, args: ArgsModel) -> list[str]:
    """
    Получает список моделей для миграции либо возвращает исключение,
    если модели отсутствуют.

    Если аргумент `args.model` пуст, запрашивает полный список моделей через API.

    Args:
        tenant (str): Имя тенанта.
        args (ArgsModel): Аргументы командной строки или конфигурации.

    Raises:
        ValueError: Если после получения списка моделей оказывается, что мигрировать нечего.

    Returns:
        list[str]: Список названий моделей для миграции.
    """
    models_names: list[str] = args.model or []

    if not models_names:
        async with mt_api_client_context() as client:
            v1_client = V1Api(client)
            models = await v1_client.get_models(tenant_name=tenant)
            models_names = [model.name for model in models]
    models_names = get_not_empty_or_raise(models_names, custom_raise_text="No models to migrate")
    logger.info("Models to migrate: %s", models_names)
    return models_names


def prepare_args(args: ArgsModel) -> None:
    """
    Устанавливает аргументы по умолчанию, если ни один из специальных флагов не указан.

    Args:
        args (ArgsModel): Объект аргументов миграции.
    """
    if (
        not args.composite_delete
        and not args.composite_create
        and not args.datastorage_update
        and not args.datastorage_create
        and not args.with_delete_not_empty
        and not args.with_delete_columns
        and not args.recreate_dictionry
        and not args.raw_sql
    ):
        args.composite_delete = False
        args.composite_create = False
        args.datastorage_update = True
        args.with_delete_columns = True
        args.with_delete_not_empty = True
        args.recreate_dictionry = False
        args.raw_sql = False


@with_task_broker()
async def run_migrations(args: ArgsModel, context: dict[str, str]) -> list[TaskResponse]:
    """
    Запуск миграции базы данных клиентов асинхронно.

    Args:
        args (ArgsModel): Объект аргументов миграции, содержащий настройки выполнения миграций.
        context (dict[str, str]): Контекст выполнения миграции.

    Returns:
        List[TaskResponse]: Список результатов выполненных миграционных операций.
    """
    logger.info("Run migration mode")
    tenant = args.tenant or settings.DEFAULT_MIGRATE_TENANT_NAME
    models_names = await get_model_names_to_migrate_or_raise(tenant, args)
    tasks = [
        sync_model.kiq(
            tenant_id=tenant,
            model_name=model_name,
            call_context=context,
            composite_delete=args.composite_delete,
            composite_create=args.composite_create,
            datastorage_update=args.datastorage_update,
            datastorage_create=args.datastorage_create,
            with_delete_columns=args.with_delete_columns,
            with_delete_not_empty=args.with_delete_not_empty,
            recreate_dictionry=args.recreate_dictionry,
            force_datastorages=args.force_datastorages,
            force_composites=args.force_composites,
            force_dimensions=args.force_dimensions,
        )  # type: ignore
        for model_name in models_names
    ]
    gather_tasks = asyncio.gather(*tasks)
    results = await gather_tasks
    return [TaskResponse(task_id=task.task_id) for task in results]


async def run(args: ArgsModel) -> list[TaskResponse]:
    """
    Запуск миграции базы данных клиентов асинхронно.

    Args:
        args (ArgsModel): Объект аргументов миграции, содержащий настройки выполнения миграций.
    Returns:
        List[TaskResponse]: Список результатов выполненных миграционных операций.
    """
    prepare_args(args)
    call_context = get_migration_call_context()
    context_manager: Any
    if context.exists():
        context_manager = nullcontext()
    else:
        context_manager = request_cycle_context(call_context)
    with context_manager:
        result = await run_migrations(args, call_context)
        logger.info("Migration result: %s", result)
        return result
