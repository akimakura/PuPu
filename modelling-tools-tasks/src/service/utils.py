import asyncio

from py_common_lib.logger import EPMPYLogger

from src.config import settings
from src.integrations.modelling_tools_api.codegen import (
    InternalApi,
    ObjectStatus,
    ObjectType,
    RespObjectStatus,
    Result,
)
from src.models.database_object import DatabaseObjectGenerationResult

logger = EPMPYLogger(__name__)


def get_change_object_status(
    model_name: str, object_type: ObjectType, object_name: str, change_result: list[DatabaseObjectGenerationResult]
) -> list[ObjectStatus]:
    """
    Формирует список объектов состояния (ObjectStatus), отражающих результат изменения объекта базы данных.

    Args:
        model_name (str): Название модели.
        object_type (ObjectType): Тип объекта.
        object_name (str): Имя объекта.
        change_result (List[DatabaseObjectGenerationResult]): Список результатов изменений структуры таблицы.

    Returns:
        List[ObjectStatus]: Список объектов статуса, каждый из которых содержит статус выполнения операции над таблицей и общее состояние хранилища данных.
    """
    result = []
    errors = []
    change_result_dict: dict[str, ObjectStatus] = {}
    for change_result_item in change_result:
        if change_result_item.table.name not in change_result_dict:
            change_result_dict[change_result_item.table.name] = ObjectStatus.model_validate({
                "schemaName": change_result_item.table.schema_name,
                "objectName": change_result_item.table.name,
                "modelName": model_name,
                "objectType": ObjectType.DATABASE_OBJECT,
                "status": Result.SUCCESS if change_result_item.error is None else Result.FAILURE,
                "msg": change_result_item.error,
            })
        else:
            change_result_dict[change_result_item.table.name].status = (
                Result.SUCCESS
                if change_result_item.error is None and change_result_dict[change_result_item.table.name].msg is None
                else Result.FAILURE
            )
            msg = change_result_dict[change_result_item.table.name].msg
            if msg is None:
                change_result_dict[change_result_item.table.name].msg = change_result_item.error
            elif msg is not None:
                change_result_dict[change_result_item.table.name].msg = f"{msg}; {change_result_item.error}"
    for object_status in change_result_dict.values():
        result.append(object_status)
        if object_status.msg:
            errors.append(object_status.msg)
    result_datastorage_status = ObjectStatus.model_validate({
        "schemaName": None,
        "objectName": object_name,
        "modelName": model_name,
        "objectType": object_type,
        "status": Result.SUCCESS if not errors else Result.FAILURE,
        "msg": None if not errors else "; ".join(errors),
    })
    result.append(result_datastorage_status)
    return result


async def process_chunk_change_model_status(
    tenant_id: str, mt_internal_api_client: InternalApi, chunk: list[ObjectStatus]
) -> list[RespObjectStatus]:
    """
    Обрабатывает изменение статуса объектов модели для переданного чанка (сегмента).

    Args:
        tenant_id (str): Идентификатор клиента/арендатора системы.

        mt_internal_api_client (InternalApi): Экземпляр класса клиентского API внутренней службы MT.

        chunk (list[ObjectStatus]): Список объектов ObjectStatus, представляющих элементы, статус которых нужно изменить.

    Returns:
        list[RespObjectStatus]: Список объектов RespObjectStatus, содержащий результаты изменения статуса каждого объекта.
            В случае ошибки возвращается пустой список.
    """
    try:
        result = await mt_internal_api_client.change_object_status_in_model(
            tenant_id, chunk, _request_timeout=settings.MT_API_TIMEOUT
        )
        return result
    except Exception as e:
        logger.error(f"Error processing chunk: {str(e)}")
        return []


async def change_model_status(
    tenant_id: str, model_name: str, mt_internal_api_client: InternalApi, change_status_request: list[ObjectStatus]
) -> list[RespObjectStatus]:
    """
    Изменяет статус объектов в модели через API.

    Args:
        tenant_id (str): Идентификатор тенанта.
        model_name (str): Имя модели.
        mt_internal_api_client ('InternalApi'): Клиент внутреннего API для взаимодействия с моделью.
        change_status_request (List['ObjectStatus']): Список запросов на изменение статуса объекта.

    Returns:
        List['RespObjectStatus']: Список результатов выполнения запроса изменения статуса объектов.
    """
    if not change_status_request:
        logger.debug("No change status request")
        return []
    count_pending = 0
    count_failed = 0
    count_success = 0
    for change_status in change_status_request:
        match change_status.status:
            case Result.PENDING:
                count_pending += 1
                logger.warning(
                    "The PENDING %s remains. Name: %s. Model: %s, Schema: %s, Msg: %s",
                    change_status.object_type,
                    change_status.object_name,
                    model_name,
                    change_status.schema_name,
                    change_status.msg,
                )
            case Result.FAILURE:
                count_failed += 1
                logger.error(
                    "The FAILURE %s remains. Name: %s. Model: %s, Schema: %s, Msg: %s",
                    change_status.object_type,
                    change_status.object_name,
                    model_name,
                    change_status.schema_name,
                    change_status.msg,
                )
            case Result.SUCCESS:
                count_success += 1

    logger.info(
        "Send object status change request. Success: %s, Pending: %s, Failed: %s, Total: %s",
        count_success,
        count_pending,
        count_failed,
        len(change_status_request),
    )
    chunk_size = 1000
    chunks = [change_status_request[i : i + chunk_size] for i in range(0, len(change_status_request), chunk_size)]
    responses = await asyncio.gather(*[
        process_chunk_change_model_status(tenant_id, mt_internal_api_client, chunk) for chunk in chunks
    ])
    results = []
    for response in responses:
        results.extend(response)
    logger.info("All status updates completed.")
    return results
