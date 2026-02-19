from http import HTTPStatus

from fastapi import APIRouter, Query
from py_common_lib.metrics import health_check_endpoint
from taskiq_redis.exceptions import ResultIsMissingError

from src.broker import broker
from src.models.responses import TaskResultResponse

router = APIRouter()


router.add_api_route("/health_check", health_check_endpoint, status_code=HTTPStatus.OK)


@router.get(
    "/tasks/info/{taskId}",
    status_code=HTTPStatus.OK,
    description="get task info by id",
)
async def get_task_info_by_id(
    task_id: str = Query(alias="taskId"),
) -> TaskResultResponse:
    try:
        result = await broker.result_backend.get_result(task_id, with_logs=True)
        response = TaskResultResponse(
            task_id=task_id,
            error=str(result.error),
            log=str(result.log),
            execution_time=result.execution_time,
            return_value=str(result.return_value) if result.return_value is not None else None,
            labels=str(result.labels) if result.labels is not None else None,
        )
    except ResultIsMissingError:
        response = TaskResultResponse(task_id=task_id, error="Task not found")

    return response
