"""
API Example key-value.
"""

import asyncio
from http import HTTPStatus
from typing import Annotated

from fastapi import APIRouter, Body, Depends, Path, Query

from src.api.dependencies import get_call_context
from src.models.responses import TaskResponse
from src.tasks.dimension import create_dimension_task, update_dimension_task
from src.utils.context import CallContext

router = APIRouter()


@router.post(
    "/tenants/{tenantName}/dimensions/",
    status_code=HTTPStatus.OK,
    description="create_datastorage",
)
async def create_dimension(
    call_context: Annotated[CallContext, Depends(get_call_context)],
    tenant_id: str = Path(alias="tenantName"),
    models_names: list[str] = Body(alias="modelNames", validation_alias="modelNames"),
    dimensions: list[str] = Body(alias="dimensions"),
    if_not_exists: bool = Query(alias="ifNotExists", default=False),
    delete_if_failder: bool = Query(alias="deleteIfFailder", default=False),
) -> list[TaskResponse]:
    tasks = [
        create_dimension_task.kiq(
            call_context=call_context.context,
            tenant_id=tenant_id,
            model_name=model_name,
            dimension_name=dimension_name,
            if_not_exists=if_not_exists,
            delete_if_failder=delete_if_failder,
        )  # type: ignore
        for dimension_name in dimensions
        for model_name in models_names
    ]
    results = await asyncio.gather(*tasks)
    return [TaskResponse(task_id=task.task_id) for task in results]


@router.patch(
    "/tenants/{tenantName}/dimensions/",
    status_code=HTTPStatus.OK,
    description="update_datastorage",
)
async def update_dimension(
    call_context: Annotated[CallContext, Depends(get_call_context)],
    tenant_id: str = Path(alias="tenantName"),
    models_names: list[str] = Body(alias="modelNames", validation_alias="modelNames"),
    dimensions: list[str] = Body(alias="dimensions"),
    enable_delete_column: bool = Query(alias="enableDeleteColumn", default=True),
    enable_delete_not_empty: bool = Query(alias="enableDeleteNotEmpty", default=False),
) -> list[TaskResponse]:
    tasks = [
        update_dimension_task.kiq(
            call_context=call_context.context,
            tenant_id=tenant_id,
            model_name=model_name,
            dimension_name=dimension_name,
            enable_delete_column=enable_delete_column,
            enable_delete_not_empty=enable_delete_not_empty,
        )  # type: ignore
        for dimension_name in dimensions
        for model_name in models_names
    ]
    results = await asyncio.gather(*tasks)
    return [TaskResponse(task_id=task.task_id) for task in results]
