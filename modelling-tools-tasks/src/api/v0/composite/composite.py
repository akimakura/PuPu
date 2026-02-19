"""
API Example key-value.
"""

import asyncio
from http import HTTPStatus
from typing import Annotated

from fastapi import APIRouter, Body, Depends, Path, Query

from src.api.dependencies import get_call_context
from src.models.responses import TaskResponse
from src.tasks.composite import create_composite_task, update_composite_task
from src.utils.context import CallContext

router = APIRouter()


@router.post(
    "/tenants/{tenantName}/composites/",
    status_code=HTTPStatus.OK,
    description="create_composite",
)
async def create_composite(
    call_context: Annotated[CallContext, Depends(get_call_context)],
    tenant_id: str = Path(alias="tenantName"),
    models_names: list[str] = Body(alias="modelNames", validation_alias="modelNames"),
    composites: list[str] = Body(alias="composites"),
    replace: bool = Query(alias="replace", default=False),
) -> list[TaskResponse]:
    tasks = [
        create_composite_task.kiq(  # type: ignore
            call_context=call_context.context,
            tenant_id=tenant_id,
            model_name=model_name,
            composite_name=composite_name,
            replace=replace,
        )
        for composite_name in composites
        for model_name in models_names
    ]
    results = await asyncio.gather(*tasks)
    return [TaskResponse(task_id=task.task_id) for task in results]


@router.patch(
    "/tenants/{tenantName}/composites/",
    status_code=HTTPStatus.OK,
    description="update_composite",
)
async def update_composite(
    call_context: Annotated[CallContext, Depends(get_call_context)],
    tenant_id: str = Path(alias="tenantName"),
    models_names: list[str] = Body(alias="modelNames", validation_alias="modelNames"),
    composites: list[str] = Body(alias="composites"),
) -> list[TaskResponse]:
    tasks = [
        update_composite_task.kiq(  # type: ignore
            call_context=call_context.context,
            tenant_id=tenant_id,
            model_name=model_name,
            composite_name=composite_name,
        )
        for composite_name in composites
        for model_name in models_names
    ]
    results = await asyncio.gather(*tasks)
    return [TaskResponse(task_id=task.task_id) for task in results]
