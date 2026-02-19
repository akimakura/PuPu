from http import HTTPStatus
from typing import Annotated

from fastapi import APIRouter, Body, Depends, Path
from fastapi.security import HTTPAuthorizationCredentials
from py_common_lib.metrics import health_check_endpoint
from py_common_lib.permissions import PermissionChecker
from py_common_lib.permissions.permissions_checker import http_bearer
from starlette.requests import Request
from starlette.responses import JSONResponse

from src.api.dependencies import get_relations_service
from src.api.internal.dependencies import get_permissions_service
from src.cache import FastAPICache
from src.models.model_relations import ChangeObjectStatusRequest, ChangeObjectStatusResponse
from src.models.permissions import ALL_PERMISSIONS, PermissionEnum
from src.service.permissions import PermissionsService
from src.service.relations import RelationsService
from src.utils.hide_endpoint_decorator import hide_endpoint

router = APIRouter()


@router.post(
    "/invalidate_caches",
    status_code=HTTPStatus.OK,
    description="Очистить весь кэш приложения",
    dependencies=[Depends(PermissionChecker(required_permissions=ALL_PERMISSIONS))],
)
@hide_endpoint("HIDE_INVALIDATE_CACHES")
async def invalidate_caches(request: Request) -> JSONResponse:
    """Очистить весь кэш приложения."""
    await FastAPICache.clear()
    return JSONResponse("ok")


@router.get(
    "/semanticPermissions",
    status_code=HTTPStatus.OK,
    description="Получить список разрешений токена",
    response_model=list[PermissionEnum],
)
@hide_endpoint("HIDE_GET_SEMANTIC_PERMISSIONS")
async def get_semantic_permissions(
    request: Request,
    service: Annotated[PermissionsService, Depends(get_permissions_service)],
    token: HTTPAuthorizationCredentials = Depends(http_bearer),
) -> list[PermissionEnum]:
    """Получить пермишены семантического слоя из auth-proxy"""
    permissions = await service.get_permissions(token)
    return permissions


@router.post(
    "/tenants/{tenantName}/changeObjectStatus",
    status_code=HTTPStatus.OK,
    description="Обновить статус у объектов",
    response_model=list[ChangeObjectStatusResponse],
)
@hide_endpoint("HIDE_CHANGE_OBJECT_STATUS")
async def change_object_status(
    request: Request,
    service: Annotated[RelationsService, Depends(get_relations_service)],
    tenant_id: str = Path(alias="tenantName"),
    statuses: list[ChangeObjectStatusRequest] = Body(),
) -> list[ChangeObjectStatusResponse]:
    """
    Обновляет статусы указанных объектов.

    Args:
        request (Request): Запрос от клиента.
        service (RelationsService): Сервис для работы с отношениями объектов.
        tenant_id (str): Идентификатор арендатора (tenant).
        statuses (list[ChangeObjectStatusRequest]): Список запросов на изменение статуса объекта.

    Returns:
        list[ChangeObjectStatusResponse]: Список результатов изменения статуса каждого объекта.
    """
    response = await service.update_relations_status(tenant_id, statuses)
    return response


router.add_api_route("/health_check", health_check_endpoint, status_code=HTTPStatus.OK)
