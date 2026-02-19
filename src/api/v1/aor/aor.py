"""
API для моделей.
"""

from http import HTTPStatus
from typing import Annotated

from fastapi import APIRouter, Body, Depends, Path, Request
from py_common_lib.logger import EPMPYLogger, audit_types
from py_common_lib.logger.enums import StatusType
from py_common_lib.permissions import PermissionChecker

from src.api.v1.aor.dependencies import get_aor_service
from src.api.v1.const import AOR_URL, MODEL_NAME_URL, TENANT_NAME_URL
from src.integration.aor.model import PushAorCommand
from src.models.aor import CreateAorRequest, CreateModelAorRequest
from src.models.permissions import PermissionEnum
from src.service.aor import AorService
from src.utils.auth import api_key_auth
from src.utils.exceptions import HTTPExceptionWithAuditLogging
from src.utils.hide_endpoint_decorator import hide_endpoint

logger = EPMPYLogger(__name__)
router = APIRouter()


@router.post(
    TENANT_NAME_URL + AOR_URL,
    status_code=HTTPStatus.CREATED,
    description="Создать объект в AOR",
    response_description="Модель",
    response_model=dict,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.MODEL_CREATE]))],
)
@hide_endpoint("HIDE_CREATE_IN_AOR")
async def create_in_aor(
    request: Request,
    service: Annotated[AorService, Depends(get_aor_service)],
    aor_request: CreateAorRequest = Body(alias="aorRequest"),
    tenant_id: str = Path(alias="tenantName"),
) -> dict:
    """Создать объект в в АОРе"""
    audit_kwargs = {
        "audit_type": audit_types.C4,
        "status": StatusType.FAIL,
        "object_id": f"{request.url.path}/{aor_request.type}/{aor_request.name}",
        "object_properties": aor_request.model_fields_set,
        "message": aor_request.model_dump(by_alias=True, mode="json", exclude_unset=True),
        "object_name": aor_request.type,
        "extra": {"include_fields": ["object_id", "object_name"]},
    }
    try:
        await service.send_to_aor(tenant_id=tenant_id, aor_request=aor_request)
        audit_kwargs["audit_type"] = audit_types.C3
        audit_kwargs["status"] = StatusType.SUCCESS
        logger.audit(**audit_kwargs)
    except Exception as ex:  # noqa: PIE786
        reason = "UNKNOWN ERROR: " + str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs, status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=reason
        )
    return {"details": {"msg": "OK"}}


@router.post(
    AOR_URL + "/modelling_tool/proxy",
    status_code=HTTPStatus.OK,
    description="Создать объект в AOR",
    response_description="Модель",
    response_model=dict,
    dependencies=[
        Depends(api_key_auth),
    ],
)
@hide_endpoint("HIDE_DEPLOY_BY_AOR")
async def deploy_by_aor(
    request: Request,
    service: Annotated[AorService, Depends(get_aor_service)],
    aor_command: PushAorCommand = Body(alias="aorCommand"),
) -> dict:
    """Создать объект в в АОРе"""
    audit_kwargs = {
        "audit_type": audit_types.C4,
        "status": StatusType.FAIL,
        "object_id": f"{request.url.path}/{aor_command.type}/{aor_command.name}",
        "object_properties": aor_command.model_fields_set,
        "message": aor_command.model_dump(by_alias=True, mode="json", exclude_unset=True),
        "object_name": aor_command.type,
        "extra": {"include_fields": ["object_id", "object_name"]},
    }
    try:
        await service.deploy_by_aor(aor_command)
        audit_kwargs["audit_type"] = audit_types.C3
        audit_kwargs["status"] = StatusType.SUCCESS
        logger.audit(**audit_kwargs)
    except Exception as ex:  # noqa: PIE786
        reason = "UNKNOWN ERROR: " + str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs, status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=reason
        )
    return {"details": {"msg": "OK"}}


@router.post(
    TENANT_NAME_URL + MODEL_NAME_URL + AOR_URL,
    status_code=HTTPStatus.CREATED,
    description="Создать модель в AOR",
    response_description="Модель",
    response_model=dict,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.MODEL_CREATE]))],
)
@hide_endpoint("HIDE_CREATE_MODEL_IN_AOR")
async def create_model_in_aor(
    request: Request,
    service: Annotated[AorService, Depends(get_aor_service)],
    aor_model_request: CreateModelAorRequest = Body(alias="aorModelRequest"),
    model_name: str = Path(alias="modelName"),
    tenant_id: str = Path(alias="tenantName"),
) -> dict:
    """Создать объект в в АОРе"""
    audit_kwargs = {
        "audit_type": audit_types.C4,
        "status": StatusType.FAIL,
        "object_id": f"{request.url.path}/{aor_model_request.type}/{model_name}",
        "object_properties": aor_model_request.model_fields_set,
        "message": aor_model_request.model_dump(by_alias=True, mode="json", exclude_unset=True),
        "object_name": aor_model_request.type,
        "extra": {"include_fields": ["object_id", "object_name"]},
    }
    try:
        await service.send_to_aor_by_model_and_type(
            tenant_id=tenant_id, model_name=model_name, aor_model_request=aor_model_request
        )
        audit_kwargs["audit_type"] = audit_types.C3
        audit_kwargs["status"] = StatusType.SUCCESS
        logger.audit(**audit_kwargs)
    except Exception as ex:  # noqa: PIE786
        reason = "UNKNOWN ERROR: " + str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs, status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=reason
        )
    return {"details": {"msg": "OK"}}
