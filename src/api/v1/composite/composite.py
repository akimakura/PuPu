"""
API для признаков.
"""

from http import HTTPStatus
from typing import Annotated

from fastapi import APIRouter, Body, Depends, Header, Path, Query, Request
from fastapi.responses import JSONResponse
from py_common_lib.logger import EPMPYLogger, audit_types
from py_common_lib.logger.enums import StatusType
from py_common_lib.permissions import PermissionChecker
from sqlalchemy.exc import IntegrityError, NoResultFound, SQLAlchemyError

from src.api.dependencies import get_pagination_params
from src.api.v1.composite.dependencies import get_composite_service
from src.api.v1.const import COMPOSITE_URL, COMPOSITE_URL_WITHOUT_MODEL
from src.api.v1.enums import CacheNamespaceEnum
from src.cache.decorator import cache
from src.cache.types import CacheHeaderEnum
from src.models.composite import Composite, CompositeCreateRequest, CompositeEditRequest, CompositeV1
from src.models.copy_model import CopyModelRequest, DetailsObjectCopyReponse
from src.models.exceptions import SemanticObjectRelationException
from src.models.permissions import PermissionEnum
from src.models.request_params import Pagination
from src.service.composite import CompositeService
from src.utils.exceptions import HTTPExceptionWithAuditLogging
from src.utils.hide_endpoint_decorator import hide_endpoint

logger = EPMPYLogger(__name__)
router = APIRouter()


@router.get(
    COMPOSITE_URL + "/",
    status_code=HTTPStatus.OK,
    description="Получить список композитов",
    response_description="Список композитов",
    response_model=list[CompositeV1],
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.COMPOSITE_VIEW]))],
)
@hide_endpoint("HIDE_GET_COMPOSITE_LIST_BY_MODEL_NAME")
@cache(namespace=CacheNamespaceEnum.COMPOSITE)
async def get_composite_list_by_model_name(
    request: Request,
    service: Annotated[CompositeService, Depends(get_composite_service)],
    pagination: Annotated[Pagination, Depends(get_pagination_params)],
    model_name: str = Path(alias="modelName"),
    tenant_id: str = Path(alias="tenantName"),
    cache_header: CacheHeaderEnum | str = Header(alias="Cache-Control", default=CacheHeaderEnum.EMPTY),
) -> list[CompositeV1]:
    """Получить список всех Composite."""
    audit_kwargs = {
        "audit_type": audit_types.C2,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": Composite.__qualname__,
    }
    try:
        result = await service.get_composite_list_by_model_name(
            tenant_id=tenant_id,
            model_name=model_name,
            pagination=pagination,
        )
        audit_kwargs["audit_type"] = audit_types.C1
        audit_kwargs["status"] = StatusType.SUCCESS
        logger.audit(**audit_kwargs)
    except IntegrityError as ex:
        reason = "BAD_REQUEST: " + str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs, status_code=HTTPStatus.BAD_REQUEST, detail=reason
        )
    except SQLAlchemyError as ex:
        reason = "INTERNAL_SERVER_ERROR: " + str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs, status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=reason
        )
    except Exception as ex:  # noqa: PIE786
        reason = "UNKNOWN ERROR: " + str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs, status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=reason
        )
    return [CompositeV1.model_validate(composite) for composite in result]


@router.get(
    COMPOSITE_URL + "/{compositeName}",
    status_code=HTTPStatus.OK,
    description="Получить композит по имени",
    response_description="Композит",
    response_model=CompositeV1,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.COMPOSITE_VIEW]))],
)
@hide_endpoint("HIDE_GET_COMPOSITE_BY_COMPOSITE_NAME")
@cache(namespace=CacheNamespaceEnum.COMPOSITE)
async def get_composite_by_composite_name(
    request: Request,
    service: Annotated[CompositeService, Depends(get_composite_service)],
    model_name: str = Path(alias="modelName"),
    composite_name: str = Path(alias="compositeName"),
    tenant_id: str = Path(alias="tenantName"),
    cache_header: CacheHeaderEnum | str = Header(alias="Cache-Control", default=CacheHeaderEnum.EMPTY),
) -> CompositeV1:
    """Получить Composite по имени."""
    audit_kwargs = {
        "audit_type": audit_types.C2,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": Composite.__qualname__,
    }
    try:
        result = await service.get_composite_by_name(name=composite_name, model_name=model_name, tenant_id=tenant_id)
        audit_kwargs["audit_type"] = audit_types.C1
        audit_kwargs["status"] = StatusType.SUCCESS
        logger.audit(**audit_kwargs)
    except IntegrityError as ex:
        reason = "BAD_REQUEST: " + str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs, status_code=HTTPStatus.BAD_REQUEST, detail=reason
        )
    except NoResultFound as ex:
        reason = str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(audit_kwargs=audit_kwargs, status_code=HTTPStatus.NOT_FOUND, detail=reason)
    except SQLAlchemyError as ex:
        reason = "INTERNAL_SERVER_ERROR: " + str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs, status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=reason
        )
    except Exception as ex:  # noqa: PIE786
        reason = "UNKNOWN ERROR: " + str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs, status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=reason
        )
    return CompositeV1.model_validate(result)


@router.post(
    COMPOSITE_URL + "/create",
    status_code=HTTPStatus.CREATED,
    description="Создать объект Composite",
    response_description="Композит",
    response_model=CompositeV1,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.COMPOSITE_CREATE]))],
)
@router.post(
    COMPOSITE_URL + "/",
    status_code=HTTPStatus.CREATED,
    description="Создать объект Composite",
    response_description="Композит",
    response_model=CompositeV1,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.COMPOSITE_CREATE]))],
)
@hide_endpoint("HIDE_CREATE_COMPOSITE")
async def create_composite(
    request: Request,
    service: Annotated[CompositeService, Depends(get_composite_service)],
    model_name: str = Path(alias="modelName"),
    composite: CompositeCreateRequest = Body(alias="composite"),
    tenant_id: str = Path(alias="tenantName"),
    generate_on_db: bool = Query(default=False, alias="generateOnDB"),
    replace: bool = Query(default=False, alias="replace"),
) -> CompositeV1:
    """Создать объект Composite в базе данных"""
    audit_kwargs = {
        "audit_type": audit_types.C4,
        "status": StatusType.FAIL,
        "object_id": request.url.path + composite.name,
        "object_properties": composite.model_fields_set,
        "message": composite.model_dump(by_alias=True, mode="json", exclude_unset=True),
        "object_name": Composite.__qualname__,
        "extra": {"include_fields": ["object_id", "object_name"]},
    }
    try:
        result = await service.create_composite_by_schema(
            tenant_id=tenant_id,
            model_name=model_name,
            composite=composite,
            generate_on_db=generate_on_db,
            replace=replace,
        )
        audit_kwargs["audit_type"] = audit_types.C3
        audit_kwargs["status"] = StatusType.SUCCESS
        logger.audit(**audit_kwargs)
    except IntegrityError as ex:
        reason = "BAD_REQUEST: " + str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs, status_code=HTTPStatus.BAD_REQUEST, detail=reason
        )
    except SQLAlchemyError as ex:
        reason = "INTERNAL_SERVER_ERROR: " + str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs, status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=reason
        )
    except Exception as ex:  # noqa: PIE786
        reason = "UNKNOWN ERROR: " + str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs, status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=reason
        )
    return CompositeV1.model_validate(result)


@router.delete(
    COMPOSITE_URL + "/{compositeName}",
    status_code=HTTPStatus.NO_CONTENT,
    description="Удалить объект Composite из базы данных.",
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.COMPOSITE_DELETE]))],
)
@hide_endpoint("HIDE_DELETE_COMPOSITE_BY_NAME")
async def delete_composite_by_name(
    request: Request,
    service: Annotated[CompositeService, Depends(get_composite_service)],
    model_name: str = Path(alias="modelName"),
    composite_name: str = Path(alias="compositeName"),
    tenant_id: str = Path(alias="tenantName"),
) -> None:
    """Удалить объект Composite из базы данных."""
    audit_kwargs = {
        "audit_type": audit_types.C6,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": Composite.__qualname__,
    }
    try:
        await service.delete_composite_by_name(tenant_id=tenant_id, model_name=model_name, name=composite_name)
        audit_kwargs["audit_type"] = audit_types.C5
        audit_kwargs["status"] = StatusType.SUCCESS
        logger.audit(**audit_kwargs)
    except IntegrityError as ex:
        reason = "BAD_REQUEST: " + str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs, status_code=HTTPStatus.BAD_REQUEST, detail=reason
        )
    except NoResultFound as ex:
        reason = str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(audit_kwargs=audit_kwargs, status_code=HTTPStatus.NOT_FOUND, detail=reason)
    except SQLAlchemyError as ex:
        reason = "INTERNAL_SERVER_ERROR: " + str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs, status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=reason
        )
    except SemanticObjectRelationException as ex:
        reason = "There are semantic objects related to this composite" + str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs, status_code=HTTPStatus.BAD_REQUEST, detail=reason
        )
    except Exception as ex:  # noqa: PIE786
        reason = "UNKNOWN ERROR: " + str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs, status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=reason
        )


@router.patch(
    COMPOSITE_URL + "/{compositeName}",
    status_code=HTTPStatus.OK,
    description="Обновить объект Composite из базы данных.",
    response_description="Композит",
    response_model=CompositeV1,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.COMPOSITE_EDIT]))],
)
@hide_endpoint("HIDE_UPDATE_COMPOSITE_BY_NAME")
async def update_composite_by_name(
    request: Request,
    service: Annotated[CompositeService, Depends(get_composite_service)],
    composite_name: str = Path(alias="compositeName"),
    model_name: str = Path(alias="modelName"),
    composite: CompositeEditRequest = Body(alias="composite"),
    tenant_id: str = Path(alias="tenantName"),
    generate_on_db: bool = Query(default=False, alias="generateOnDB"),
) -> CompositeV1:
    """Обновить объект Composite из базы данных."""
    audit_kwargs = {
        "audit_type": audit_types.C4,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": Composite.__qualname__,
        "message": composite.model_dump(by_alias=True, mode="json", exclude_unset=True),
        "object_properties": composite.model_fields_set,
        "extra": {"include_fields": ["object_id", "object_name"]},
    }
    try:
        message = await service.get_updated_fields(
            tenant_id=tenant_id,
            model_name=None,
            name=composite_name,
            composite=composite,
        )
        result = await service.update_composite_by_name_and_schema(
            tenant_id=tenant_id,
            model_name=model_name,
            name=composite_name,
            composite=composite,
            generate_on_db=generate_on_db,
        )
        audit_kwargs["audit_type"] = audit_types.C3
        audit_kwargs["message"] = message
        audit_kwargs["status"] = StatusType.SUCCESS
        logger.audit(**audit_kwargs)
    except IntegrityError as ex:
        reason = "BAD_REQUEST: " + str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs, status_code=HTTPStatus.BAD_REQUEST, detail=reason
        )
    except NoResultFound as ex:
        reason = str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(audit_kwargs=audit_kwargs, status_code=HTTPStatus.NOT_FOUND, detail=reason)
    except SQLAlchemyError as ex:
        reason = "INTERNAL_SERVER_ERROR: " + str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs, status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=reason
        )
    except Exception as ex:  # noqa: PIE786
        reason = "UNKNOWN ERROR: " + str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs, status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=reason
        )
    return CompositeV1.model_validate(result)


@router.post(
    COMPOSITE_URL_WITHOUT_MODEL + "/copyToAnotherModel",
    status_code=HTTPStatus.CREATED,
    description="Скопировать Composite в другие модели.",
    response_description="DetailsObjectCopyReponse",
    response_model=DetailsObjectCopyReponse,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.COMPOSITE_EDIT]))],
)
@hide_endpoint("HIDE_COPY_MODEL_COMPOSITE")
async def copy_model_composite(
    request: Request,
    service: Annotated[CompositeService, Depends(get_composite_service)],
    copy_model: CopyModelRequest = Body(alias="copy_model"),
    tenant_id: str = Path(alias="tenantName"),
    generate_on_db: bool = Query(default=False, alias="generateOnDB"),
    replace: bool = Query(default=True, alias="replace"),
) -> DetailsObjectCopyReponse | JSONResponse:
    """Копировать объект Composite в другие модели."""
    audit_kwargs = {
        "audit_type": audit_types.C4,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": Composite.__qualname__,
        "message": {"models": copy_model.models, "composites": copy_model.objects},
        "object_properties": ["models"],
        "extra": {"include_fields": ["object_id", "object_name"]},
    }
    try:
        composite, has_error = await service.copy_model_composites(
            tenant_id,
            copy_model.models,
            copy_model.objects,
            generate_on_db=generate_on_db,
            replace=replace,
        )
        audit_kwargs["audit_type"] = audit_types.C3
        if has_error:
            return JSONResponse(content=composite.model_dump(), status_code=HTTPStatus.INTERNAL_SERVER_ERROR)
        audit_kwargs["status"] = StatusType.SUCCESS
        logger.audit(**audit_kwargs)
    except IntegrityError as ex:
        reason = "BAD_REQUEST: " + str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs, status_code=HTTPStatus.BAD_REQUEST, detail=reason
        )
    except NoResultFound as ex:
        reason = str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(audit_kwargs=audit_kwargs, status_code=HTTPStatus.NOT_FOUND, detail=reason)
    except SQLAlchemyError as ex:
        reason = "INTERNAL_SERVER_ERROR: " + str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs, status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=reason
        )
    except Exception as ex:  # noqa: PIE786
        reason = "UNKNOWN ERROR: " + str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs, status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=reason
        )
    return composite
