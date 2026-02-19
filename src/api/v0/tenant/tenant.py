"""
API для моделей.
"""

from http import HTTPStatus
from typing import Annotated

from fastapi import APIRouter, Body, Depends, Header, Path, Request
from py_common_lib.logger import EPMPYLogger, audit_types
from py_common_lib.logger.enums import StatusType
from py_common_lib.permissions import PermissionChecker
from sqlalchemy.exc import IntegrityError, NoResultFound, SQLAlchemyError

from src.api.dependencies import get_pagination_params, get_relations_service
from src.api.v0.enums import CacheNamespaceEnum
from src.api.v0.tenant.dependencies import get_tenant_service
from src.cache.decorator import cache
from src.cache.types import CacheHeaderEnum
from src.models.permissions import PermissionEnum
from src.models.request_params import Pagination
from src.models.tenant import (
    FindWhereUsedRequest,
    Tenant,
    TenantCreateRequest,
    TenantEditRequest,
    TenantObjectSearchRequest,
    TenantSearchObjectResponse,
)
from src.service.relations import RelationsService
from src.service.tenant import TenantService
from src.utils.exceptions import HTTPExceptionWithAuditLogging
from src.utils.hide_endpoint_decorator import hide_endpoint

logger = EPMPYLogger(__name__)
router = APIRouter()


@router.get(
    "/",
    status_code=HTTPStatus.OK,
    description="Получить список всех тенантов.",
    response_description="Список тенантов.",
    response_model=list[Tenant],
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.TENANT_VIEW]))],
)
@hide_endpoint("HIDE_GET_TENANT_LIST")
@cache(namespace=CacheNamespaceEnum.TENANT)
async def get_tenant_list(
    request: Request,
    service: Annotated[TenantService, Depends(get_tenant_service)],
    pagination: Annotated[Pagination, Depends(get_pagination_params)],
    cache_header: CacheHeaderEnum | str = Header(alias="Cache-Control", default=CacheHeaderEnum.EMPTY),
) -> list[Tenant]:
    """Получить список всех тенантов."""
    audit_kwargs = {
        "audit_type": audit_types.C2,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": Tenant.__qualname__,
    }
    try:
        result = await service.get_tenant_list(pagination=pagination)
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
    return result


@router.get(
    "/{tenantName}",
    status_code=HTTPStatus.OK,
    description="Получить тенант по имени.",
    response_description="Тенант",
    response_model=Tenant,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.TENANT_VIEW]))],
)
@hide_endpoint("HIDE_GET_TENANT_BY_NAME")
@cache(namespace=CacheNamespaceEnum.TENANT)
async def get_tenant_by_name(
    request: Request,
    service: Annotated[TenantService, Depends(get_tenant_service)],
    name: str = Path(alias="tenantName"),
    cache_header: CacheHeaderEnum | str = Header(alias="Cache-Control", default=CacheHeaderEnum.EMPTY),
) -> Tenant:
    """Получить тенант по имени."""
    audit_kwargs = {
        "audit_type": audit_types.C2,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": Tenant.__qualname__,
    }
    try:
        result = await service.get_tenant_by_name(name=name)
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
    return result


@router.delete(
    "/{tenantName}",
    status_code=HTTPStatus.NO_CONTENT,
    description="Удалить объект Tenant из базы данных.",
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.TENANT_DELETE]))],
)
@hide_endpoint("HIDE_DELETE_TENANT_BY_NAME")
async def delete_tenant_by_name(
    request: Request,
    service: Annotated[TenantService, Depends(get_tenant_service)],
    name: str = Path(alias="tenantName"),
) -> None:
    """Удалить объект Tenant из базы данных."""
    audit_kwargs = {
        "audit_type": audit_types.C6,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": Tenant.__qualname__,
    }
    try:
        await service.delete_tenant_by_name(name=name)
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
    except Exception as ex:  # noqa: PIE786
        reason = "UNKNOWN ERROR: " + str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs, status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=reason
        )


@router.post(
    "/",
    status_code=HTTPStatus.CREATED,
    description="Создать объект tenant",
    response_description="База данных",
    response_model=Tenant,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.TENANT_CREATE]))],
)
@hide_endpoint("HIDE_CREATE_TENANT")
async def create_tenant(
    request: Request,
    service: Annotated[TenantService, Depends(get_tenant_service)],
    tenant: TenantCreateRequest = Body(alias="tenant"),
) -> Tenant:
    """Создать объект tenant в базе данных"""
    audit_kwargs = {
        "audit_type": audit_types.C4,
        "status": StatusType.FAIL,
        "object_id": request.url.path + tenant.name,
        "object_properties": tenant.model_fields_set,
        "message": tenant.model_dump(by_alias=True, mode="json", exclude_unset=True),
        "object_name": Tenant.__qualname__,
        "extra": {"include_fields": ["object_id", "object_name"]},
    }
    try:
        result = await service.create_tenant_by_schema(tenant=tenant)
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
    return result


@router.patch(
    "/{tenantName}",
    status_code=HTTPStatus.OK,
    description="Обновить объект Tenant из базы данных.",
    response_description="Tenant",
    response_model=Tenant,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.TENANT_EDIT]))],
)
@hide_endpoint("HIDE_UPDATE_TENANT_BY_NAME")
async def update_tenant_by_name(
    request: Request,
    service: Annotated[TenantService, Depends(get_tenant_service)],
    name: str = Path(alias="tenantName"),
    tenant: TenantEditRequest = Body(alias="tenant"),
) -> Tenant:
    """Обновить объект Tenant из базы данных."""
    audit_kwargs = {
        "audit_type": audit_types.C4,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": Tenant.__qualname__,
        "message": tenant.model_dump(by_alias=True, mode="json", exclude_unset=True),
        "object_properties": tenant.model_fields_set,
        "extra": {"include_fields": ["object_id", "object_name"]},
    }
    try:
        message = await service.get_updated_fields(
            name=name,
            tenant=tenant,
        )
        result = await service.update_tenant_by_name_and_schema(name=name, tenant=tenant)
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
    return result


@router.get(
    "/{tenantName}/search/objects",
    status_code=HTTPStatus.OK,
    description="Поиск по объектам в тенанте",
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.TENANT_VIEW]))],
    response_model=TenantSearchObjectResponse,
)
async def search_objects(
    request: Request,
    service: Annotated[TenantService, Depends(get_tenant_service)],
    params: TenantObjectSearchRequest = Depends(),
    tenant_name: str = Path(alias="tenantName"),
) -> TenantSearchObjectResponse:
    """
    Поиск по объектам в тенанте
    Args:
        request (Request): Объект, хранящий информации по HTTP запросу
        service (TenantService): Сервис, предоставляющий интерфейс взаимодействия с сущностью Тенанта
        params (TenantObjectSearchRequest): Параметры запроса
        tenant_name (str): Имя тенанта. Берется из URI параметров
    Returns:
        TenantSearchObjectResponse: Возвращает список моделей и объектов семантики, относящихся к ним
    """
    audit_kwargs = {
        "audit_type": audit_types.C1,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
    }

    try:
        db_result = await service.search_elements(
            tenant_name=tenant_name,
            search=params.query,
            element_type=params.object_type,
            model_name=params.model,
        )
        result = {}
        for model_name, semantic_objects in db_result.items():
            if not semantic_objects.is_empty():
                result[model_name] = {
                    "dimensions": semantic_objects.dimensions,
                    "data_storages": semantic_objects.data_storages,
                    "measures": semantic_objects.measures,
                    "composites": semantic_objects.composites,
                }
        if not db_result:
            raise HTTPExceptionWithAuditLogging(
                audit_kwargs=audit_kwargs,
                status_code=HTTPStatus.NOT_FOUND,
                detail="No objects found",
            )
    except Exception as e:
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs,
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail=str(e),
        )
    return TenantSearchObjectResponse(results=result)


@router.get(
    "/{tenantName}/models/{modelName}/objectLinks/{objectName}",
    status_code=HTTPStatus.OK,
    description="Поиск по объектам в тенанте",
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.TENANT_VIEW]))],
    response_model=TenantSearchObjectResponse,
)
async def find_where_used(
    request: Request,
    service: Annotated[RelationsService, Depends(get_relations_service)],
    params: FindWhereUsedRequest = Depends(),
    tenant_name: str = Path(alias="tenantName"),
    model_name: str = Path(alias="modelName"),
    object_name: str = Path(alias="objectName"),
) -> TenantSearchObjectResponse:
    """
    Поиск связанных объектов семантики
    Args:
        request (Request): Объект, хранящий информации по HTTP запросу
        service (TenantService): Сервис, предоставляющий интерфейс взаимодействия с сущностью Тенанта
        params (FindWhereUsedRequest): Параметры запроса
        tenant_name (str): Имя тенанта. Берется из URI параметров
        model_name (str): Имя модели. Берется из URI параметров
        object_name (str): Имя объекта. Берется из URI параметров
    Returns:
        TenantSearchObjectResponse: Возвращает список моделей и объектов семантики, относящихся к ним
    """
    audit_kwargs = {
        "audit_type": audit_types.C1,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
    }
    if not params.object_type:
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs,
            status_code=HTTPStatus.BAD_REQUEST,
            detail="Object type is required",
        )
    try:
        result = await service.find_where_used(
            tenant_name=tenant_name,
            model_name=model_name,
            object_name=object_name,
            object_type=params.object_type,
        )
    except Exception as e:
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs,
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail=str(e),
        )

    return TenantSearchObjectResponse(
        results={
            model_name: {
                "dimensions": result.dimensions,
                "data_storages": result.data_storages,
                "measures": result.measures,
                "composites": result.composites,
            }
        }
    )
