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

from src.api.dependencies import get_pagination_params
from src.api.v1.database.dependencies import get_database_service
from src.api.v1.enums import CacheNamespaceEnum
from src.cache.decorator import cache
from src.cache.types import CacheHeaderEnum
from src.models.database import Database, DatabaseCreateRequest, DatabaseEditRequest
from src.models.permissions import PermissionEnum
from src.models.request_params import Pagination
from src.service.database import DatabaseService
from src.utils.exceptions import HTTPExceptionWithAuditLogging
from src.utils.hide_endpoint_decorator import hide_endpoint

logger = EPMPYLogger(__name__)
router = APIRouter()


@router.get(
    "/",
    status_code=HTTPStatus.OK,
    description="Получить список всех баз данных.",
    response_description="Список баз данных.",
    response_model=list[Database],
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.DATABASE_VIEW]))],
)
@hide_endpoint("HIDE_GET_DATABASE_LIST")
@cache(namespace=CacheNamespaceEnum.DATABASE)
async def get_database_list(
    request: Request,
    service: Annotated[DatabaseService, Depends(get_database_service)],
    pagination: Annotated[Pagination, Depends(get_pagination_params)],
    tenant_id: str = Path(alias="tenantName"),
    cache_header: CacheHeaderEnum | str = Header(alias="Cache-Control", default=CacheHeaderEnum.EMPTY),
) -> list[Database]:
    """Получить список всех баз данных."""
    audit_kwargs = {
        "audit_type": audit_types.C2,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": Database.__qualname__,
    }
    try:
        result = await service.get_database_list(tenant_id=tenant_id, pagination=pagination)
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
    "/{databaseName}",
    status_code=HTTPStatus.OK,
    description="Получить базу данных по имени.",
    response_description="База данных",
    response_model=Database,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.DATABASE_VIEW]))],
)
@hide_endpoint("HIDE_GET_DATABASE_BY_NAME")
@cache(namespace=CacheNamespaceEnum.DATABASE)
async def get_database_by_name(
    request: Request,
    service: Annotated[DatabaseService, Depends(get_database_service)],
    name: str = Path(alias="databaseName"),
    tenant_id: str = Path(alias="tenantName"),
    cache_header: CacheHeaderEnum | str = Header(alias="Cache-Control", default=CacheHeaderEnum.EMPTY),
) -> Database:
    """Получить базу данных по имени."""
    audit_kwargs = {
        "audit_type": audit_types.C2,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": Database.__qualname__,
    }
    try:
        result = await service.get_database_by_name(tenant_id=tenant_id, name=name)
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
    "/{databaseName}",
    status_code=HTTPStatus.NO_CONTENT,
    description="Удалить объект DataBase из базы данных.",
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.DATABASE_DELETE]))],
)
@hide_endpoint("HIDE_DELETE_DATABASE_BY_NAME")
async def delete_database_by_name(
    request: Request,
    service: Annotated[DatabaseService, Depends(get_database_service)],
    tenant_id: str = Path(alias="tenantName"),
    name: str = Path(alias="databaseName"),
) -> None:
    """Удалить объект DataBase из базы данных."""
    audit_kwargs = {
        "audit_type": audit_types.C6,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": Database.__qualname__,
    }
    try:
        await service.delete_database_by_name(tenant_id=tenant_id, database_name=name)
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
    description="Создать объект DataBase",
    response_description="База данных",
    response_model=Database,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.DATABASE_CREATE]))],
)
@hide_endpoint("HIDE_CREATE_DATABASE")
async def create_database(
    request: Request,
    service: Annotated[DatabaseService, Depends(get_database_service)],
    database: DatabaseCreateRequest = Body(alias="dataBase"),
    tenant_id: str = Path(alias="tenantName"),
) -> Database:
    """Создать объект DataBase в базе данных"""
    audit_kwargs = {
        "audit_type": audit_types.C4,
        "status": StatusType.FAIL,
        "object_id": request.url.path + database.name,
        "object_properties": database.model_fields_set,
        "message": database.model_dump(by_alias=True, mode="json", exclude_unset=True),
        "object_name": Database.__qualname__,
        "extra": {"include_fields": ["object_id", "object_name"]},
    }
    try:
        result = await service.create_database_by_schema(tenant_id=tenant_id, database=database)
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
    "/{databaseName}",
    status_code=HTTPStatus.OK,
    description="Обновить объект DataBase из базы данных.",
    response_description="База данных",
    response_model=Database,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.DATABASE_EDIT]))],
)
@hide_endpoint("HIDE_UPDATE_DATABASE_BY_NAME")
async def update_database_by_name(
    request: Request,
    service: Annotated[DatabaseService, Depends(get_database_service)],
    name: str = Path(alias="databaseName"),
    database: DatabaseEditRequest = Body(alias="dataBase"),
    tenant_id: str = Path(alias="tenantName"),
) -> Database:
    """Обновить объект DataBase из базы данных."""
    audit_kwargs = {
        "audit_type": audit_types.C4,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": Database.__qualname__,
        "message": database.model_dump(by_alias=True, mode="json", exclude_unset=True),
        "object_properties": database.model_fields_set,
        "extra": {"include_fields": ["object_id", "object_name"]},
    }
    try:
        message = await service.get_updated_fields(
            tenant_id=tenant_id,
            name=name,
            database=database,
        )
        result = await service.update_database_by_name_and_schema(
            tenant_id=tenant_id, database_name=name, database=database
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
    return result
