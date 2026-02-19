"""
API для показателей.
"""

from http import HTTPStatus
from typing import Annotated

from fastapi import APIRouter, Body, Depends, Header, Path, Request
from fastapi.responses import JSONResponse
from py_common_lib.logger import EPMPYLogger, audit_types
from py_common_lib.logger.enums import StatusType
from py_common_lib.permissions import PermissionChecker
from sqlalchemy.exc import IntegrityError, NoResultFound, SQLAlchemyError

from src.api.dependencies import get_pagination_params
from src.api.v0.const import MEASURE_URL, MEASURE_WITHOUT_MODEL_URL
from src.api.v0.enums import CacheNamespaceEnum
from src.api.v0.measure.dependencies import get_measure_service
from src.cache.decorator import cache
from src.cache.types import CacheHeaderEnum
from src.models.copy_model import CopyModelRequest, DetailsObjectCopyReponse
from src.models.exceptions import SemanticObjectRelationException
from src.models.measure import Measure, MeasureCreateRequest, MeasureEditRequest, MeasureV0
from src.models.permissions import PermissionEnum
from src.models.request_params import Pagination
from src.service.measure import MeasureService
from src.utils.exceptions import HTTPExceptionWithAuditLogging
from src.utils.hide_endpoint_decorator import hide_endpoint

logger = EPMPYLogger(__name__)
router = APIRouter()


@router.get(
    MEASURE_URL + "/",
    status_code=HTTPStatus.OK,
    description="Получить список всех показателей.",
    response_description="Список показателей.",
    response_model=list[MeasureV0],
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.MEASURE_VIEW]))],
)
@hide_endpoint("HIDE_GET_MEASURE_LIST_BY_MODEL_NAME")
@cache(namespace=CacheNamespaceEnum.MEASURE)
async def get_measure_list_by_model_name(
    request: Request,
    service: Annotated[MeasureService, Depends(get_measure_service)],
    pagination: Annotated[Pagination, Depends(get_pagination_params)],
    model_name: str = Path(alias="modelName"),
    tenant_id: str = Path(alias="tenantName"),
    cache_header: CacheHeaderEnum | str = Header(alias="Cache-Control", default=CacheHeaderEnum.EMPTY),
) -> list[MeasureV0]:
    """Получить список всех Measure."""
    audit_kwargs = {
        "audit_type": audit_types.C2,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": Measure.__qualname__,
    }
    try:
        result = await service.get_measure_list_by_model_name(
            tenant_id=tenant_id, model_name=model_name, pagination=pagination
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
    return [MeasureV0.model_validate(measure) for measure in result]


@router.post(
    MEASURE_URL + "/",
    status_code=HTTPStatus.OK,
    description="Получить список показателей с именами names",
    response_description="Список показателей.",
    response_model=list[MeasureV0],
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.MEASURE_VIEW]))],
)
@hide_endpoint("HIDE_GET_MEASURE_LIST_BY_NAMES")
@cache(namespace=CacheNamespaceEnum.MEASURE)
async def get_measure_list_by_names(
    request: Request,
    service: Annotated[MeasureService, Depends(get_measure_service)],
    pagination: Annotated[Pagination, Depends(get_pagination_params)],
    names: list[str] = Body(alias="measureNames"),
    model_name: str = Path(alias="modelName"),
    tenant_id: str = Path(alias="tenantName"),
    cache_header: CacheHeaderEnum | str = Header(alias="Cache-Control", default=CacheHeaderEnum.EMPTY),
) -> list[MeasureV0]:
    """Получить список показателей с именами names"""
    audit_kwargs = {
        "audit_type": audit_types.C2,
        "status": StatusType.FAIL,
        "object_id": request.url.path + f"?names={names}",
        "object_name": Measure.__qualname__,
    }
    try:
        result = await service.get_measure_list_by_names(
            tenant_id=tenant_id, model_name=model_name, names=names, pagination=pagination
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
    return [MeasureV0.model_validate(measure) for measure in result]


@router.get(
    MEASURE_URL + "/{measureName}",
    status_code=HTTPStatus.OK,
    description="Получить показатель по имени.",
    response_description="Показатель.",
    response_model=MeasureV0,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.MEASURE_VIEW]))],
)
@hide_endpoint("HIDE_GET_MEASURE_BY_MEASURE_NAME")
@cache(namespace=CacheNamespaceEnum.MEASURE)
async def get_measure_by_measure_name(
    request: Request,
    service: Annotated[MeasureService, Depends(get_measure_service)],
    model_name: str = Path(alias="modelName"),
    measure_name: str = Path(alias="measureName"),
    tenant_id: str = Path(alias="tenantName"),
    cache_header: CacheHeaderEnum | str = Header(alias="Cache-Control", default=CacheHeaderEnum.EMPTY),
) -> MeasureV0:
    """Получить Measure по имени и модели."""
    audit_kwargs = {
        "audit_type": audit_types.C2,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": Measure.__qualname__,
    }
    try:
        result = await service.get_measure_by_measure_name(
            name=measure_name, tenant_id=tenant_id, model_name=model_name
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
    return MeasureV0.model_validate(result)


@router.post(
    MEASURE_URL + "/create",
    status_code=HTTPStatus.CREATED,
    description="Создать объект Measure",
    response_description="Показатель",
    response_model=MeasureV0,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.MEASURE_CREATE]))],
)
@hide_endpoint("HIDE_CREATE_MEASURE")
async def create_measure(
    request: Request,
    service: Annotated[MeasureService, Depends(get_measure_service)],
    model_name: str = Path(alias="modelName"),
    measure: MeasureCreateRequest = Body(alias="measure"),
    tenant_id: str = Path(alias="tenantName"),
) -> MeasureV0:
    """Создать объект Measure в базе данных"""
    audit_kwargs = {
        "audit_type": audit_types.C4,
        "status": StatusType.FAIL,
        "object_id": request.url.path.replace("create", measure.name),
        "object_properties": measure.model_fields_set,
        "message": measure.model_dump(by_alias=True, mode="json", exclude_unset=True),
        "object_name": Measure.__qualname__,
        "extra": {"include_fields": ["object_id", "object_name"]},
    }
    try:
        result = await service.create_measure_by_schema(tenant_id=tenant_id, model_name=model_name, measure=measure)
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
    return MeasureV0.model_validate(result)


@router.delete(
    MEASURE_URL + "/{measureName}",
    status_code=HTTPStatus.NO_CONTENT,
    description="Удалить объект Measure из базы данных.",
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.MEASURE_DELETE]))],
)
@hide_endpoint("HIDE_DELETE_MEASURE_BY_NAME")
async def delete_measure_by_name(
    request: Request,
    service: Annotated[MeasureService, Depends(get_measure_service)],
    model_name: str = Path(alias="modelName"),
    measure_name: str = Path(alias="measureName"),
    tenant_id: str = Path(alias="tenantName"),
) -> None:
    """Удалить объект Measure из базы данных."""
    audit_kwargs = {
        "audit_type": audit_types.C6,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": Measure.__qualname__,
    }
    try:
        await service.delete_measure_by_name(tenant_id=tenant_id, model_name=model_name, name=measure_name)
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
        reason = "There are semantic objects related to this measure. " + str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs,
            status_code=HTTPStatus.BAD_REQUEST,
        )
    except Exception as ex:  # noqa: PIE786
        reason = "UNKNOWN ERROR: " + str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs, status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=reason
        )


@router.patch(
    MEASURE_URL + "/{measureName}",
    status_code=HTTPStatus.OK,
    description="Обновить объект Measure в базе данных.",
    response_description="DSO",
    response_model=MeasureV0,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.MEASURE_EDIT]))],
)
@hide_endpoint("HIDE_UPDATE_MEASURE_BY_NAME")
async def update_measure_by_name(
    request: Request,
    service: Annotated[MeasureService, Depends(get_measure_service)],
    measure_name: str = Path(alias="measureName"),
    model_name: str = Path(alias="modelName"),
    measure: MeasureEditRequest = Body(alias="measure"),
    tenant_id: str = Path(alias="tenantName"),
) -> MeasureV0:
    """Обновить объект Measure в базе данных."""
    audit_kwargs = {
        "audit_type": audit_types.C4,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": Measure.__qualname__,
        "message": measure.model_dump(by_alias=True, mode="json", exclude_unset=True),
        "object_properties": measure.model_fields_set,
        "extra": {"include_fields": ["object_id", "object_name"]},
    }
    try:
        message = await service.get_updated_fields(
            tenant_id=tenant_id,
            model_name=model_name,
            name=measure_name,
            measure=measure,
        )
        result = await service.update_measure_by_name_and_schema(
            tenant_id=tenant_id,
            model_name=model_name,
            name=measure_name,
            measure=measure,
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
    return MeasureV0.model_validate(result)


@router.post(
    MEASURE_WITHOUT_MODEL_URL + "/copyToAnotherModel",
    status_code=HTTPStatus.CREATED,
    description="Скопировать measure в другие модели.",
    response_description="DetailsObjectCopyReponse",
    response_model=DetailsObjectCopyReponse,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.MEASURE_EDIT]))],
)
@hide_endpoint("HIDE_COPY_MODEL_MEASURE")
async def copy_model_measure(
    request: Request,
    service: Annotated[MeasureService, Depends(get_measure_service)],
    copy_model: CopyModelRequest = Body(alias="copy_model"),
    tenant_id: str = Path(alias="tenantName"),
) -> DetailsObjectCopyReponse | JSONResponse:
    """Копировать объект Measure в другие модели."""
    audit_kwargs = {
        "audit_type": audit_types.C4,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": Measure.__qualname__,
        "message": {"models": copy_model.models, "measures": copy_model.objects},
        "object_properties": ["models"],
        "extra": {"include_fields": ["object_id", "object_name"]},
    }
    try:
        copy_model_response, has_error = await service.copy_model_measures(
            tenant_id, copy_model.models, copy_model.objects
        )
        audit_kwargs["audit_type"] = audit_types.C3
        if has_error:
            return JSONResponse(content=copy_model_response.model_dump(), status_code=HTTPStatus.INTERNAL_SERVER_ERROR)
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
    return copy_model_response
