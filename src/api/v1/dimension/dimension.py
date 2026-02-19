"""
API для измерений.
"""

from http import HTTPStatus
from typing import Annotated, Optional

from fastapi import APIRouter, Body, Depends, File, Header, HTTPException, Path, Query, Request, UploadFile
from fastapi.responses import JSONResponse
from py_common_lib.logger import EPMPYLogger, audit_types
from py_common_lib.logger.enums import StatusType
from py_common_lib.permissions import PermissionChecker
from sqlalchemy.exc import IntegrityError, NoResultFound, SQLAlchemyError

from src.api.dependencies import get_pagination_params
from src.api.v1.const import DIMENSION_URL, MODEL_NAME_URL
from src.api.v1.dimension.dependencies import get_dimension_service, get_meta_sync_service
from src.api.v1.enums import CacheNamespaceEnum
from src.cache.decorator import cache
from src.cache.types import CacheHeaderEnum
from src.config import settings
from src.integration.pv_dictionaries.models import PVDictionary, PVDictionaryWithoutName
from src.models.copy_model import CopyModelRequest, DetailsObjectCopyReponse
from src.models.dimension import Dimension, DimensionCreateRequest, DimensionEditRequest, DimensionV1
from src.models.exceptions import SemanticObjectRelationException
from src.models.meta_synchronizer import DetailsMetaSynchronizerResponse
from src.models.model_import import ImportFromFileResponse
from src.models.permissions import PermissionEnum
from src.models.request_params import Pagination
from src.service.dimension import DimensionService
from src.service.meta_synchronizer import MetaSynchronizerService
from src.utils.exceptions import HTTPExceptionWithAuditLogging
from src.utils.hide_endpoint_decorator import hide_endpoint

logger = EPMPYLogger(__name__)
router = APIRouter()


@router.get(
    MODEL_NAME_URL + DIMENSION_URL + "/",
    status_code=HTTPStatus.OK,
    description="Получить список всех измерений для модели.",
    response_description="Список измерений.",
    response_model=list[DimensionV1],
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.DIMENSION_VIEW]))],
)
@hide_endpoint("HIDE_GET_DIMENSION_LIST_BY_MODEL_NAME")
@cache(namespace=CacheNamespaceEnum.DIMENSION)
async def get_dimension_list_by_model_name(
    request: Request,
    service: Annotated[DimensionService, Depends(get_dimension_service)],
    pagination: Annotated[Pagination, Depends(get_pagination_params)],
    model_name: str = Path(alias="modelName"),
    tenant_id: str = Path(alias="tenantName"),
    cache_header: CacheHeaderEnum | str = Header(alias="Cache-Control", default=CacheHeaderEnum.EMPTY),
) -> list[DimensionV1]:
    """Получить список всех Dimension."""
    audit_kwargs = {
        "audit_type": audit_types.C2,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": Dimension.__qualname__,
    }
    try:
        result = await service.get_dimension_list_by_model_name(
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
    return [DimensionV1.model_validate(dimension) for dimension in result]


@router.get(
    MODEL_NAME_URL + DIMENSION_URL + "/{dimensionName}",
    status_code=HTTPStatus.OK,
    description="Получить измерение по имени.",
    response_description="Измерение.",
    response_model=DimensionV1,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.DIMENSION_VIEW]))],
)
@hide_endpoint("HIDE_GET_DIMENSION_BY_DIMENSION_NAME")
@cache(namespace=CacheNamespaceEnum.DIMENSION)
async def get_dimension_by_dimension_name(
    request: Request,
    service: Annotated[DimensionService, Depends(get_dimension_service)],
    model_name: str = Path(alias="modelName"),
    dimension_name: str = Path(alias="dimensionName"),
    tenant_id: str = Path(alias="tenantName"),
    cache_header: CacheHeaderEnum | str = Header(alias="Cache-Control", default=CacheHeaderEnum.EMPTY),
) -> DimensionV1:
    """Получить Dimension по имени."""
    audit_kwargs = {
        "audit_type": audit_types.C2,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": Dimension.__qualname__,
    }
    try:
        result = await service.get_dimension_by_dimension_name(
            tenant_id=tenant_id, name=dimension_name, model_name=model_name
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
    return DimensionV1.model_validate(result)


@router.post(
    MODEL_NAME_URL + DIMENSION_URL + "/",
    status_code=HTTPStatus.OK,
    description="Получить все измерения из списка имен",
    response_description="Список измерений.",
    response_model=list[DimensionV1],
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.DIMENSION_VIEW]))],
)
@hide_endpoint("HIDE_GET_DIMENSION_BY_NAMES")
@cache(namespace=CacheNamespaceEnum.DIMENSION)
async def get_dimension_by_names(
    request: Request,
    service: Annotated[DimensionService, Depends(get_dimension_service)],
    pagination: Annotated[Pagination, Depends(get_pagination_params)],
    names: list[str] = Body(alias="dimensionNames"),
    model_name: str = Path(alias="modelName"),
    tenant_id: str = Path(alias="tenantName"),
    cache_header: CacheHeaderEnum | str = Header(alias="Cache-Control", default=CacheHeaderEnum.EMPTY),
) -> list[DimensionV1]:
    """Получить Dimension по имени."""
    audit_kwargs = {
        "audit_type": audit_types.C2,
        "status": StatusType.FAIL,
        "object_id": request.url.path + f"?names={names}",
        "object_name": Dimension.__qualname__,
    }
    try:
        result = await service.get_dimension_list_by_names(
            tenant_id=tenant_id, names=names, model_name=model_name, pagination=pagination
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
    return [DimensionV1.model_validate(dimension) for dimension in result]


@router.delete(
    MODEL_NAME_URL + DIMENSION_URL + "/{dimensionName}",
    status_code=HTTPStatus.NO_CONTENT,
    description="Удалить объект Dimension из базы данных.",
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.DIMENSION_DELETE]))],
)
@hide_endpoint("HIDE_DELETE_DIMENSION_BY_NAME")
async def delete_dimension_by_name(
    request: Request,
    service: Annotated[DimensionService, Depends(get_dimension_service)],
    name: str = Path(alias="dimensionName"),
    model_name: str = Path(alias="modelName"),
    tenant_id: str = Path(alias="tenantName"),
    if_exists: bool = Query(alias="ifExists", default=False),
) -> None:
    """Удалить объект Dimension из базы данных."""
    audit_kwargs = {
        "audit_type": audit_types.C6,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": Dimension.__qualname__,
    }
    try:
        result = await service.delete_dimension_by_name(
            tenant_id=tenant_id,
            model_name=model_name,
            name=name,
            if_exists=if_exists,
        )
        if not result:
            raise Exception(f"Cannot delete dimension in blacklist models: {settings.MODELS_BLACKLIST}")
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
        reason = "There are semantic objects related to this dimension: " + str(ex)
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


@router.post(
    MODEL_NAME_URL + DIMENSION_URL + "/create",
    status_code=HTTPStatus.CREATED,
    description="Создать объект Dimension",
    response_description="База данных",
    response_model=DimensionV1,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.DIMENSION_CREATE]))],
)
@hide_endpoint("HIDE_CREATE_DIMENSION")
async def create_dimension(
    request: Request,
    service: Annotated[DimensionService, Depends(get_dimension_service)],
    dimension: DimensionCreateRequest = Body(alias="dimension"),
    model_name: str = Path(alias="modelName"),
    tenant_id: str = Path(alias="tenantName"),
    generate_on_db: bool = Query(default=False, alias="generateOnDB"),
    if_not_exists: bool = Query(default=False, alias="ifNotExists"),
) -> DimensionV1:
    """Создать объект Dimension в базе данных"""
    audit_kwargs = {
        "audit_type": audit_types.C4,
        "status": StatusType.FAIL,
        "object_id": request.url.path.replace("create", dimension.name),
        "object_properties": dimension.model_fields_set,
        "message": dimension.model_dump(by_alias=True, mode="json", exclude_unset=True),
        "object_name": Dimension.__qualname__,
        "extra": {"include_fields": ["object_id", "object_name"]},
    }
    try:
        result = await service.create_dimension_by_schema(
            tenant_id=tenant_id,
            model_name=model_name,
            dimension=dimension,
            generate_on_db=generate_on_db,
            if_not_exists=if_not_exists,
        )
        if result is None:
            raise Exception(f"Cannot create dimension in blacklist models: {settings.MODELS_BLACKLIST}")
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
    return DimensionV1.model_validate(result)


@router.patch(
    MODEL_NAME_URL + DIMENSION_URL + "/{dimensionName}",
    status_code=HTTPStatus.OK,
    description="Обновить объект Dimension из базы данных.",
    response_description="Измерение",
    response_model=DimensionV1,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.DIMENSION_EDIT]))],
)
@hide_endpoint("HIDE_UPDATE_DIMENSION_BY_NAME")
async def update_dimension_by_name(
    request: Request,
    service: Annotated[DimensionService, Depends(get_dimension_service)],
    name: str = Path(alias="dimensionName"),
    dimension: DimensionEditRequest = Body(alias="dimension"),
    model_name: str = Path(alias="modelName"),
    tenant_id: str = Path(alias="tenantName"),
    generate_on_db: bool = Query(default=False, alias="generateOnDB"),
    enable_delete_column: bool = Query(alias="enableDeleteColumn", default=True),
    enable_delete_not_empty: bool = Query(alias="enableDeleteNotEmpty", default=False),
) -> DimensionV1:
    """Обновить объект Dimension в базе данных."""
    audit_kwargs = {
        "audit_type": audit_types.C4,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": Dimension.__qualname__,
        "message": dimension.model_dump(by_alias=True, mode="json", exclude_unset=True),
        "object_properties": dimension.model_fields_set,
        "extra": {"include_fields": ["object_id", "object_name"]},
    }
    try:
        message = await service.get_updated_fields(
            tenant_id=tenant_id,
            model_name=model_name,
            name=name,
            dimension=dimension,
        )
        result = await service.update_dimension_by_name_and_schema(
            tenant_id=tenant_id,
            model_name=model_name,
            name=name,
            dimension=dimension,
            generate_on_db=generate_on_db,
            enable_delete_column=enable_delete_column,
            enable_delete_not_empty=enable_delete_not_empty,
        )
        if result is None:
            raise Exception(f"Cannot update dimension in blacklist models: {settings.MODELS_BLACKLIST}")
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
    return DimensionV1.model_validate(result)


@router.post(
    MODEL_NAME_URL + DIMENSION_URL + "/{dimensionName}/pvd",
    status_code=HTTPStatus.CREATED,
    description="Создать dimension в PVD.",
    response_description="Dimension",
    response_model=DimensionV1,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.DIMENSION_EDIT]))],
)
@hide_endpoint("HIDE_CREATE_DIMENSION_IN_PVD")
async def create_dimension_in_pvd(
    request: Request,
    service: Annotated[DimensionService, Depends(get_dimension_service)],
    pv_dictionary: Optional[PVDictionary] = Body(alias="dimension", default=None),
    model_name: str = Path(alias="modelName"),
    dimension_name: str = Path(alias="dimensionName"),
    tenant_id: str = Path(alias="tenantName"),
) -> DimensionV1:
    """Создать объект Dimension в PVD."""
    try:
        result = await service.create_pv_dictionary_by_dimension(
            tenant_id=tenant_id,
            name=dimension_name,
            pv_dictionary=pv_dictionary,
        )
    except IntegrityError as ex:
        reason = "BAD_REQUEST: " + str(ex)
        logger.exception(reason)
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=reason)
    except NoResultFound as ex:
        reason = str(ex)
        logger.exception(reason)
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail=reason)
    except SQLAlchemyError as ex:
        reason = "INTERNAL_SERVER_ERROR: " + str(ex)
        logger.exception(reason)
        raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=reason)
    except Exception as ex:  # noqa: PIE786
        reason = "UNKNOWN ERROR: " + str(ex)
        logger.exception(reason)
        raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=reason)
    return DimensionV1.model_validate(result)


@router.delete(
    MODEL_NAME_URL + DIMENSION_URL + "/{dimensionName}/pvd",
    status_code=HTTPStatus.NO_CONTENT,
    description="Удалить dimension в PVD.",
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.DIMENSION_DELETE]))],
)
@hide_endpoint("HIDE_DELETE_DIMENSION_IN_PVD")
async def delete_dimension_in_pvd(
    request: Request,
    service: Annotated[DimensionService, Depends(get_dimension_service)],
    dimension_name: str = Path(alias="dimensionName"),
    model_name: str = Path(alias="modelName"),
    tenant_id: str = Path(alias="tenantName"),
) -> None:
    """
    Удалить объект Dimension в PVD.

    Args:
        dimension_name (str): Имя объекта Dimension
        model_name: (str):  Имя модели, привязанного к объекту Dimension
        tenant_id: (str): Идентификатор тенанта
    """
    try:
        await service.delete_dimension_in_pvd_by_tenant_model_name(
            tenant_id=tenant_id,
            dimension_name=dimension_name,
            model_name=model_name,
        )
    except IntegrityError as ex:
        reason = "BAD_REQUEST: " + str(ex)
        logger.exception(reason)
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=reason)
    except NoResultFound as ex:
        reason = str(ex)
        logger.exception(reason)
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail=reason)
    except SQLAlchemyError as ex:
        reason = "INTERNAL_SERVER_ERROR: " + str(ex)
        logger.exception(reason)
        raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=reason)
    except Exception as ex:  # noqa: PIE786
        reason = "UNKNOWN ERROR: " + str(ex)
        logger.exception(reason)
        raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=reason)


@router.patch(
    MODEL_NAME_URL + DIMENSION_URL + "/{dimensionName}/pvd",
    status_code=HTTPStatus.CREATED,
    description="Обновить dimension в PVD.",
    response_description="Dimension",
    response_model=DimensionV1,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.DIMENSION_EDIT]))],
)
@hide_endpoint("HIDE_UPDATE_DIMENSION_IN_PVD")
async def update_dimension_in_pvd(
    request: Request,
    service: Annotated[DimensionService, Depends(get_dimension_service)],
    pv_dictionary: Optional[PVDictionary] = Body(alias="dimension", default=None),
    dimension_name: str = Path(alias="dimensionName"),
    model_name: str = Path(alias="modelName"),
    tenant_id: str = Path(alias="tenantName"),
) -> DimensionV1:
    """Создать объект Dimension в PVD."""
    try:
        result = await service.update_pv_dictionary_by_dimension(
            tenant_id=tenant_id,
            name=dimension_name,
            pv_dictionary=pv_dictionary,
        )
    except IntegrityError as ex:
        reason = "BAD_REQUEST: " + str(ex)
        logger.exception(reason)
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=reason)
    except NoResultFound as ex:
        reason = str(ex)
        logger.exception(reason)
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail=reason)
    except SQLAlchemyError as ex:
        reason = "INTERNAL_SERVER_ERROR: " + str(ex)
        logger.exception(reason)
        raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=reason)
    except Exception as ex:  # noqa: PIE786
        reason = "UNKNOWN ERROR: " + str(ex)
        logger.exception(reason)
        raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=reason)
    return DimensionV1.model_validate(result)


@router.post(
    MODEL_NAME_URL + DIMENSION_URL + "/pvd",
    status_code=HTTPStatus.CREATED,
    description="Создать все dimension из модели в PVD.",
    response_description="DetailsMetaSynchronizerResponse",
    response_model=DetailsMetaSynchronizerResponse,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.DIMENSION_EDIT]))],
)
@hide_endpoint("HIDE_CREATE_DIMENSIONS_IN_PVD")
async def create_dimensions_in_pvd(
    request: Request,
    service: Annotated[MetaSynchronizerService, Depends(get_meta_sync_service)],
    pv_dictionary: Optional[PVDictionaryWithoutName] = Body(alias="dimension", default=None),
    model_name: str = Path(alias="modelName"),
    tenant_id: str = Path(alias="tenantName"),
    recreate: bool = Query(default=False),
) -> DetailsMetaSynchronizerResponse:
    """
    Создать все справочники в PVD для модели.

    Args:
        tenant_id (str): Имя тенанта, в котором находятся справочники
        model_name (str): Имя модели, в которой находятся справочники
        pv_dictionary (Optional[PVDictionaryWithoutName]): Поля pv_dictionary с доменом и тенантом
    Returns:
        DetailsMetaSynchronizerResponse: Список успешно или неуспешно созданных справочников
    """
    try:
        result = await service.create_all_pvds_for_dimensions_in_model(
            tenant_id=tenant_id,
            model_name=model_name,
            pv_dictionary_without_name=pv_dictionary,
            recreate=recreate,
        )
    except IntegrityError as ex:
        reason = "BAD_REQUEST: " + str(ex)
        logger.exception(reason)
        logger.exception(reason)
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=reason)
    except NoResultFound as ex:
        reason = str(ex)
        logger.exception(reason)
        logger.exception(reason)
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail=reason)
    except SQLAlchemyError as ex:
        reason = "INTERNAL_SERVER_ERROR: " + str(ex)
        logger.exception(reason)
        logger.exception(reason)
        raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=reason)
    except Exception as ex:  # noqa: PIE786
        reason = "UNKNOWN ERROR: " + str(ex)
        logger.exception(reason)
        logger.exception(reason)
        raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=reason)
    return result


@router.post(
    MODEL_NAME_URL + DIMENSION_URL + "/create/model",
    status_code=HTTPStatus.CREATED,
    description="Создать несколько Dimension из xlsx или csv.",
    response_description="Список созданных и обновленных Dimension.",
    response_model=ImportFromFileResponse,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.DIMENSION_CREATE]))],
)
@hide_endpoint("HIDE_CREATE_MODEL_DIMENSION")
async def create_model_dimension(
    request: Request,
    service: Annotated[DimensionService, Depends(get_dimension_service)],
    model_name: str = Path(alias="modelName"),
    tenant_id: str = Path(alias="tenantName"),
    dimensions: Optional[UploadFile] = File(default=None),
    attributes: Optional[UploadFile] = File(default=None),
) -> ImportFromFileResponse:
    """Создать объект Dimension в базе данных"""
    try:
        result = await service.create_dimensions_by_files(
            tenant_id=tenant_id,
            model_name=model_name,
            dimensions_file=dimensions,
            attributes_file=attributes,
        )
    except IntegrityError as ex:
        logger.exception("BAD_REQUEST")
        reason = "BAD_REQUEST: " + str(ex)
        logger.exception(reason)
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=reason)
    except SQLAlchemyError as ex:
        logger.exception("INTERNAL_SERVER_ERROR")
        reason = "INTERNAL_SERVER_ERROR: " + str(ex)
        logger.exception(reason)
        raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=reason)
    except Exception as ex:  # noqa: PIE786
        logger.exception("UNKNOWN ERROR")
        reason = "UNKNOWN ERROR: " + str(ex)
        logger.exception(reason)
        raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=reason)
    return result


@router.post(
    DIMENSION_URL + "/copyToAnotherModel",
    status_code=HTTPStatus.CREATED,
    description="Скопировать dimension в другие модели.",
    response_description="DetailsObjectCopyReponse",
    response_model=DetailsObjectCopyReponse,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.DIMENSION_EDIT]))],
)
@hide_endpoint("HIDE_COPY_MODEL_DIMENSION")
async def copy_model_data_storage(
    request: Request,
    service: Annotated[DimensionService, Depends(get_dimension_service)],
    copy_model: CopyModelRequest = Body(alias="copy_model"),
    tenant_id: str = Path(alias="tenantName"),
    copy_attributes: bool = Query(alias="copyAttributes", default=True),
    generate_on_db: bool = Query(alias="generateOnDb", default=False),
    if_not_exists: bool = Query(alias="ifNotExists", default=True),
) -> DetailsObjectCopyReponse | JSONResponse:
    """Копировать объект Dimension в другие модели."""

    audit_kwargs = {
        "audit_type": audit_types.C4,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": Dimension.__qualname__,
        "message": {"models": copy_model.models, "dimensions": copy_model.objects},
        "object_properties": ["models"],
        "extra": {"include_fields": ["object_id", "object_name"]},
    }
    try:
        dimension, has_error = await service.copy_model_dimensions(
            tenant_id,
            copy_model.models,
            copy_model.objects,
            copy_attributes=copy_attributes,
            generate_on_db=generate_on_db,
            if_not_exists=if_not_exists,
        )
        audit_kwargs["audit_type"] = audit_types.C3
        if has_error:
            return JSONResponse(content=dimension.model_dump(), status_code=HTTPStatus.INTERNAL_SERVER_ERROR)
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
    return dimension
