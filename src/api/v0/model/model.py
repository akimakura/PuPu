"""
API для моделей.
"""

from http import HTTPStatus
from typing import Annotated

from fastapi import APIRouter, Body, Depends, Header, HTTPException, Path, Query, Request
from py_common_lib.logger import EPMPYLogger, audit_types
from py_common_lib.logger.enums import StatusType
from py_common_lib.permissions import PermissionChecker
from sqlalchemy.exc import IntegrityError, NoResultFound, SQLAlchemyError

from src.api.dependencies import get_pagination_params
from src.api.v0.enums import CacheNamespaceEnum
from src.api.v0.model.dependencies import get_meta_sync_service, get_model_service
from src.api.v1.data_storage.dependencies import get_dso_service
from src.cache.decorator import cache
from src.cache.types import CacheHeaderEnum
from src.models.database_object import DatabaseObject as DatabaseObjectModel
from src.models.meta_synchronizer import DetailMetaSynchronizerResponse, DetailsMetaSynchronizerResponse
from src.models.model import Model, ModelCreateRequest, ModelEditRequest
from src.models.permissions import PermissionEnum
from src.models.request_params import Pagination
from src.models.tenant import SemanticObjectsTypeEnum
from src.repository.database_object import DatabaseObjectRepository
from src.service.data_storage import DataStorageService
from src.service.meta_synchronizer import MetaSynchronizerService
from src.service.model import ModelService
from src.utils.exceptions import HTTPExceptionWithAuditLogging
from src.utils.hide_endpoint_decorator import hide_endpoint

logger = EPMPYLogger(__name__)
router = APIRouter()


@router.get(
    "/",
    status_code=HTTPStatus.OK,
    description="Получить список всех моделей.",
    response_description="Список моделей.",
    response_model=list[Model],
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.MODEL_VIEW]))],
)
@hide_endpoint("HIDE_GET_MODEL_LIST")
@cache(namespace=CacheNamespaceEnum.MODEL)
async def get_model_list(
    request: Request,
    service: Annotated[ModelService, Depends(get_model_service)],
    pagination: Annotated[Pagination, Depends(get_pagination_params)],
    tenant_id: str = Path(alias="tenantName"),
    cache_header: CacheHeaderEnum | str = Header(alias="Cache-Control", default=CacheHeaderEnum.EMPTY),
) -> list[Model]:
    """Получить список всех Model."""
    audit_kwargs = {
        "audit_type": audit_types.C2,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": Model.__qualname__,
    }
    try:
        result = await service.get_model_list(tenant_id=tenant_id, pagination=pagination)
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
    "/{modelName}",
    status_code=HTTPStatus.OK,
    description="Получить модель по имени.",
    response_description="Модель",
    response_model=Model,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.MODEL_VIEW]))],
)
@hide_endpoint("HIDE_GET_MODEL_BY_NAME")
@cache(namespace=CacheNamespaceEnum.MODEL)
async def get_model_by_name(
    request: Request,
    service: Annotated[ModelService, Depends(get_model_service)],
    name: str = Path(alias="modelName"),
    tenant_id: str = Path(alias="tenantName"),
    cache_header: CacheHeaderEnum | str = Header(alias="Cache-Control", default=CacheHeaderEnum.EMPTY),
) -> Model:
    """Получить Model по имени."""
    audit_kwargs = {
        "audit_type": audit_types.C2,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": Model.__qualname__,
    }
    try:
        result = await service.get_model_by_name(tenant_id=tenant_id, name=name)
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
    "/{modelName}",
    status_code=HTTPStatus.NO_CONTENT,
    description="Удалить объект Model из базы данных.",
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.MODEL_DELETE]))],
)
@hide_endpoint("HIDE_DELETE_MODEL_BY_NAME")
async def delete_model_by_name(
    request: Request,
    service: Annotated[ModelService, Depends(get_model_service)],
    name: str = Path(alias="modelName"),
    tenant_id: str = Path(alias="tenantName"),
) -> None:
    """Удалить объект Model из базы данных."""
    audit_kwargs = {
        "audit_type": audit_types.C6,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": Model.__qualname__,
    }
    try:
        await service.delete_model_by_name(tenant_id=tenant_id, model_name=name)
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
    description="Создать объект Model",
    response_description="Модель",
    response_model=Model,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.MODEL_CREATE]))],
)
@hide_endpoint("HIDE_CREATE_MODEL")
async def create_model(
    request: Request,
    service: Annotated[ModelService, Depends(get_model_service)],
    model: ModelCreateRequest = Body(alias="model"),
    tenant_id: str = Path(alias="tenantName"),
) -> Model:
    """Создать объект Model в базе данных"""
    audit_kwargs = {
        "audit_type": audit_types.C4,
        "status": StatusType.FAIL,
        "object_id": request.url.path + model.name,
        "object_properties": model.model_fields_set,
        "message": model.model_dump(by_alias=True, mode="json", exclude_unset=True),
        "object_name": Model.__qualname__,
        "extra": {"include_fields": ["object_id", "object_name"]},
    }
    try:
        result = await service.create_model_by_schema(tenant_id=tenant_id, model=model)
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
    "/{modelName}",
    status_code=HTTPStatus.OK,
    description="Обновить объект Model из базы данных.",
    response_description="Модель",
    response_model=Model,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.MODEL_EDIT]))],
)
@hide_endpoint("HIDE_UPDATE_MODEL_BY_NAME")
async def update_model_by_name(
    request: Request,
    service: Annotated[ModelService, Depends(get_model_service)],
    name: str = Path(alias="modelName"),
    model: ModelEditRequest = Body(alias="model"),
    tenant_id: str = Path(alias="tenantName"),
    enable_recreate_not_empty_tables: bool = Query(alias="enableRecreateNotEmptyTables", default=False),
) -> Model:
    """Обновить объект Model из базы данных."""
    audit_kwargs = {
        "audit_type": audit_types.C4,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": Model.__qualname__,
        "message": model.model_dump(by_alias=True, mode="json", exclude_unset=True),
        "object_properties": model.model_fields_set,
        "extra": {"include_fields": ["object_id", "object_name"]},
    }
    try:
        message = await service.get_updated_fields(
            tenant_id=tenant_id,
            name=name,
            model=model,
        )
        result = await service.update_model_by_name_and_schema(
            tenant_id=tenant_id,
            model_name=name,
            model=model,
            enable_recreate_not_empty_tables=enable_recreate_not_empty_tables,
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


@router.post(
    "/{modelName}/findDependentObjects",
    status_code=HTTPStatus.OK,
    description="Собрать зависимые объекты для модели (пока только VIEW для DATA_STORAGE).",
    response_description="Список databaseObject.",
    response_model=list[DatabaseObjectModel],
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.DATASTORAGE_EDIT]))],
)
async def find_dependent_objects(
    request: Request,
    service: Annotated[DataStorageService, Depends(get_dso_service)],
    model_name: str = Path(alias="modelName"),
    tenant_id: str = Path(alias="tenantName"),
    object_type: SemanticObjectsTypeEnum = Query(alias="objectType"),
    payload: list[str] | None = Body(default=None),
) -> list[DatabaseObjectModel]:
    """Запустить сбор зависимых VIEW и вернуть список databaseObject."""
    audit_kwargs = {
        "audit_type": audit_types.C4,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": DatabaseObjectModel.__qualname__,
        "object_properties": [payload] if payload else [],
        "message": payload if payload else [],
    }
    try:
        if object_type != SemanticObjectsTypeEnum.DATA_STORAGE:
            raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail="Only DATA_STORAGE is supported.")
        object_names = payload if payload else None
        collected_ids = await service.collect_views_for_model(tenant_id, model_name, object_names)
        if not collected_ids:
            audit_kwargs["audit_type"] = audit_types.C3
            audit_kwargs["status"] = StatusType.SUCCESS
            logger.audit(**audit_kwargs)
            return []
        db_objects = await DatabaseObjectRepository(service.data_repository.session).get_by_ids(
            sorted(set(collected_ids)),
            tenant_id=tenant_id,
        )
        result = [DatabaseObjectModel.model_validate(db_object) for db_object in db_objects]
        audit_kwargs["audit_type"] = audit_types.C3
        audit_kwargs["status"] = StatusType.SUCCESS
        logger.audit(**audit_kwargs)
        return result
    except HTTPException as ex:
        audit_kwargs["reason"] = str(ex.detail)
        logger.exception(audit_kwargs["reason"])
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs, status_code=ex.status_code, detail=str(ex.detail)
        )
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


@router.post(
    "/{modelName}/synchronization/dataStorages/",
    status_code=HTTPStatus.OK,
    description="Обновить все таблицы в базе данных",
    response_description="Обновленные модели",
    response_model=DetailsMetaSynchronizerResponse,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.MODEL_CREATE]))],
)
@hide_endpoint("HIDE_CREATE_DATA_STORAGES_IN_DATABASE_FROM_META")
async def create_data_storages_in_database_from_meta(
    request: Request,
    service: Annotated[MetaSynchronizerService, Depends(get_meta_sync_service)],
    tenant_id: str = Path(alias="tenantName"),
    model_name: str = Path(alias="modelName"),
    check_possible_to_drop_model: bool = Query(
        alias="checkPossibleToDropModel",
        default=True,
    ),
    ignore: list[str] = Body(default=[]),
    recreate: bool = Query(alias="recreate", default=False),
    generate_on_db: bool = Query(alias="generateOnDB", default=True),
    enable_delete_column: bool = Query(alias="enableDeleteColumn", default=True),
    enable_delete_not_empty: bool = Query(alias="enableDeleteNotEmpty", default=False),
) -> DetailsMetaSynchronizerResponse:
    """
    Обновить все таблицы в базе данных, которые привязаны к модели.
    Создать или пересоздать таблицы (если они не пустые).
    Args:
        check_possible_to_drop_model (bool): Флаг запрещает пересоздавать все таблицы,
    если хотя бы одна из таблиц непустая.
    """
    try:
        if recreate:
            return await service.create_data_storages_in_database_from_meta(
                tenant_id,
                model_name,
                ignore,
                check_possible_to_drop_model,
            )
        return await service.update_data_storages_in_database_from_meta(
            tenant_id,
            model_name,
            ignore,
            generate_on_db=generate_on_db,
            enable_delete_column=enable_delete_column,
            enable_delete_not_empty=enable_delete_not_empty,
        )
    except Exception as ex:  # noqa: PIE786
        reason = "UNKNOWN ERROR: " + str(ex)
        logger.exception(reason)
        raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=reason)


@router.post(
    "/{modelName}/synchronization/dataStorages/{dataStorageName}",
    status_code=HTTPStatus.OK,
    description="Обновить таблицу в базе данных",
    response_description="Обновленная таблица",
    response_model=DetailMetaSynchronizerResponse,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.MODEL_CREATE]))],
)
@hide_endpoint("HIDE_CREATE_DATA_STORAGE_IN_DATABASE_FROM_META")
async def create_data_storage_in_database_from_meta(
    request: Request,
    service: Annotated[MetaSynchronizerService, Depends(get_meta_sync_service)],
    tenant_id: str = Path(alias="tenantName"),
    model_name: str = Path(alias="modelName"),
    data_storage_name: str = Path(alias="dataStorageName"),
    recreate: bool = Query(alias="recreate", default=False),
    generate_on_db: bool = Query(alias="generateOnDB", default=True),
    enable_delete_column: bool = Query(alias="enableDeleteColumn", default=True),
    enable_delete_not_empty: bool = Query(alias="enableDeleteNotEmpty", default=False),
) -> DetailMetaSynchronizerResponse:
    """
    Обновить таблицу в базе данных для DataStorage.
    Создать таблицу в базе данных, если её нет или пересоздать (если она пустая).
    """
    try:
        if recreate:
            return await service.create_data_storage_in_database_from_meta(tenant_id, model_name, data_storage_name)
        return await service.update_data_storage_in_database_from_meta(
            tenant_id,
            model_name,
            data_storage_name,
            generate_on_db=generate_on_db,
            enable_delete_column=enable_delete_column,
            enable_delete_not_empty=enable_delete_not_empty,
        )
    except Exception as ex:  # noqa: PIE786
        reason = "UNKNOWN ERROR: " + str(ex)
        logger.exception(reason)
        raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=reason)


@router.post(
    "/{modelName}/synchronization/composites/",
    status_code=HTTPStatus.OK,
    description="Обновить все view в базе данных",
    response_description="Обновленные view",
    response_model=DetailsMetaSynchronizerResponse,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.MODEL_CREATE]))],
)
@hide_endpoint("HIDE_CREATE_COMPOSITES_IN_DATABASE_FROM_META")
async def create_composites_in_database_from_meta(
    request: Request,
    service: Annotated[MetaSynchronizerService, Depends(get_meta_sync_service)],
    tenant_id: str = Path(alias="tenantName"),
    model_name: str = Path(alias="modelName"),
    ignore: list[str] = Body(default=[]),
    replace: bool = Query(alias="recreate", default=True),
    generate_on_db: bool = Query(alias="generateOnDB", default=True),
) -> DetailsMetaSynchronizerResponse:
    """
    Обновить все view в базе данных, которые привязаны к модели.
    Создать или пересоздать view.
    """
    try:
        return await service.create_composites_in_database_from_meta(
            tenant_id,
            model_name,
            ignore,
            replace=replace,
            generate_on_db=generate_on_db,
        )
    except Exception as ex:  # noqa: PIE786
        reason = "UNKNOWN ERROR: " + str(ex)
        logger.exception(reason)
        raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=reason)


@router.post(
    "/{modelName}/synchronization/composites/{compositeName}",
    status_code=HTTPStatus.OK,
    description="Обновить view в базе данных",
    response_description="Обновленная view",
    response_model=DetailMetaSynchronizerResponse,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.MODEL_CREATE]))],
)
@hide_endpoint("HIDE_CREATE_COMPOSITE_IN_DATABASE_FROM_META")
async def create_composite_in_database_from_meta(
    request: Request,
    service: Annotated[MetaSynchronizerService, Depends(get_meta_sync_service)],
    tenant_id: str = Path(alias="tenantName"),
    model_name: str = Path(alias="modelName"),
    composite_name: str = Path(alias="compositeName"),
    replace: bool = Query(alias="recreate", default=True),
    generate_on_db: bool = Query(alias="generateOnDB", default=True),
) -> DetailMetaSynchronizerResponse:
    """
    Обновить view в базе данных для Composite.
    Создать view в базе данных, если её нет или пересоздать.
    """
    try:
        return await service.create_composite_in_database_from_meta(
            tenant_id, model_name, composite_name, replace=replace, generate_on_db=generate_on_db
        )
    except Exception as ex:  # noqa: PIE786
        reason = "UNKNOWN ERROR: " + str(ex)
        logger.exception(reason)
        raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=reason)
