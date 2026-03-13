"""
API для признаков.
"""

import asyncio

from http import HTTPStatus
from typing import Annotated, Optional

from fastapi import APIRouter, Body, Depends, File, Header, HTTPException, Path, Query, Request, UploadFile
from fastapi.responses import JSONResponse
from py_common_lib.logger import EPMPYLogger, audit_types
from py_common_lib.logger.enums import StatusType
from py_common_lib.permissions import PermissionChecker
from sqlalchemy.exc import IntegrityError, NoResultFound, SQLAlchemyError

from src.api.dependencies import get_pagination_params
from src.api.v0.const import DATASTORAGE_URL, DATASTORAGE_URL_WITHOUT_MODEL
from src.api.v0.data_storage.dependencies import get_dso_service
from src.api.v0.enums import CacheNamespaceEnum
from src.cache.decorator import cache
from src.cache.types import CacheHeaderEnum
from src.models.copy_model import CopyModelRequest, DetailsObjectCopyReponse
from src.models.data_storage import DataStorage, DataStorageCreateRequest, DataStorageEditRequest, DataStorageV0
from src.models.database_object import DatabaseObject, DatabaseObjectRequest
from src.models.exceptions import SemanticObjectRelationException
from src.models.model_import import ImportFromFileResponse
from src.models.permissions import PermissionEnum
from src.models.request_params import Pagination
from src.service.data_storage import DataStorageService
from src.utils.exceptions import HTTPExceptionWithAuditLogging
from src.utils.hide_endpoint_decorator import hide_endpoint

logger = EPMPYLogger(__name__)
router = APIRouter()


@router.get(
    DATASTORAGE_URL + "/",
    status_code=HTTPStatus.OK,
    description="Получить список DSO",
    response_description="Список DSO",
    response_model=list[DataStorageV0],
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.DATASTORAGE_VIEW]))],
)
@hide_endpoint("HIDE_GET_DATA_STORAGE_LIST_BY_MODEL_NAME")
@cache(namespace=CacheNamespaceEnum.DATASTORAGE)
async def get_data_storage_list_by_model_name(
    request: Request,
    service: Annotated[DataStorageService, Depends(get_dso_service)],
    pagination: Annotated[Pagination, Depends(get_pagination_params)],
    model_name: str = Path(alias="modelName"),
    tenant_id: str = Path(alias="tenantName"),
    cache_header: CacheHeaderEnum | str = Header(alias="Cache-Control", default=CacheHeaderEnum.EMPTY),
) -> list[DataStorageV0]:
    """Получить список всех DSO."""
    print(f"REAL WORK dataStorage list tenant={tenant_id} model={model_name}", flush=True)
    await asyncio.sleep(20)
    audit_kwargs = {
        "audit_type": audit_types.C2,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": DataStorage.__qualname__,
    }
    try:
        result = await service.get_data_storage_list_by_model_name(
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
            audit_kwargs=audit_kwargs,
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail=reason,
        )
    except Exception as ex:  # noqa: PIE786
        reason = "UNKNOWN ERROR: " + str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs,
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail=reason,
        )
    return [DataStorageV0.model_validate(data_storage) for data_storage in result]


@router.get(
    DATASTORAGE_URL + "/{dataStorageName}",
    status_code=HTTPStatus.OK,
    description="Получить DSO по имени",
    response_description="DSO",
    response_model=DataStorageV0,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.DATASTORAGE_VIEW]))],
)
@hide_endpoint("HIDE_GET_DATA_STORAGE_BY_DS_NAME")
@cache(namespace=CacheNamespaceEnum.DATASTORAGE)
async def get_data_storage_by_ds_name(
    request: Request,
    service: Annotated[DataStorageService, Depends(get_dso_service)],
    model_name: str = Path(alias="modelName"),
    data_storage_name: str = Path(alias="dataStorageName"),
    tenant_id: str = Path(alias="tenantName"),
    cache_header: CacheHeaderEnum | str = Header(alias="Cache-Control", default=CacheHeaderEnum.EMPTY),
) -> DataStorageV0:
    """Получить DSO по имени."""
    audit_kwargs = {
        "audit_type": audit_types.C2,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": DataStorage.__qualname__,
    }
    try:
        result = await service.get_data_storage_by_name(
            name=data_storage_name, tenant_id=tenant_id, model_name=model_name
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
            audit_kwargs=audit_kwargs,
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail=reason,
        )
    except Exception as ex:  # noqa: PIE786
        reason = "UNKNOWN ERROR: " + str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs,
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail=reason,
        )
    return DataStorageV0.model_validate(result)


@router.post(
    DATASTORAGE_URL + "/create",
    status_code=HTTPStatus.CREATED,
    description="Создать объект DataStorage",
    response_description="DSO",
    response_model=DataStorageV0,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.DATASTORAGE_CREATE]))],
)
@router.post(
    DATASTORAGE_URL + "/",
    status_code=HTTPStatus.CREATED,
    description="Создать объект DataStorage",
    response_description="DSO",
    response_model=DataStorageV0,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.DATASTORAGE_CREATE]))],
)
@hide_endpoint("HIDE_CREATE_DATA_STORAGE")
async def create_data_storage(
    request: Request,
    service: Annotated[DataStorageService, Depends(get_dso_service)],
    model_name: str = Path(alias="modelName"),
    data_storage: DataStorageCreateRequest = Body(alias="dataStorage"),
    tenant_id: str = Path(alias="tenantName"),
    if_not_exists: bool = Query(default=False, alias="ifNotExists"),
    generate_on_db: bool = Query(default=True, alias="generateOnDB"),
) -> DataStorageV0:
    """Создать объект DataStorage в базе данных"""
    audit_kwargs = {
        "audit_type": audit_types.C4,
        "status": StatusType.FAIL,
        "object_id": request.url.path + data_storage.name,
        "object_properties": data_storage.model_fields_set,
        "message": data_storage.model_dump(by_alias=True, mode="json", exclude_unset=True),
        "object_name": DataStorage.__qualname__,
        "extra": {"include_fields": ["object_id", "object_name"]},
    }
    try:
        result = await service.create_data_storage_by_schema(
            tenant_id=tenant_id,
            model_name=model_name,
            data_storage=data_storage,
            if_not_exists=if_not_exists,
            generate_on_db=generate_on_db,
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
            audit_kwargs=audit_kwargs,
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail=reason,
        )
    except Exception as ex:  # noqa: PIE786
        reason = "UNKNOWN ERROR: " + str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs,
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail=reason,
        )
    return DataStorageV0.model_validate(result)


@router.post(
    DATASTORAGE_URL + "/byDbObject",
    status_code=HTTPStatus.OK,
    description="Получить DataStorage по dbObject.",
    response_description="DSO",
    response_model=DataStorageV0,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.DATABASE_CREATE]))],
)
@hide_endpoint("HIDE_GET_DATA_STORAGE_BY_DB_OBJECT")
@cache(namespace=CacheNamespaceEnum.DATASTORAGE)
async def get_data_storage_by_db_object(
    request: Request,
    service: Annotated[DataStorageService, Depends(get_dso_service)],
    tenant_id: str = Path(alias="tenantName"),
    model_name: str = Path(alias="modelName"),
    db_object: DatabaseObjectRequest = Body(alias="dbObject"),
    cache_header: CacheHeaderEnum | str = Header(alias="Cache-Control", default=CacheHeaderEnum.EMPTY),
) -> DataStorageV0:
    """Получить DataStorage по dbObject."""
    audit_kwargs = {
        "audit_type": audit_types.C2,
        "status": StatusType.FAIL,
        "object_id": request.url.path + f"?name={db_object.name}&schema_name={db_object.schema_name}",
        "object_name": DatabaseObject.__qualname__,
    }
    try:
        result = await service.get_data_storage_by_db_object(
            tenant_id=tenant_id, model_name=model_name, db_object=db_object
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
            audit_kwargs=audit_kwargs,
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail=reason,
        )
    except Exception as ex:  # noqa: PIE786
        reason = "UNKNOWN ERROR: " + str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs,
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail=reason,
        )
    return DataStorageV0.model_validate(result)


@router.delete(
    DATASTORAGE_URL + "/{dataStorageName}",
    status_code=HTTPStatus.NO_CONTENT,
    description="Удалить объект DataBase из базы данных.",
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.DATABASE_DELETE]))],
)
@hide_endpoint("HIDE_DELETE_DATA_STORAGE_BY_NAME")
async def delete_data_storage_by_name(
    request: Request,
    service: Annotated[DataStorageService, Depends(get_dso_service)],
    model_name: str = Path(alias="modelName"),
    data_storage_name: str = Path(alias="dataStorageName"),
    tenant_id: str = Path(alias="tenantName"),
    if_exists: bool = Query(default=False, alias="ifExists"),
) -> None:
    """Удалить объект DataStorage из базы данных."""
    audit_kwargs = {
        "audit_type": audit_types.C6,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": DataStorage.__qualname__,
    }
    try:
        await service.delete_data_storage_by_name(
            tenant_id=tenant_id,
            model_name=model_name,
            name=data_storage_name,
            if_exists=if_exists,
        )
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
            audit_kwargs=audit_kwargs,
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail=reason,
        )
    except SemanticObjectRelationException as ex:
        reason = "There are semantic objects related to this data storage." + str(ex)
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
            audit_kwargs=audit_kwargs,
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail=reason,
        )


@router.patch(
    DATASTORAGE_URL + "/{dataStorageName}",
    status_code=HTTPStatus.OK,
    description="Обновить объект DataStorage из базы данных.",
    response_description="DSO",
    response_model=DataStorageV0,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.DATABASE_EDIT]))],
)
@hide_endpoint("HIDE_UPDATE_DATA_STORAGE_BY_NAME")
async def update_data_storage_by_name(
    request: Request,
    service: Annotated[DataStorageService, Depends(get_dso_service)],
    data_storage_name: str = Path(alias="dataStorageName"),
    model_name: str = Path(alias="modelName"),
    data_storage: DataStorageEditRequest = Body(alias="dataBase"),
    tenant_id: str = Path(alias="tenantName"),
    generate_on_db: bool = Query(default=True, alias="generateOnDB"),
    enable_delete_column: bool = Query(default=True, alias="enableDeleteColumn"),
    enable_delete_not_empty: bool = Query(default=False, alias="enableDeleteNotEmpty"),
) -> DataStorageV0:
    """Обновить объект DataStorage из базы данных."""
    audit_kwargs = {
        "audit_type": audit_types.C4,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": DataStorage.__qualname__,
        "message": data_storage.model_dump(by_alias=True, mode="json", exclude_unset=True),
        "object_properties": data_storage.model_fields_set,
        "extra": {"include_fields": ["object_id", "object_name"]},
    }
    try:
        message = await service.get_updated_fields(
            tenant_id=tenant_id,
            model_name=model_name,
            name=data_storage_name,
            data_storage=data_storage,
        )
        result = await service.update_data_storage_by_name_and_schema(
            tenant_id=tenant_id,
            model_name=model_name,
            name=data_storage_name,
            data_storage=data_storage,
            generate_on_db=generate_on_db,
            enable_delete_column=enable_delete_column,
            enable_delete_not_empty=enable_delete_not_empty,
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
            audit_kwargs=audit_kwargs,
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail=reason,
        )
    except Exception as ex:  # noqa: PIE786
        reason = "UNKNOWN ERROR: " + str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs,
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail=reason,
        )
    return DataStorageV0.model_validate(result)


@router.post(
    DATASTORAGE_URL_WITHOUT_MODEL + "/copyToAnotherModel",
    status_code=HTTPStatus.CREATED,
    description="Скопировать dataStorage в другие модели.",
    response_description="DetailsObjectCopyReponse",
    response_model=DetailsObjectCopyReponse,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.DATASTORAGE_EDIT]))],
)
@hide_endpoint("HIDE_COPY_MODEL_DATA_STORAGE")
async def copy_model_data_storage(
    request: Request,
    service: Annotated[DataStorageService, Depends(get_dso_service)],
    copy_model: CopyModelRequest = Body(alias="copy_model"),
    tenant_id: str = Path(alias="tenantName"),
    generate_on_db: bool = Query(default=True, alias="generateOnDB"),
    if_not_exists: bool = Query(default=True, alias="ifNotExists"),
) -> DetailsObjectCopyReponse | JSONResponse:
    """Копировать объект Measure в другие модели."""
    audit_kwargs = {
        "audit_type": audit_types.C4,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": DataStorage.__qualname__,
        "message": {"models": copy_model.models, "measures": copy_model.objects},
        "object_properties": ["models"],
        "extra": {"include_fields": ["object_id", "object_name"]},
    }
    try:
        dso, has_error = await service.copy_model_data_storages(
            tenant_id,
            copy_model.models,
            copy_model.objects,
            generate_on_db,
            if_not_exist=if_not_exists,
        )
        audit_kwargs["audit_type"] = audit_types.C3
        if has_error:
            return JSONResponse(content=dso.model_dump(), status_code=HTTPStatus.INTERNAL_SERVER_ERROR)
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
            audit_kwargs=audit_kwargs,
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail=reason,
        )
    except Exception as ex:  # noqa: PIE786
        reason = "UNKNOWN ERROR: " + str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs,
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail=reason,
        )
    return dso


@router.post(
    DATASTORAGE_URL + "/create/model",
    status_code=HTTPStatus.CREATED,
    description="Создать несколько DataStorage из xlsx или csv.",
    response_description="Список созданных и обновленных DataStorage.",
    response_model=ImportFromFileResponse,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.DATABASE_CREATE]))],
)
@hide_endpoint("HIDE_CREATE_MODEL_DATASTORAGE")
async def create_model_data_storage(
    request: Request,
    service: Annotated[DataStorageService, Depends(get_dso_service)],
    model_name: str = Path(alias="modelName"),
    tenant_id: str = Path(alias="tenantName"),
    data_storages: Optional[UploadFile] = File(default=None, alias="dataStorages"),
    fields: Optional[UploadFile] = File(default=None),
) -> ImportFromFileResponse:
    """
    Создать несколько объектов DataStorage в базе данных из файлов.

    Args:
        request: (Request): запрос пользователя
        service (DataStorageService): сервис DataStorage
        tenant_id (str): тенант, где обновить/создать DataStorage
        model_name (str): имя модели в которой обновить/создать DataStorage
        data_storages (Optional[UploadFile]): файл формата csv или xlsx с списком DataStorage на создание.
        fields (Optional[UploadFile]): файл формата csv или xlsx с списком полей DataStorage на обновление.
    Returns:
        ImportFromFileResponse: Модель, содержащая списки успешно/неуспешно созданных/обновленных DataStorage
    """
    try:
        result = await service.create_or_update_data_storages_by_files(
            tenant_id=tenant_id,
            model_name=model_name,
            data_storages_file=data_storages,
            fields_file=fields,
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
