"""
API для показателей.
"""

from http import HTTPStatus
from typing import Annotated, Optional

from fastapi import APIRouter, Body, Depends, Path, Request
from py_common_lib.logger import EPMPYLogger, audit_types
from py_common_lib.logger.enums import StatusType
from py_common_lib.permissions import PermissionChecker
from sqlalchemy.exc import IntegrityError, NoResultFound, SQLAlchemyError

from src.api.v0.const import DIMENSION_HIERARCHY_URL, HIERARCHY_URL, MODEL_NAME_URL
from src.api.v0.enums import CacheNamespaceEnum
from src.api.v0.hierarchy.dependencies import get_hierarchy_pvd_service, get_hierarchy_service
from src.cache.decorator import cache
from src.models.hierarchy import (
    HierarchyCopyResponse,
    HierarchyCreateRequest,
    HierarchyEditRequest,
    HierarchyMetaOut,
    HierarchyPvdCreateRequest,
)
from src.models.permissions import PermissionEnum
from src.service.hierarchy import HierarchyService
from src.service.pv_hierarchy import HierarchyPvdService
from src.utils.exceptions import HTTPExceptionWithAuditLogging
from src.utils.hide_endpoint_decorator import hide_endpoint

logger = EPMPYLogger(__name__)
router = APIRouter()


def __handle_errors(audit_kwargs: dict, ex: Exception) -> None:
    """
    Обработчик ошибок, регистрирующий исключения в аудите.

    Args:
        audit_kwargs (dict): Словарь с параметрами для записи в аудит.
        ex (Exception): Исключение, которое произошло и должно быть обработано.

    Returns:
        None: Метод не возвращает значения, он обрабатывает исключение и записывает информацию в журнал аудита.
    """
    if isinstance(ex, IntegrityError):
        reason = "BAD_REQUEST: " + str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs, status_code=HTTPStatus.BAD_REQUEST, detail=reason
        )
    if isinstance(ex, NoResultFound):
        reason = str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(audit_kwargs=audit_kwargs, status_code=HTTPStatus.NOT_FOUND, detail=reason)
    if isinstance(ex, ValueError):
        reason = str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs, status_code=HTTPStatus.BAD_REQUEST, detail=reason
        )
    if isinstance(ex, SQLAlchemyError):
        reason = "INTERNAL_SERVER_ERROR: " + str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs, status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=reason
        )
    if isinstance(ex, Exception):
        reason = "UNKNOWN ERROR: " + str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs, status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=reason
        )


@router.post(
    MODEL_NAME_URL + DIMENSION_HIERARCHY_URL + "/",
    status_code=HTTPStatus.OK,
    description="Получить иерархию по имени.",
    response_description="Иерархия.",
    response_model=list[HierarchyMetaOut],
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.HIERARCHY_VIEW]))],
)
@hide_endpoint("HIDE_GET_HIERARCHY_BY_HIERARCHY_NAME")
async def get_hierarchy_by_hierarchy_name(
    request: Request,
    service: Annotated[HierarchyService, Depends(get_hierarchy_service)],
    hierarchy_names: list[str] = Body(alias="hierarchyNames"),
    model_name: str = Path(alias="modelName"),
    dimension_name: str = Path(alias="dimensionName"),
    tenant_id: str = Path(alias="tenantName"),
) -> HierarchyCreateRequest:
    """
    Получает иерархии по их именам.

    Args:
        request (Request): HTTP-запрос от клиента.
        service (HierarchyService): Сервис для работы с иерархиями.
        hierarchy_names (list[str]): Список имён иерархий, которые нужно найти.
        model_name (str): Имя модели, в которой искать иерархии.
        dimension_name (str): Имя измерения, к которому относятся иерархии.
        tenant_id (str): Идентификатор арендатора (тенанта).

    Returns:
        list[HierarchyMetaOut]: Список найденных иерархий.
    """
    audit_kwargs = {
        "audit_type": audit_types.C2,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": HierarchyCreateRequest.__qualname__,
    }
    try:
        result = await service.get_multi(
            tenant_id=tenant_id,
            model_name=model_name,
            dimension_names=[dimension_name],
            hierarchy_names=hierarchy_names,
        )
        audit_kwargs["audit_type"] = audit_types.C1
        audit_kwargs["status"] = StatusType.SUCCESS
        logger.audit(**audit_kwargs)
    except Exception as ex:  # noqa: PIE786
        __handle_errors(audit_kwargs, ex)
    return result


@router.post(
    MODEL_NAME_URL + HIERARCHY_URL + "/",
    status_code=HTTPStatus.OK,
    description="Получить иерархию по имени.",
    response_description="Иерархия.",
    response_model=list[HierarchyMetaOut],
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.HIERARCHY_VIEW]))],
)
@hide_endpoint("HIDE_GET_HIERARCHY_BY_HIERARCHY_NAME_WITHOUT_DIMENSIONS")
async def get_hierarchy_by_hierarchy_name_without_dimensions(
    request: Request,
    service: Annotated[HierarchyService, Depends(get_hierarchy_service)],
    hierarchy_names: list[str] = Body(alias="hierarchyNames"),
    model_name: str = Path(alias="modelName"),
    tenant_id: str = Path(alias="tenantName"),
) -> HierarchyCreateRequest:
    """
    Получает иерархии по их именам.

    Args:
        request (Request): HTTP-запрос от клиента.
        service (HierarchyService): Сервис для работы с иерархиями.
        hierarchy_names (list[str]): Список имён иерархий, которые нужно найти.
        model_name (str): Имя модели, в которой искать иерархии.
        dimension_name (str): Имя измерения, к которому относятся иерархии.
        tenant_id (str): Идентификатор арендатора (тенанта).

    Returns:
        list[HierarchyMetaOut]: Список найденных иерархий.
    """
    audit_kwargs = {
        "audit_type": audit_types.C2,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": HierarchyCreateRequest.__qualname__,
    }
    try:
        result = await service.get_multi(
            tenant_id=tenant_id,
            model_name=model_name,
            dimension_names=None,
            hierarchy_names=hierarchy_names,
        )
        audit_kwargs["audit_type"] = audit_types.C1
        audit_kwargs["status"] = StatusType.SUCCESS
        logger.audit(**audit_kwargs)
    except Exception as ex:  # noqa: PIE786
        __handle_errors(audit_kwargs, ex)
    return result


@router.post(
    MODEL_NAME_URL + HIERARCHY_URL + "/byDimensions/",
    status_code=HTTPStatus.OK,
    description="Получить иерархию по имени.",
    response_description="Иерархия.",
    response_model=list[HierarchyMetaOut],
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.HIERARCHY_VIEW]))],
)
@hide_endpoint("HIDE_GET_HIERARCHY_BY_HIERARCHY_NAMES")
@cache(namespace=CacheNamespaceEnum.HIERARCHY)
async def get_hierarchy_by_dimension_names(
    request: Request,
    service: Annotated[HierarchyService, Depends(get_hierarchy_service)],
    dimension_names: list[str] = Body(alias="dimensionNames"),
    model_name: str = Path(alias="modelName"),
    tenant_id: str = Path(alias="tenantName"),
) -> HierarchyCreateRequest:
    """
    Получает иерархии по именам измерений.

    Args:
        request (Request): HTTP-запрос от клиента.
        service (HierarchyService): Сервис для работы с иерархиями.
        dimension_names (list[str]): Список имён измерений, по которым выбираются иерархии.
        model_name (str): Имя модели, в которой ищут иерархии.
        tenant_id (str): Идентификатор арендатора (тенанта).

    Returns:
        list[HierarchyMetaOut]: Список найденных иерархий.
    """
    audit_kwargs = {
        "audit_type": audit_types.C2,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": HierarchyCreateRequest.__qualname__,
    }
    try:
        result = await service.get_multi(
            tenant_id=tenant_id,
            model_name=model_name,
            dimension_names=dimension_names,
            hierarchy_names=[],
        )
        audit_kwargs["audit_type"] = audit_types.C1
        audit_kwargs["status"] = StatusType.SUCCESS
        logger.audit(**audit_kwargs)
    except Exception as ex:  # noqa: PIE786
        __handle_errors(audit_kwargs, ex)
    return result


@router.get(
    MODEL_NAME_URL + HIERARCHY_URL + "/",
    status_code=HTTPStatus.OK,
    description="Получить иерархию по имени.",
    response_description="Иерархия.",
    response_model=list[HierarchyMetaOut],
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.HIERARCHY_VIEW]))],
)
@hide_endpoint("HIDE_GET_HIERARCHY_BY_HIERARCHIES")
@cache(namespace=CacheNamespaceEnum.HIERARCHY)
async def get_hierarchy_by_hierarchies(
    request: Request,
    service: Annotated[HierarchyService, Depends(get_hierarchy_service)],
    model_name: str = Path(alias="modelName"),
    tenant_id: str = Path(alias="tenantName"),
) -> HierarchyCreateRequest:
    """
    Получает иерархии по их именам.

    Args:
        request (Request): HTTP-запрос от клиента.
        service (HierarchyService): Сервис для работы с иерархиями.
        hierarchy_names (list[str]): Список имён иерархий, которые нужно выбрать.
        model_name (str): Имя модели, в которой находятся иерархии.
        tenant_id (str): Идентификатор арендатора (тенанта).

    Returns:
        list[HierarchyMetaOut]: Список найденных иерархий.
    """
    audit_kwargs = {
        "audit_type": audit_types.C2,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": HierarchyCreateRequest.__qualname__,
    }
    try:
        result = await service.get_multi(
            tenant_id=tenant_id,
            model_name=model_name,
            dimension_names=None,
            hierarchy_names=[],
        )
        audit_kwargs["audit_type"] = audit_types.C1
        audit_kwargs["status"] = StatusType.SUCCESS
        logger.audit(**audit_kwargs)
    except Exception as ex:  # noqa: PIE786
        __handle_errors(audit_kwargs, ex)
    return result


@router.get(
    MODEL_NAME_URL + DIMENSION_HIERARCHY_URL + "/",
    status_code=HTTPStatus.OK,
    description="Получить иерархию по имени.",
    response_description="Иерархия.",
    response_model=list[HierarchyMetaOut],
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.HIERARCHY_VIEW]))],
)
@hide_endpoint("HIDE_GET_HIERARCHY_BY_HIERARCHIES_AND_DIMENSION")
@cache(namespace=CacheNamespaceEnum.HIERARCHY)
async def get_hierarchy_by_hierarchies_and_dimension(
    request: Request,
    service: Annotated[HierarchyService, Depends(get_hierarchy_service)],
    model_name: str = Path(alias="modelName"),
    dimension_name: str = Path(alias="dimensionName"),
    tenant_id: str = Path(alias="tenantName"),
) -> HierarchyCreateRequest:
    """
    Получает иерархии по имени измерения.

    Args:
        request (Request): HTTP-запрос от клиента.
        service (HierarchyService): Сервис для работы с иерархиями.
        model_name (str): Имя модели, в которой находятся иерархии.
        dimension_name (str): Имя измерения, по которому выбирают иерархии.
        tenant_id (str): Идентификатор арендатора (тенанта).

    Returns:
        list[HierarchyMetaOut]: Список найденных иерархий.
    """
    audit_kwargs = {
        "audit_type": audit_types.C2,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": HierarchyCreateRequest.__qualname__,
    }
    try:
        result = await service.get_multi(
            tenant_id=tenant_id,
            model_name=model_name,
            dimension_names=[dimension_name],
            hierarchy_names=[],
        )
        audit_kwargs["audit_type"] = audit_types.C1
        audit_kwargs["status"] = StatusType.SUCCESS
        logger.audit(**audit_kwargs)
    except Exception as ex:  # noqa: PIE786
        __handle_errors(audit_kwargs, ex)
    return result


@router.get(
    MODEL_NAME_URL + DIMENSION_HIERARCHY_URL + "/{hierarchyName}",
    status_code=HTTPStatus.OK,
    description="Получить иерархию по имени.",
    response_description="Иерархия.",
    response_model=HierarchyMetaOut,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.HIERARCHY_VIEW]))],
)
@hide_endpoint("HIDE_GET_HIERARCHY_DIMENSION_AND_HIERARCHY_NAME")
@cache(namespace=CacheNamespaceEnum.HIERARCHY)
async def get_hierarchy_dimension_and_hierarchy_name(
    request: Request,
    service: Annotated[HierarchyService, Depends(get_hierarchy_service)],
    model_name: str = Path(alias="modelName"),
    dimension_name: str = Path(alias="dimensionName"),
    hierarchy_name: str = Path(alias="hierarchyName"),
    tenant_id: str = Path(alias="tenantName"),
) -> HierarchyCreateRequest:
    """
    Получает одну иерархию по её имени.

    Args:
        request (Request): HTTP-запрос от клиента.
        service (HierarchyService): Сервис для работы с иерархиями.
        model_name (str): Имя модели, в которой находится иерархия.
        dimension_name (str): Имя измерения, к которому относится иерархия.
        hierarchy_name (str): Имя самой иерархии.
        tenant_id (str): Идентификатор арендатора (тенанта).

    Returns:
        HierarchyMetaOut: Найденная иерархия.
    """
    audit_kwargs = {
        "audit_type": audit_types.C2,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": HierarchyCreateRequest.__qualname__,
    }
    try:
        result = await service.get_multi(
            tenant_id=tenant_id,
            model_name=model_name,
            dimension_names=[dimension_name],
            hierarchy_names=[hierarchy_name],
        )
        audit_kwargs["audit_type"] = audit_types.C1
        audit_kwargs["status"] = StatusType.SUCCESS
        logger.audit(**audit_kwargs)
    except Exception as ex:  # noqa: PIE786
        __handle_errors(audit_kwargs, ex)
    return result[0]


@router.patch(
    MODEL_NAME_URL + DIMENSION_HIERARCHY_URL + "/{hierarchyName}",
    status_code=HTTPStatus.OK,
    description="Обновить иерархию по имени.",
    response_description="Иерархия.",
    response_model=HierarchyMetaOut,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.HIERARCHY_EDIT]))],
)
@hide_endpoint("HIDE_UPDATE_HIERARCHY")
async def update_hierarchy(
    request: Request,
    service: Annotated[HierarchyService, Depends(get_hierarchy_service)],
    hierarchy_update_data: Annotated[HierarchyEditRequest, Body(alias="hierarchy")],
    model_name: str = Path(alias="modelName"),
    dimension_name: str = Path(alias="dimensionName"),
    hierarchy_name: str = Path(alias="hierarchyName"),
    tenant_id: str = Path(alias="tenantName"),
) -> HierarchyMetaOut:
    """
    Обновляет иерархию по её имени.

    Args:
        request (Request): HTTP-запрос от клиента.
        service (HierarchyService): Сервис для работы с иерархиями.
        hierarchy_update_data (HierarchyEditRequest): Данные для обновления иерархии.
        model_name (str): Имя модели, к которой относится иерархия.
        dimension_name (str): Имя измерения, к которому относится иерархия.
        hierarchy_name (str): Имя иерархии, которую нужно обновить.
        tenant_id (str): Идентификатор арендатора (тенанта).

    Returns:
        HierarchyMetaOut: Обновлённая иерархия.
    """
    audit_kwargs = {
        "audit_type": audit_types.C2,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": HierarchyCreateRequest.__qualname__,
    }
    try:
        result = await service.update_hierarchy_by_schema(
            tenant_id=tenant_id,
            model_name=model_name,
            dimension_name=dimension_name,
            hierarchy=hierarchy_update_data,
            hierarchy_name=hierarchy_name,
        )
    except Exception as ex:  # noqa: PIE786
        __handle_errors(audit_kwargs, ex)

    return result


@router.delete(
    MODEL_NAME_URL + DIMENSION_HIERARCHY_URL + "/{hierarchyName}",
    status_code=HTTPStatus.NO_CONTENT,
    description="Удалить иерархию по имени.",
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.HIERARCHY_DELETE]))],
)
@hide_endpoint("HIDE_DELETE_HIERARCHY")
async def delete_hierarchy(
    request: Request,
    service: Annotated[HierarchyService, Depends(get_hierarchy_service)],
    model_name: str = Path(alias="modelName"),
    dimension_name: str = Path(alias="dimensionName"),
    hierarchy_name: str = Path(alias="hierarchyName"),
    tenant_id: str = Path(alias="tenantName"),
) -> None:
    """
    Удаляет иерархию по её имени.

    Args:
        request (Request): HTTP-запрос от клиента.
        service (HierarchyService): Сервис для работы с иерархиями.
        model_name (str): Имя модели, к которой относится иерархия.
        dimension_name (str): Имя измерения, к которому относится иерархия.
        hierarchy_name (str): Имя иерархии, которую нужно удалить.
        tenant_id (str): Идентификатор арендатора (тенанта).

    Returns:
        None: Метод не возвращает значения, он удаляет иерархию.
    """
    audit_kwargs = {
        "audit_type": audit_types.C2,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": HierarchyCreateRequest.__qualname__,
    }
    try:
        await service.delete_hierarchy(
            model_name=model_name, dimension_name=dimension_name, hierarchy_name=hierarchy_name, tenant_id=tenant_id
        )

    except Exception as ex:  # noqa: PIE786
        __handle_errors(audit_kwargs, ex)


@router.post(
    MODEL_NAME_URL + DIMENSION_HIERARCHY_URL + "/create",
    status_code=HTTPStatus.CREATED,
    description="Создать объект Hierarchy",
    response_description="Иерархия",
    response_model=HierarchyMetaOut,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.HIERARCHY_CREATE]))],
)
@hide_endpoint("HIDE_CREATE_HIERARCHY")
async def create_hierarchy(
    request: Request,
    service: Annotated[HierarchyService, Depends(get_hierarchy_service)],
    hierarchy: HierarchyCreateRequest = Body(alias="hierarchy"),
    model_name: str = Path(alias="modelName"),
    dimension_name: str = Path(alias="dimensionName"),
    tenant_id: str = Path(alias="tenantName"),
) -> HierarchyMetaOut:
    """
    Создаёт новую иерархию.

    Args:
        request (Request): HTTP-запрос от клиента.
        service (HierarchyService): Сервис для работы с иерархиями.
        hierarchy (HierarchyCreateRequest): Данные для создания новой иерархии.
        model_name (str): Имя модели, к которой относится иерархия.
        dimension_name (str): Имя измерения, к которому относится иерархия.
        tenant_id (str): Идентификатор арендатора (тенанта).

    Returns:
        HierarchyMetaOut: Созданная иерархия.
    """
    audit_kwargs = {
        "audit_type": audit_types.C4,
        "status": StatusType.FAIL,
        "object_id": request.url.path + hierarchy.name,
        "object_properties": hierarchy.model_fields_set,
        "message": hierarchy.model_dump(by_alias=True, mode="json", exclude_unset=True),
        "object_name": HierarchyCreateRequest.__qualname__,
        "extra": {"include_fields": ["object_id", "object_name"]},
    }
    try:
        result = await service.create_hierarchy_by_schema(
            tenant_id=tenant_id,
            model_name=model_name,
            dimension_name=dimension_name,
            hierarchy=hierarchy,
        )
        audit_kwargs["audit_type"] = audit_types.C3
        audit_kwargs["status"] = StatusType.SUCCESS
        logger.audit(**audit_kwargs)
    except IntegrityError as ex:
        reason = "BAD_REQUEST: " + str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        await service.hierarchy_repo.session.rollback()
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs, status_code=HTTPStatus.BAD_REQUEST, detail=reason
        )
    except SQLAlchemyError as ex:
        reason = "INTERNAL_SERVER_ERROR: " + str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        await service.hierarchy_repo.session.rollback()
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs, status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=reason
        )
    except Exception as ex:  # noqa: PIE786
        reason = "UNKNOWN ERROR: " + str(ex)
        logger.exception(reason)
        audit_kwargs["reason"] = reason
        await service.hierarchy_repo.session.rollback()
        raise HTTPExceptionWithAuditLogging(
            audit_kwargs=audit_kwargs, status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=reason
        )
    return result


@router.post(
    MODEL_NAME_URL + DIMENSION_HIERARCHY_URL + "/{hierarchyName}/pvd",
    status_code=HTTPStatus.CREATED,
    description="Создать иерархию в PVD.",
    response_description="Иерархия с заполненным pvDictionary.",
    response_model=HierarchyMetaOut,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.HIERARCHY_CREATE]))],
)
@hide_endpoint("HIDE_CREATE_HIERARCHY_IN_PVD")
async def create_hierarchy_in_pvd(
    request: Request,
    service: Annotated[HierarchyPvdService, Depends(get_hierarchy_pvd_service)],
    model_name: str = Path(alias="modelName"),
    dimension_name: str = Path(alias="dimensionName"),
    hierarchy_name: str = Path(alias="hierarchyName"),
    tenant_id: str = Path(alias="tenantName"),
    pv_request: Optional[HierarchyPvdCreateRequest] = Body(default=None),
) -> HierarchyMetaOut:
    """
    Создать иерархию в PVD.

    Находит существующую иерархию в Семантическом слое, формирует запрос к PVD
    на создание иерархии и сохраняет результат (pvDictionary) в базу данных.

    Args:
        request: HTTP-запрос.
        service: Сервис для работы с иерархиями.
        model_name: Имя модели.
        dimension_name: Имя измерения.
        hierarchy_name: Имя иерархии.
        tenant_id: Идентификатор тенанта.
        pv_request: Тело запроса с параметрами PVD (необязательно).

    Returns:
        HierarchyMetaOut: Иерархия с заполненным pvDictionary.
    """
    audit_kwargs = {
        "audit_type": audit_types.C4,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": HierarchyCreateRequest.__qualname__,
    }
    try:
        result = await service.create_hierarchy_in_pvd(
            tenant_id=tenant_id,
            model_name=model_name,
            dimension_name=dimension_name,
            hierarchy_name=hierarchy_name,
            pv_request=pv_request,
        )
        audit_kwargs["audit_type"] = audit_types.C3
        audit_kwargs["status"] = StatusType.SUCCESS
        logger.audit(**audit_kwargs)
    except Exception as ex:  # noqa: PIE786
        __handle_errors(audit_kwargs, ex)
    return result


@router.patch(
    MODEL_NAME_URL + DIMENSION_HIERARCHY_URL + "/{hierarchyName}/pvd",
    status_code=HTTPStatus.OK,
    description="Обновить иерархию в PVD.",
    response_description="Обновлённая иерархия с pvDictionary.",
    response_model=HierarchyMetaOut,
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.HIERARCHY_EDIT]))],
)
@hide_endpoint("HIDE_UPDATE_HIERARCHY_IN_PVD")
async def update_hierarchy_in_pvd(
    request: Request,
    service: Annotated[HierarchyPvdService, Depends(get_hierarchy_pvd_service)],
    model_name: str = Path(alias="modelName"),
    dimension_name: str = Path(alias="dimensionName"),
    hierarchy_name: str = Path(alias="hierarchyName"),
    tenant_id: str = Path(alias="tenantName"),
    pv_request: Optional[HierarchyPvdCreateRequest] = Body(default=None),
) -> HierarchyMetaOut:
    """
    Обновить иерархию в PVD.

    Обновляет поля иерархии в PVD (labels, isVersioned,
    isTimeDependent, timeDependencyType, dictionaryNameList).

    Args:
        request: HTTP-запрос.
        service: Сервис для работы с иерархиями.
        model_name: Имя модели.
        dimension_name: Имя измерения.
        hierarchy_name: Имя иерархии.
        tenant_id: Идентификатор тенанта.
        pv_request: Тело запроса с параметрами PVD (необязательно).

    Returns:
        HierarchyMetaOut: Обновлённая иерархия.
    """
    audit_kwargs = {
        "audit_type": audit_types.C4,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": HierarchyCreateRequest.__qualname__,
    }
    try:
        result = await service.update_hierarchy_in_pvd(
            tenant_id=tenant_id,
            model_name=model_name,
            dimension_name=dimension_name,
            hierarchy_name=hierarchy_name,
            pv_request=pv_request,
        )
        audit_kwargs["audit_type"] = audit_types.C3
        audit_kwargs["status"] = StatusType.SUCCESS
        logger.audit(**audit_kwargs)
    except Exception as ex:  # noqa: PIE786
        __handle_errors(audit_kwargs, ex)
    return result


@router.delete(
    MODEL_NAME_URL + DIMENSION_HIERARCHY_URL + "/{hierarchyName}/pvd",
    status_code=HTTPStatus.NO_CONTENT,
    description="Удалить иерархию из PVD.",
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.HIERARCHY_DELETE]))],
)
@hide_endpoint("HIDE_DELETE_HIERARCHY_IN_PVD")
async def delete_hierarchy_from_pvd(
    request: Request,
    service: Annotated[HierarchyPvdService, Depends(get_hierarchy_pvd_service)],
    model_name: str = Path(alias="modelName"),
    dimension_name: str = Path(alias="dimensionName"),
    hierarchy_name: str = Path(alias="hierarchyName"),
    tenant_id: str = Path(alias="tenantName"),
) -> None:
    """
    Удалить иерархию из PVD.

    Удаляет иерархию из PVD и очищает привязку pvDictionary в Семантическом слое.
    Сама иерархия в Семантическом слое не удаляется.

    Args:
        request: HTTP-запрос.
        service: Сервис для работы с иерархиями.
        model_name: Имя модели.
        dimension_name: Имя измерения.
        hierarchy_name: Имя иерархии.
        tenant_id: Идентификатор тенанта.
    """
    audit_kwargs = {
        "audit_type": audit_types.C4,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": HierarchyCreateRequest.__qualname__,
    }
    try:
        await service.delete_hierarchy_from_pvd(
            tenant_id=tenant_id,
            model_name=model_name,
            dimension_name=dimension_name,
            hierarchy_name=hierarchy_name,
        )
        audit_kwargs["audit_type"] = audit_types.C3
        audit_kwargs["status"] = StatusType.SUCCESS
        logger.audit(**audit_kwargs)
    except Exception as ex:  # noqa: PIE786
        __handle_errors(audit_kwargs, ex)


@router.post(
    "/hierarchies/copyToAnotherModel",
    status_code=HTTPStatus.OK,
    description="Получить иерархию по имени.",
    response_description="Иерархия.",
    response_model=list[HierarchyCopyResponse],
    dependencies=[Depends(PermissionChecker(required_permissions=[PermissionEnum.HIERARCHY_EDIT]))],
)
@hide_endpoint("HIDE_COPY_HIERARCHIES_TO_ANOTHER_MODEL")
async def copy_hierarchies_to_another_model(
    request: Request,
    service: Annotated[HierarchyService, Depends(get_hierarchy_service)],
    names_of_models: list[str] = Body(alias="modelNames", validation_alias="modelNames"),
    hierarchy_names: list[str] = Body(alias="objects", validation_alias="objects"),
    tenant_id: str = Path(alias="tenantName"),
) -> list[HierarchyCopyResponse]:
    """
    Копирует указанные иерархии в одну или несколько целевых моделей.

    Args:
        request (Request): HTTP-запрос от клиента.
        service (HierarchyService): Сервис для работы с иерархиями.
        names_of_models (list[str]): Список имён моделей, в которые будут скопированы иерархии.
        hierarchy_names (list[str]): Список имён иерархий, подлежащих копированию.
        tenant_id (str): Идентификатор арендатора (тенанта).

    Returns:
        list[HierarchyCopyResponse]: Список результатов копирования для каждой целевой модели,
        содержащий информацию об успешных и неудавшихся операциях копирования.
    """
    audit_kwargs = {
        "audit_type": audit_types.C2,
        "status": StatusType.FAIL,
        "object_id": request.url.path,
        "object_name": HierarchyCreateRequest.__qualname__,
    }
    try:
        result = await service.copy_hierarchies_to_another_model(
            tenant_id=tenant_id,
            hierarchy_names=hierarchy_names,
            names_of_models=names_of_models,
        )
        audit_kwargs["audit_type"] = audit_types.C1
        audit_kwargs["status"] = StatusType.SUCCESS
        logger.audit(**audit_kwargs)
    except Exception as ex:  # noqa: PIE786
        __handle_errors(audit_kwargs, ex)
    return result
