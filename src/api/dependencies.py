"""
Определение зависимостей из сервисного слоя.
"""

from http import HTTPStatus
from typing import Annotated, AsyncGenerator, Optional

from fastapi import Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.engine import database_connector
from src.models.request_params import Pagination, SortDirectionEnum
from src.repository.aor import AorRepository
from src.repository.composite import CompositeRepository
from src.repository.data_storage import DataStorageRepository
from src.repository.database import DatabaseRepository
from src.repository.database_object import DatabaseObjectRepository
from src.repository.database_object_relations import DatabaseObjectRelationsRepository
from src.repository.dimension import DimensionRepository
from src.repository.hierarchy import HierarchyRepository
from src.repository.history.hierarchy import HierarchyHistoryRepository
from src.repository.measure import MeasureRepository
from src.repository.meta_synchronizer import MetaSynchronizerRepository
from src.repository.model import ModelRepository
from src.repository.model_relations import ModelRelationsRepository
from src.repository.tenant import TenantRepository
from src.service.relations import RelationsService


def get_pagination_params(
    page: Optional[int] = Query(default=None, ge=1, le=100),
    page_size: Optional[int] = Query(default=None, ge=1, le=100, alias="pageSize"),
    sort_direction: SortDirectionEnum = Query(default=SortDirectionEnum.asc, alias="sortDirection"),
) -> Pagination:
    """Получить параметры пагинации."""
    if (page is None) ^ (page_size is None):
        raise HTTPException(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            detail="One of pagination parameters 'page' or 'pageSize' is empty.",
        )
    offset = (page - 1) * page_size if page is not None and page_size is not None else None
    return Pagination(offset=offset, limit=page_size, sort_direction=sort_direction)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    _, async_session_maker = await database_connector.get_not_pg_is_in_recovery()
    async with async_session_maker() as session:
        try:
            yield session
        finally:
            await session.close()


async def get_aor_repository(session: Annotated[AsyncSession, Depends(get_session)]) -> AorRepository:
    return AorRepository(session)


async def get_tenant_repository(session: Annotated[AsyncSession, Depends(get_session)]) -> TenantRepository:
    return TenantRepository(session)


async def get_database_repository(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> DatabaseRepository:
    return DatabaseRepository(session)


async def get_model_repository(
    session: Annotated[AsyncSession, Depends(get_session)],
    database_repository: Annotated[DatabaseRepository, Depends(get_database_repository)],
) -> ModelRepository:
    return ModelRepository(session, database_repository)


async def get_measure_repository(
    session: Annotated[AsyncSession, Depends(get_session)],
    model_repository: Annotated[ModelRepository, Depends(get_model_repository)],
) -> MeasureRepository:
    return MeasureRepository(session, model_repository)


async def get_database_object_repository(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> DatabaseObjectRepository:
    return DatabaseObjectRepository(session)


async def get_database_object_relations_repository(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> DatabaseObjectRelationsRepository:
    return DatabaseObjectRelationsRepository(session)


async def get_datastorage_repository(
    session: Annotated[AsyncSession, Depends(get_session)],
    model_repository: Annotated[ModelRepository, Depends(get_model_repository)],
    database_object_repository: Annotated[DatabaseObjectRepository, Depends(get_database_object_repository)],
) -> DataStorageRepository:
    return DataStorageRepository(session, model_repository, database_object_repository)


async def get_composite_repository(
    session: Annotated[AsyncSession, Depends(get_session)],
    model_repository: Annotated[ModelRepository, Depends(get_model_repository)],
    database_object_repository: Annotated[DatabaseObjectRepository, Depends(get_database_object_repository)],
    datastorage_repository: Annotated[DataStorageRepository, Depends(get_datastorage_repository)],
) -> CompositeRepository:
    return CompositeRepository(session, model_repository, datastorage_repository, database_object_repository)


async def get_model_relations_repository(
    session: Annotated[AsyncSession, Depends(get_session)],
    model_repository: Annotated[ModelRepository, Depends(get_model_repository)],
    datastorage_repository: Annotated[DataStorageRepository, Depends(get_datastorage_repository)],
    composite_repository: Annotated[CompositeRepository, Depends(get_composite_repository)],
    measure_repository: Annotated[MeasureRepository, Depends(get_measure_repository)],
) -> ModelRelationsRepository:
    """
    Получить репозиторий для работы со связями между объектами модели.
    Args:
        session (Annotated[AsyncSession, Depends(get_session)]): Сессия для работы с БД.
        model_repository (Annotated[ModelRepository, Depends(get_model_repository)]): Репозиторий для работы с моделями.
        datastorage_repository (Annotated[DataStorageRepository, Depends(get_datastorage_repository)]):
        composite_repository (Annotated[CompositeRepository, Depends(get_composite_repository)]):
        measure_repository (Annotated[MeasureRepository, Depends(get_measure_repository)]):
    Returns:
        ModelRelationsRepository: Репозиторий для работы со связями между объектами модели.
    """
    return ModelRelationsRepository(
        session,
        model_repository=model_repository,
        datastorage_repository=datastorage_repository,
        composite_repository=composite_repository,
        measure_repository=measure_repository,
    )


async def get_hierarchy_repository(session: Annotated[AsyncSession, Depends(get_session)]) -> HierarchyRepository:
    return HierarchyRepository(session)


async def get_meta_synchronizer_repository(
    session: Annotated[AsyncSession, Depends(get_session)],
    model_repository: Annotated[ModelRepository, Depends(get_model_repository)],
    datastorage_repository: Annotated[DataStorageRepository, Depends(get_datastorage_repository)],
    composite_repository: Annotated[CompositeRepository, Depends(get_composite_repository)],
) -> MetaSynchronizerRepository:
    return MetaSynchronizerRepository(session, model_repository, composite_repository, datastorage_repository)


async def get_dimension_repository(
    session: Annotated[AsyncSession, Depends(get_session)],
    model_repository: Annotated[ModelRepository, Depends(get_model_repository)],
    database_object_repository: Annotated[DatabaseObjectRepository, Depends(get_database_object_repository)],
    datastorage_repository: Annotated[DataStorageRepository, Depends(get_datastorage_repository)],
    model_relations_reposiotory: Annotated[ModelRelationsRepository, Depends(get_model_relations_repository)],
    measure_repository: Annotated[MeasureRepository, Depends(get_measure_repository)],
) -> DimensionRepository:
    return DimensionRepository(
        session,
        model_repository,
        datastorage_repository,
        database_object_repository,
        model_relations_reposiotory,
        measure_repository,
    )


async def get_relations_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    model_relations_repository: Annotated[ModelRelationsRepository, Depends(get_model_relations_repository)],
) -> RelationsService:
    """
    Получить сервис для работы с связями между объектами модели.
    Args:
        session (Annotated[AsyncSession, Depends(get_session)]): Сессия для работы с БД.
        model_relations_repository (Annotated[ModelRelationsRepository, Depends(get_model_relations_repository)]):
    Returns:
        RelationsService: Сервис для работы с связями между объектами модели.
    """
    return RelationsService(session, model_relations_repository)


async def get_hierarchy_history_repo(
    session: Annotated[AsyncSession, Depends(get_session)],
    hierarchy_repository: Annotated[HierarchyRepository, Depends(get_hierarchy_repository)],
) -> HierarchyHistoryRepository:
    return HierarchyHistoryRepository(session, hierarchy_repository)
