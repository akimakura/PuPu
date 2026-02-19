"""
Определение зависимостей из сервисного слоя.
"""

from typing import Annotated

from fastapi import Depends

from src.api.dependencies import (
    get_aor_repository,
    get_composite_repository,
    get_database_object_relations_repository,
    get_database_object_repository,
    get_database_repository,
    get_datastorage_repository,
    get_dimension_repository,
    get_hierarchy_history_repo,
    get_hierarchy_repository,
    get_measure_repository,
    get_meta_synchronizer_repository,
    get_model_relations_repository,
    get_model_repository,
)
from src.integration.aor import ClientAOR, get_aor_client
from src.integration.worker_manager import ClientWorkerManager, get_worker_manager_client
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
from src.service.aor import AorService
from src.service.composite import CompositeService
from src.service.data_storage import DataStorageService
from src.service.database import DatabaseService
from src.service.dimension import DimensionService
from src.service.hierarchy import HierarchyService
from src.service.measure import MeasureService
from src.service.model import ModelService


async def get_dso_service(
    datastorage_repository: Annotated[DataStorageRepository, Depends(get_datastorage_repository)],
    dimension_repository: Annotated[DimensionRepository, Depends(get_dimension_repository)],
    model_relations_repository: Annotated[ModelRelationsRepository, Depends(get_model_relations_repository)],
    model_repository: Annotated[ModelRepository, Depends(get_model_repository)],
    database_object_repository: Annotated[DatabaseObjectRepository, Depends(get_database_object_repository)],
    database_object_relations_repository: Annotated[
        DatabaseObjectRelationsRepository, Depends(get_database_object_relations_repository)
    ],
    worker_manager_client: Annotated[ClientWorkerManager, Depends(get_worker_manager_client)],
    aor_client: Annotated[ClientAOR, Depends(get_aor_client)],
    aor_repository: Annotated[AorRepository, Depends(get_aor_repository)],
) -> DataStorageService:
    return DataStorageService(
        datastorage_repository,
        dimension_repository,
        model_relations_repository,
        model_repository,
        database_object_repository,
        database_object_relations_repository,
        worker_manager_client,
        aor_client=aor_client,
        aor_repository=aor_repository,
    )


async def get_composite_service(
    composite_repository: Annotated[CompositeRepository, Depends(get_composite_repository)],
    model_relations_repo: Annotated[ModelRelationsRepository, Depends(get_model_relations_repository)],
    worker_manager_client: Annotated[ClientWorkerManager, Depends(get_worker_manager_client)],
    aor_client: Annotated[ClientAOR, Depends(get_aor_client)],
    aor_repository: Annotated[AorRepository, Depends(get_aor_repository)],
) -> CompositeService:
    return CompositeService(
        composite_repository,
        model_relations_repo,
        worker_manager_client,
        aor_client=aor_client,
        aor_repository=aor_repository,
    )


async def get_dimension_service(
    dimension_repository: Annotated[DimensionRepository, Depends(get_dimension_repository)],
    model_relations_repo: Annotated[ModelRelationsRepository, Depends(get_model_relations_repository)],
    client_worker_manager: Annotated[ClientWorkerManager, Depends(get_worker_manager_client)],
    aor_client: Annotated[ClientAOR, Depends(get_aor_client)],
    aor_repository: Annotated[AorRepository, Depends(get_aor_repository)],
) -> DimensionService:
    return DimensionService(
        dimension_repository,
        model_relations_repo,
        client_worker_manager,
        aor_client,
        aor_repository=aor_repository,
    )


async def get_measure_service(
    measure_repository: Annotated[MeasureRepository, Depends(get_measure_repository)],
    model_relations_repository: Annotated[ModelRelationsRepository, Depends(get_model_relations_repository)],
    aor_client: Annotated[ClientAOR, Depends(get_aor_client)],
    aor_repository: Annotated[AorRepository, Depends(get_aor_repository)],
) -> MeasureService:
    return MeasureService(measure_repository, model_relations_repository, aor_client, aor_repository)


async def get_model_service(
    model_repository: Annotated[ModelRepository, Depends(get_model_repository)],
    meta_sync_repository: Annotated[MetaSynchronizerRepository, Depends(get_meta_synchronizer_repository)],
    aor_client: Annotated[ClientAOR, Depends(get_aor_client)],
    aor_repository: Annotated[AorRepository, Depends(get_aor_repository)],
) -> ModelService:
    return ModelService(model_repository, meta_sync_repository, aor_client, aor_repository)


async def get_database_service(
    database_repository: Annotated[DatabaseRepository, Depends(get_database_repository)],
    aor_client: Annotated[ClientAOR, Depends(get_aor_client)],
) -> DatabaseService:
    return DatabaseService(database_repository, aor_client)


async def get_hierarchy_service(
    hierarchy_repository: Annotated[HierarchyRepository, Depends(get_hierarchy_repository)],
    dimension_service: Annotated[DimensionService, Depends(get_dimension_service)],
    data_storage_service: Annotated[DataStorageService, Depends(get_dso_service)],
    database_service: Annotated[DatabaseService, Depends(get_database_service)],
    model_service: Annotated[ModelService, Depends(get_model_service)],
    aor_client: Annotated[ClientAOR, Depends(get_aor_client)],
    hierarchy_history_repo: Annotated[HierarchyHistoryRepository, Depends(get_hierarchy_history_repo)],
    aor_repository: Annotated[AorRepository, Depends(get_aor_repository)],
) -> HierarchyService:
    """
    Создаёт и возвращает экземпляр службы управления иерархиями (HierarchyService).

    Эта зависимость автоматически собирает все необходимые сервисы и репозитории,
    используемые службой иерархий, и инициализирует службу с этими зависимостями.

    Args:
        hierarchy_repository (HierarchyRepository): Репозиторий для операций с иерархиями.
        dimension_service (DimensionService): Сервис для работы с измерениями.
        data_storage_service (DataStorageService): Сервис для работы с хранилищами данных.
        database_service (DatabaseService): Сервис для работы с базой данных.
        model_service (ModelService): Сервис для работы с моделями.
        aor_client (ClientAOR): Клиент для взаимодействия с АОР.
        hierarchy_history_repo (HierarchyHistoryRepository): Репозиторий для истории иерархий.

    Returns:
        HierarchyService: Инстанс службы управления иерархиями.
    """
    return HierarchyService(
        hierarchy_repo=hierarchy_repository,
        dimension_service=dimension_service,
        data_storage_service=data_storage_service,
        database_service=database_service,
        model_service=model_service,
        aor_client=aor_client,
        hierarchy_history_repo=hierarchy_history_repo,
        aor_repository=aor_repository,
    )


async def get_aor_service(
    database_service: Annotated[DatabaseService, Depends(get_database_service)],
    model_service: Annotated[ModelService, Depends(get_model_service)],
    measure_service: Annotated[MeasureService, Depends(get_measure_service)],
    dimension_service: Annotated[DimensionService, Depends(get_dimension_service)],
    datastorage_service: Annotated[DataStorageService, Depends(get_dso_service)],
    composite_service: Annotated[CompositeService, Depends(get_composite_service)],
    hierarchy_service: Annotated[HierarchyService, Depends(get_hierarchy_service)],
) -> AorService:
    return AorService(
        database_service=database_service,
        model_service=model_service,
        measure_service=measure_service,
        dimension_service=dimension_service,
        datastorage_service=datastorage_service,
        composite_service=composite_service,
        hierarchy_service=hierarchy_service,
    )
