"""
Определение зависимостей из сервисного слоя.
"""

from typing import Annotated

from fastapi import Depends

from src.api.dependencies import get_aor_repository, get_hierarchy_history_repo, get_hierarchy_repository
from src.api.v0.data_storage.dependencies import get_dso_service
from src.api.v0.database.dependencies import get_database_service
from src.api.v0.dimension.dependencies import get_dimension_service
from src.api.v0.model.dependencies import get_model_service
from src.integration.aor import ClientAOR, get_aor_client
from src.repository.aor import AorRepository
from src.repository.hierarchy import HierarchyRepository
from src.repository.history.hierarchy import HierarchyHistoryRepository
from src.service.data_storage import DataStorageService
from src.service.database import DatabaseService
from src.service.dimension import DimensionService
from src.service.hierarchy import HierarchyService
from src.service.model import ModelService
from src.service.pv_hierarchy import HierarchyPvdService


async def get_hierarchy_pvd_service(
    hierarchy_repository: Annotated[HierarchyRepository, Depends(get_hierarchy_repository)],
    dimension_service: Annotated[DimensionService, Depends(get_dimension_service)],
) -> HierarchyPvdService:
    """
    Создаёт и возвращает экземпляр сервиса управления иерархиями в PVD.

    Args:
        hierarchy_repository: Репозиторий для операций с иерархиями.
        dimension_service: Сервис для работы с измерениями.

    Returns:
        HierarchyPvdService: Инстанс сервиса управления иерархиями в PVD.
    """
    return HierarchyPvdService(
        hierarchy_repo=hierarchy_repository,
        dimension_service=dimension_service,
    )


async def get_hierarchy_service(
    hierarchy_repository: Annotated[HierarchyRepository, Depends(get_hierarchy_repository)],
    dimension_service: Annotated[DimensionService, Depends(get_dimension_service)],
    pvd_service: Annotated[HierarchyPvdService, Depends(get_hierarchy_pvd_service)],
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
        pvd_service=pvd_service,
    )
