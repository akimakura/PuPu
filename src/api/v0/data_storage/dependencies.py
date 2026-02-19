"""
Определение зависимостей из сервисного слоя.
"""

from typing import Annotated

from fastapi import Depends

from src.api.dependencies import (
    get_aor_repository,
    get_database_object_relations_repository,
    get_database_object_repository,
    get_datastorage_repository,
    get_dimension_repository,
    get_model_relations_repository,
    get_model_repository,
)
from src.integration.aor import ClientAOR, get_aor_client
from src.integration.worker_manager import ClientWorkerManager, get_worker_manager_client
from src.repository.aor import AorRepository
from src.repository.data_storage import DataStorageRepository
from src.repository.database_object import DatabaseObjectRepository
from src.repository.database_object_relations import DatabaseObjectRelationsRepository
from src.repository.dimension import DimensionRepository
from src.repository.model import ModelRepository
from src.repository.model_relations import ModelRelationsRepository
from src.service.data_storage import DataStorageService


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
