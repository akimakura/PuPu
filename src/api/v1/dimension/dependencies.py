"""
Определение зависимостей из сервисного слоя.
"""

from typing import Annotated

from fastapi import Depends

from src.api.dependencies import (
    get_aor_repository,
    get_dimension_repository,
    get_meta_synchronizer_repository,
    get_model_relations_repository,
)
from src.integration.aor import ClientAOR, get_aor_client
from src.integration.worker_manager import ClientWorkerManager, get_worker_manager_client
from src.repository.aor import AorRepository
from src.repository.dimension import DimensionRepository
from src.repository.meta_synchronizer import MetaSynchronizerRepository
from src.repository.model_relations import ModelRelationsRepository
from src.service.dimension import DimensionService
from src.service.meta_synchronizer import MetaSynchronizerService


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
        aor_client=aor_client,
        aor_repository=aor_repository,
    )


async def get_meta_sync_service(
    meta_sync_repository: Annotated[MetaSynchronizerRepository, Depends(get_meta_synchronizer_repository)],
    client_worker_manager: Annotated[ClientWorkerManager, Depends(get_worker_manager_client)],
    aor_client: Annotated[ClientAOR, Depends(get_aor_client)],
) -> MetaSynchronizerService:
    return MetaSynchronizerService(
        meta_sync_repository,
        client_worker_manager,
        aor_client=aor_client,
    )
