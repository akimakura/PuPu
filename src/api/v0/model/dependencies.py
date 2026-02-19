"""
Определение зависимостей из сервисного слоя.
"""

from typing import Annotated

from fastapi import Depends

from src.api.dependencies import get_aor_repository, get_meta_synchronizer_repository, get_model_repository
from src.integration.aor import ClientAOR, get_aor_client
from src.integration.worker_manager import ClientWorkerManager, get_worker_manager_client
from src.repository.aor import AorRepository
from src.repository.meta_synchronizer import MetaSynchronizerRepository
from src.repository.model import ModelRepository
from src.service.meta_synchronizer import MetaSynchronizerService
from src.service.model import ModelService


async def get_model_service(
    model_repository: Annotated[ModelRepository, Depends(get_model_repository)],
    meta_sync_repository: Annotated[MetaSynchronizerRepository, Depends(get_meta_synchronizer_repository)],
    aor_client: Annotated[ClientAOR, Depends(get_aor_client)],
    aor_repository: Annotated[AorRepository, Depends(get_aor_repository)],
) -> ModelService:
    return ModelService(model_repository, meta_sync_repository, aor_client, aor_repository)


async def get_meta_sync_service(
    meta_sync_repository: Annotated[MetaSynchronizerRepository, Depends(get_meta_synchronizer_repository)],
    client_worker_manager: Annotated[ClientWorkerManager, Depends(get_worker_manager_client)],
    aor_client: Annotated[ClientAOR, Depends(get_aor_client)],
) -> MetaSynchronizerService:
    return MetaSynchronizerService(meta_sync_repository, client_worker_manager, aor_client)
