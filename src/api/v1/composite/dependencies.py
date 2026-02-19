"""
Определение зависимостей из сервисного слоя.
"""

from typing import Annotated

from fastapi import Depends

from src.api.dependencies import get_aor_repository, get_composite_repository, get_model_relations_repository
from src.integration.aor import ClientAOR, get_aor_client
from src.integration.worker_manager import ClientWorkerManager, get_worker_manager_client
from src.repository.aor import AorRepository
from src.repository.composite import CompositeRepository
from src.repository.model_relations import ModelRelationsRepository
from src.service.composite import CompositeService


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
