"""
Определение зависимостей из сервисного слоя.
"""

from typing import Annotated

from fastapi import Depends

from src.api.dependencies import get_aor_repository, get_measure_repository, get_model_relations_repository
from src.integration.aor import ClientAOR, get_aor_client
from src.repository.aor import AorRepository
from src.repository.measure import MeasureRepository
from src.repository.model_relations import ModelRelationsRepository
from src.service.measure import MeasureService


async def get_measure_service(
    measure_repository: Annotated[MeasureRepository, Depends(get_measure_repository)],
    model_relations_repository: Annotated[ModelRelationsRepository, Depends(get_model_relations_repository)],
    aor_client: Annotated[ClientAOR, Depends(get_aor_client)],
    aor_repository: Annotated[AorRepository, Depends(get_aor_repository)],
) -> MeasureService:
    return MeasureService(
        measure_repository,
        model_relations_repository,
        aor_client=aor_client,
        aor_repository=aor_repository,
    )
