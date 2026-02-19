"""
Определение зависимостей из сервисного слоя.
"""

from typing import Annotated

from fastapi import Depends

from src.api.dependencies import get_database_repository
from src.integration.aor import ClientAOR, get_aor_client
from src.repository.database import DatabaseRepository
from src.service.database import DatabaseService


async def get_database_service(
    database_repository: Annotated[DatabaseRepository, Depends(get_database_repository)],
    aor_client: Annotated[ClientAOR, Depends(get_aor_client)],
) -> DatabaseService:
    return DatabaseService(database_repository, aor_client)
