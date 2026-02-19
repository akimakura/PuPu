"""
Определение зависимостей из сервисного слоя.
"""

from typing import Annotated

from fastapi import Depends

from src.api.dependencies import get_tenant_repository
from src.repository.tenant import TenantRepository
from src.service.tenant import TenantService


async def get_tenant_service(
    tenant_repository: Annotated[TenantRepository, Depends(get_tenant_repository)]
) -> TenantService:
    return TenantService(tenant_repository)
