"""
Сервис для работы с пермишенами.
"""

from fastapi.security import HTTPAuthorizationCredentials
from py_common_lib.permissions.permissions_checker import get_permissions_by_token

from src.models.permissions import PermissionEnum
from src.utils.backoff import RetryConfig, retry


class PermissionsService:

    @retry(RetryConfig())
    async def get_permissions(self, token: HTTPAuthorizationCredentials) -> list[PermissionEnum]:
        """Получить пермишены семантического слоя из auth-proxy"""
        permissions = await get_permissions_by_token(token)
        semantic_permissions = [permission for permission in permissions if permission.startswith("Semantic.")]
        return semantic_permissions
