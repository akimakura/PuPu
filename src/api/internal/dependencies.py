"""
Определение зависимостей из сервисного слоя.
"""

from src.service.permissions import PermissionsService


def get_permissions_service() -> PermissionsService:
    return PermissionsService()
