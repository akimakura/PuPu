"""
Сервис баз данных.
Тут реализована вся логика работы сервиса.
Работа с БД инкапсулирована через DataRepository.
"""

from typing import Optional

from src.models.request_params import Pagination
from src.models.tenant import SemanticObjectsTypeEnum, Tenant, TenantCreateRequest, TenantEditRequest
from src.repository.cache import CacheRepository
from src.repository.tenant import SemanticObjects, TenantRepository
from src.service.utils import get_updated_fields_object
from src.utils.backoff import RetryConfig, retry


class TenantService:
    def __init__(self, data_repository: TenantRepository) -> None:
        self.data_repository: TenantRepository = data_repository

    @retry(RetryConfig())
    async def get_tenant_by_name(self, name: str) -> Tenant:
        """Получить Tenant по имени."""
        result = await self.data_repository.get_by_name(name=name)
        return result

    @retry(RetryConfig())
    async def get_tenant_list(self, pagination: Optional[Pagination] = None) -> list[Tenant]:
        """Получить список всех баз данных."""
        result = await self.data_repository.get_list(pagination=pagination)
        return result

    async def delete_tenant_by_name(self, name: str) -> None:
        """Удалить Tenant."""
        await self.data_repository.delete_by_name(name=name)
        await CacheRepository.clear_tenants_cache()
        await CacheRepository.clear_tenant_cache_by_name(name=name)
        return None

    async def create_tenant_by_schema(self, tenant: TenantCreateRequest) -> Tenant:
        """Создать Tenant."""
        result = await self.data_repository.create_by_schema(tenant=tenant)
        await CacheRepository.clear_tenants_cache()
        return result

    async def update_tenant_by_name_and_schema(self, name: str, tenant: TenantEditRequest) -> Tenant:
        """Обновить Tenant."""
        result = await self.data_repository.update_by_name_and_schema(name=name, tenant=tenant)
        await CacheRepository.clear_tenants_cache()
        await CacheRepository.clear_tenant_cache_by_name(name=name)
        return result

    async def get_updated_fields(self, name: str, tenant: TenantEditRequest) -> dict:
        """Получить поля, которые были изменены"""
        original_database = await self.get_tenant_by_name(name=name)
        original_database = original_database.model_dump(mode="json", by_alias=True)
        tenant_dict = tenant.model_dump(mode="json", by_alias=True, exclude_unset=True)
        return get_updated_fields_object(original_database, tenant_dict)

    async def search_elements(
        self,
        search: str,
        tenant_name: Optional[str],
        model_name: Optional[str],
        element_type: Optional[SemanticObjectsTypeEnum],
    ) -> dict[str, SemanticObjects]:
        """
        Args:
            search (str): Поисковая фраза.
            tenant_name (str): Имя тенанта.
            model_name (Optional[str]): Название модели.
            element_type (TypeEnum): Тип элемента.

        Returns:
            dict[str, SemanticObjects]: Словарь, где ключи - названия моделей, а значения - списки элементов.
        """
        elements = await self.data_repository.get_semantic_objects_by_tenant_and_search_string(
            tenant_name=tenant_name,
            search=search,
            model_name=model_name,
            element_type=element_type,
        )
        return elements

    def __repr__(self) -> str:
        return "DatabaseService"
