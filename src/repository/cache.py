from enum import StrEnum
from typing import Optional

from src.cache import FastAPICache

KEY_PREFIX = ":*:*"
MODELS_PREFIX = "/models"
TENANTS_PREFIX = "/tenants"


class CacheNamespaceEnum(StrEnum):
    COMPOSITE = "composite"
    DATASTORAGE = "datastorage"
    DATABASE = "database"
    DIMENSION = "dimension"
    HIERARCHY = "hierarchy"
    MEASURE = "measure"
    MODEL = "model"
    TENANT = "tenant"


class CacheRepository:  # noqa
    """Репозиторий для работы с кэшом."""

    @classmethod
    async def clear_composites_cache_by_model_name(cls, tenant_id: str, model_name: str) -> None:
        """Очистить кэш эндпоинтов, которые выдают список композитов."""
        namespace = (
            CacheNamespaceEnum.COMPOSITE
            + f"{KEY_PREFIX}{TENANTS_PREFIX}/{tenant_id}{MODELS_PREFIX}/{model_name}/composites/"
        )
        await FastAPICache.clear(namespace=namespace)

    @classmethod
    async def clear_composite_cache_by_name(cls, tenant_id: str, name: Optional[str]) -> None:
        """Очистить кэш эндпоинтов, которые выдают конкретный композит с именем name."""
        namespace = (
            CacheNamespaceEnum.COMPOSITE
            + f"{KEY_PREFIX}{TENANTS_PREFIX}/{tenant_id}{MODELS_PREFIX}/*/composites/{name}"
        )
        await FastAPICache.clear(namespace=namespace)

    @classmethod
    async def clear_data_storages_cache_by_model_name(cls, tenant_id: str, model_name: str) -> None:
        """Очистить кэш эндпоинтов, которые выдают список DSO."""
        all_namespace = (
            CacheNamespaceEnum.DATASTORAGE
            + f"{KEY_PREFIX}{TENANTS_PREFIX}/{tenant_id}{MODELS_PREFIX}/{model_name}/dataStorages/"
        )
        db_object_namespace = (
            CacheNamespaceEnum.DATASTORAGE
            + f"{KEY_PREFIX}{TENANTS_PREFIX}/{tenant_id}{MODELS_PREFIX}/{model_name}/dataStorages/byDbObject"
        )
        await FastAPICache.clear(namespace=all_namespace)
        await FastAPICache.clear(namespace=db_object_namespace)

    @classmethod
    async def clear_data_storages_cache_by_name(cls, tenant_id: str, name: str) -> None:
        """Очистить кэш эндпоинтов, которые выдают конкретный DSO с именем name."""
        namespace = (
            CacheNamespaceEnum.DATASTORAGE
            + f"{KEY_PREFIX}{TENANTS_PREFIX}/{tenant_id}{MODELS_PREFIX}/*/dataStorages/{name}"
        )
        namespace_logs = namespace + "_logs"
        await FastAPICache.clear(namespace=namespace)
        await FastAPICache.clear(namespace=namespace_logs)

    @classmethod
    async def clear_data_storage_cache_by_name_and_model_name(
        cls, tenant_id: str, model_name: str, name: Optional[str]
    ) -> None:
        """Очистить кэш эндпоинтов, которые выдают конкретный DSO с именем name и моделью."""
        namespace = (
            CacheNamespaceEnum.DATASTORAGE
            + f"{KEY_PREFIX}{TENANTS_PREFIX}/{tenant_id}{MODELS_PREFIX}/{model_name}/dataStorages/{name}"
        )
        namespace_logs = namespace + "_logs"
        await FastAPICache.clear(namespace=namespace)
        await FastAPICache.clear(namespace=namespace_logs)

    @classmethod
    async def clear_databases_cache(cls, tenant_id: str) -> None:
        """Очистить кэш эндпоинтов, которые выдают список БД."""
        namespace = CacheNamespaceEnum.DATABASE + f"{KEY_PREFIX}{TENANTS_PREFIX}/{tenant_id}/databases/"
        await FastAPICache.clear(namespace=namespace)

    @classmethod
    async def clear_database_cache_by_name(cls, tenant_id: str, name: str) -> None:
        """Очистить кэш эндпоинтов, которые выдают конкретныую БД с именем name."""
        namespace = CacheNamespaceEnum.DATABASE + f"{KEY_PREFIX}{TENANTS_PREFIX}/{tenant_id}/databases/{name}"
        await FastAPICache.clear(namespace=namespace)

    @classmethod
    async def clear_dimensions_cache_by_model_name(cls, tenant_id: str, model_name: str) -> None:
        """Очистить кэш эндпоинтов, которые выдают список измерений."""
        namespace = (
            CacheNamespaceEnum.DIMENSION
            + f"{KEY_PREFIX}{TENANTS_PREFIX}/{tenant_id}{MODELS_PREFIX}/{model_name}/dimensions/"
        )
        await FastAPICache.clear(namespace=namespace)

    @classmethod
    async def clear_dimension_cache_by_name(cls, tenant_id: str, name: str) -> None:
        """Очистить кэш эндпоинтов, которые выдают конкретное измерение с именем name."""
        namespace = (
            CacheNamespaceEnum.DIMENSION
            + f"{KEY_PREFIX}{TENANTS_PREFIX}/{tenant_id}{MODELS_PREFIX}/*/dimensions/{name}"
        )
        await FastAPICache.clear(namespace=namespace)

    @classmethod
    async def clear_hierarchies_cache_by_model_name(cls, tenant_id: str, model_name: str) -> None:
        """Очистить кэш эндпоинтов, которые выдают список иерархий."""
        all_namespace = CacheNamespaceEnum.HIERARCHY + f"{KEY_PREFIX}{MODELS_PREFIX}/{model_name}/hierarchies/"
        by_dim_namespace = (
            CacheNamespaceEnum.HIERARCHY
            + f"{KEY_PREFIX}{TENANTS_PREFIX}/{tenant_id}{MODELS_PREFIX}/{model_name}/hierarchies/byDimensions/"
        )
        await FastAPICache.clear(namespace=all_namespace)
        await FastAPICache.clear(namespace=by_dim_namespace)

    @classmethod
    async def clear_hierarchy_cache_by_name(cls, tenant_id: str, dimension_name: str, name: str) -> None:
        """Очистить кэш эндпоинтов, которые выдают конкрутную иерархию с именем name."""
        hierarchy_namespace = (
            CacheNamespaceEnum.HIERARCHY
            + f"{KEY_PREFIX}{TENANTS_PREFIX}/{tenant_id}{MODELS_PREFIX}/*/dimensions/{dimension_name}/hierarchies/{name}"
        )
        await FastAPICache.clear(namespace=hierarchy_namespace)

    @classmethod
    async def clear_hierarchy_by_dimension(cls, tenant_id: str, model_name: str, dimension_name: str) -> None:
        """Очистить кэш эндпоинтов, которые выдают иерархии для dimension_name."""
        hierarchies_namespace = (
            CacheNamespaceEnum.HIERARCHY
            + f"{KEY_PREFIX}{TENANTS_PREFIX}/{tenant_id}{MODELS_PREFIX}/{model_name}/dimensions/{dimension_name}/hierarchies/"
        )
        await FastAPICache.clear(namespace=hierarchies_namespace)

    @classmethod
    async def clear_get_hierarchy_cache(cls, tenant_id: str, model_name: str) -> None:
        hierarchies_namespace = (
            CacheNamespaceEnum.HIERARCHY
            + f"{KEY_PREFIX}{TENANTS_PREFIX}/{tenant_id}{MODELS_PREFIX}/{model_name}/hierarchies/"
        )
        await FastAPICache.clear(namespace=hierarchies_namespace)

    @classmethod
    async def clear_measures_cache_by_model_name(cls, tenant_id: str, model_name: str) -> None:
        """Очистить кэш эндпоинтов, которые выдают список показателей."""
        namespace = (
            CacheNamespaceEnum.MEASURE
            + f"{KEY_PREFIX}{TENANTS_PREFIX}/{tenant_id}{MODELS_PREFIX}/{model_name}/measures/"
        )
        await FastAPICache.clear(namespace=namespace)

    @classmethod
    async def clear_measure_cache_by_name_and_model_name(cls, tenant_id: str, model_name: str, name: str) -> None:
        """Очистить кэш эндпоинтов, которые выдают  конкретный показатель с именем name"""
        namespace = (
            CacheNamespaceEnum.MEASURE
            + f"{KEY_PREFIX}{TENANTS_PREFIX}/{tenant_id}{MODELS_PREFIX}/{model_name}/measures/{name}"
        )
        await FastAPICache.clear(namespace=namespace)

    @classmethod
    async def clear_measure_cache_by_name(cls, tenant_id: str, name: str) -> None:
        """Очистить кэш эндпоинтов, которые выдают конкретный показатель с именем name"""
        namespace = (
            CacheNamespaceEnum.MEASURE + f"{KEY_PREFIX}{TENANTS_PREFIX}/{tenant_id}{MODELS_PREFIX}/*/measures/{name}"
        )
        await FastAPICache.clear(namespace=namespace)

    @classmethod
    async def clear_models_cache(cls, tenant_id: str) -> None:
        """Очистить кэш эндпоинтов, которые выдают список моделей."""
        namespace = CacheNamespaceEnum.MODEL + f"{KEY_PREFIX}{TENANTS_PREFIX}/{tenant_id}{MODELS_PREFIX}/"
        await FastAPICache.clear(namespace=namespace)

    @classmethod
    async def clear_model_cache_by_name(cls, tenant_id: str, name: str) -> None:
        """Очистить кэш эндпоинтов, которые выдают конкретную модель с именем name"""
        namespace = CacheNamespaceEnum.MODEL + f"{KEY_PREFIX}{TENANTS_PREFIX}/{tenant_id}{MODELS_PREFIX}/{name}"
        await FastAPICache.clear(namespace=namespace)

    @classmethod
    async def clear_tenants_cache(cls) -> None:
        """Очистить кэш эндпоинтов, которые выдают список Tenant."""
        namespace = CacheNamespaceEnum.TENANT + f"{KEY_PREFIX}{TENANTS_PREFIX}/"
        await FastAPICache.clear(namespace=namespace)

    @classmethod
    async def clear_tenant_cache_by_name(cls, name: str) -> None:
        """Очистить кэш эндпоинтов, которые выдают конкретныую Tenant с именем name."""
        namespace = CacheNamespaceEnum.TENANT + f"{KEY_PREFIX}{TENANTS_PREFIX}/{name}"
        await FastAPICache.clear(namespace=namespace)
