from collections import defaultdict

from sqlalchemy.ext.asyncio import AsyncSession

from src.models.model_relations import ChangeObjectStatusRequest, ChangeObjectStatusResponse
from src.models.tenant import SemanticObjects, SemanticObjectsTypeEnum
from src.repository.cache import CacheRepository
from src.repository.model_relations import ModelRelationsRepository


class UnknownObjectTypeException(Exception):
    def __init__(self, object_type: str) -> None:

        super().__init__()


class RelationsService:
    def __init__(self, session: AsyncSession, model_relations_repository: ModelRelationsRepository) -> None:
        self.session = session
        self.model_relations_repository = model_relations_repository

    async def find_where_used(
        self,
        model_name: str,
        object_name: str,
        tenant_name: str,
        object_type: SemanticObjectsTypeEnum,
    ) -> SemanticObjects:
        if object_type == SemanticObjectsTypeEnum.DIMENSION:
            result = await self.model_relations_repository.get_dimension_related_objects(
                model_name=model_name, object_name=object_name, tenant_name=tenant_name
            )
        elif object_type == SemanticObjectsTypeEnum.MEASURE:
            result = await self.model_relations_repository.get_measure_related_objects(
                model_name=model_name, object_name=object_name, tenant_name=tenant_name
            )
        elif object_type == SemanticObjectsTypeEnum.COMPOSITE:
            result = await self.model_relations_repository.get_composite_related_objects(
                model_name=model_name, object_name=object_name, tenant_name=tenant_name
            )
        elif object_type == SemanticObjectsTypeEnum.DATA_STORAGE:
            result = await self.model_relations_repository.get_datastorage_related_objects(
                model_name=model_name, object_name=object_name, tenant_name=tenant_name
            )
        else:
            raise Exception("Unknown object type")

        return result

    async def update_relations_status(
        self, tenant_id: str, requests: list[ChangeObjectStatusRequest]
    ) -> list[ChangeObjectStatusResponse]:
        """
        Обновляет статус отношений объектов модели (dimensions, measures, composites и др.) для указанного тенанта.

        Метод группирует запросы по типу объекта (`object_type`), вызывает соответствующие методы репозитория
        для обновления статуса связей каждого типа объектов, очищает кэш после изменений и сохраняет изменения
        в сессии базы данных.

        Args:
            tenant_id (str): Идентификатор тенанта, от имени которого выполняются операции.
            requests (list[ChangeObjectStatusRequest]): Список запросов на изменение статуса связи объектов.

        Returns:
            list[ChangeObjectStatusResponse]: Список результатов обработки запросов.
        """
        responses = []
        grouped_requests = defaultdict(list)
        for item in requests:
            grouped_requests[item.object_type].append(item)
        for object_type, requests in grouped_requests.items():
            if object_type == SemanticObjectsTypeEnum.DIMENSION:
                responses.extend(
                    await self.model_relations_repository.update_dimension_model_relations_status_without_commit(
                        tenant_id=tenant_id, requests=requests
                    )
                )
                for request in requests:
                    await CacheRepository.clear_dimension_cache_by_name(tenant_id, request.object_name)
                    await CacheRepository.clear_dimensions_cache_by_model_name(tenant_id, request.model)
            elif object_type == SemanticObjectsTypeEnum.MEASURE:
                responses.extend(
                    await self.model_relations_repository.update_measure_model_relations_status_without_commit(
                        tenant_id=tenant_id, requests=requests
                    )
                )
                for request in requests:
                    await CacheRepository.clear_measure_cache_by_name(tenant_id, request.object_name)
                    await CacheRepository.clear_measures_cache_by_model_name(tenant_id, request.model)
            elif object_type == SemanticObjectsTypeEnum.COMPOSITE:
                responses.extend(
                    await self.model_relations_repository.update_composite_model_relations_status_without_commit(
                        tenant_id=tenant_id, requests=requests
                    )
                )
                for request in requests:
                    await CacheRepository.clear_composite_cache_by_name(tenant_id, request.object_name)
                    await CacheRepository.clear_composites_cache_by_model_name(tenant_id, request.model)
            elif object_type == SemanticObjectsTypeEnum.DATA_STORAGE:
                responses.extend(
                    await self.model_relations_repository.update_data_storage_model_relations_status_without_commit(
                        tenant_id=tenant_id, requests=requests
                    )
                )
                for request in requests:
                    await CacheRepository.clear_data_storages_cache_by_name(tenant_id, request.object_name)
                    await CacheRepository.clear_data_storages_cache_by_model_name(tenant_id, request.model)
            elif object_type == SemanticObjectsTypeEnum.PV_DICTIONARY:
                responses.extend(
                    await self.model_relations_repository.update_pv_dictionary_model_relations_status_without_commit(
                        tenant_id=tenant_id, requests=requests
                    )
                )
                for request in requests:
                    await CacheRepository.clear_dimension_cache_by_name(tenant_id, request.object_name)
                    await CacheRepository.clear_dimensions_cache_by_model_name(tenant_id, request.model)
            elif object_type == SemanticObjectsTypeEnum.DATABASE_OBJECT:
                responses.extend(
                    await self.model_relations_repository.update_database_object_model_relations_status_without_commit(
                        tenant_id=tenant_id, requests=requests
                    )
                )
        await self.session.commit()
        return responses
