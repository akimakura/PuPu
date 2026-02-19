"""
Сервис моделей.
Тут реализована вся логика работы сервиса.
Работа с БД инкапсулирована через DataRepository.
"""

from typing import Optional
from uuid import UUID

from py_common_lib.logger import EPMPYLogger
from py_common_lib.starlette_context_plugins import AuthorizationPlugin
from sqlalchemy.exc import NoResultFound
from starlette_context import context

from src.db.model import Model as ModelORM
from src.integration.aor.client import ClientAOR
from src.integration.aor.model import AorType, CreateAorCommand, JsonData
from src.models.data_storage import DataStorageEnum
from src.models.model import Model
from src.models.request_params import Pagination
from src.repository.aor import AorRepository
from src.repository.cache import CacheRepository
from src.repository.meta_synchronizer import MetaSynchronizerRepository
from src.repository.model import ModelCreateRequestModel, ModelEditRequestModel, ModelRepository
from src.service.utils import get_updated_fields_object
from src.utils.auth import get_user_login_by_token
from src.utils.backoff import RetryConfig, retry

logger = EPMPYLogger(__name__)


class ModelService:
    def __init__(
        self,
        data_repository: ModelRepository,
        meta_sync_repository: MetaSynchronizerRepository,
        aor_client: ClientAOR,
        aor_repository: AorRepository,
    ) -> None:
        self.meta_sync_repository: MetaSynchronizerRepository = meta_sync_repository
        self.data_repository: ModelRepository = data_repository
        self.aor_repository = aor_repository
        self.aor_client = aor_client

    @retry(RetryConfig())
    async def get_model_by_name(self, tenant_id: str, name: str) -> Model | None:
        """Получить модель по имени."""
        return await self.data_repository.get_by_name(tenant_id=tenant_id, name=name)

    async def get_model_by_name_or_null(self, tenant_id: str, name: str) -> Model | None:
        """Получить модель по имени."""
        try:
            return await self.get_model_by_name(tenant_id=tenant_id, name=name)
        except NoResultFound:
            return None

    async def get_model_orm(self, tenant_id: str, name: str) -> ModelORM | None:
        """
        Получает объект ORM модели по её имени и идентификатору арендатора.

        Args:
            tenant_id (str): Идентификатор арендатора, к которому привязана модель.
            name (str): Имя модели, по которому она должна быть найдена.

        Returns:
            ModelORM | None: Объект ORM модели, если она найдена, иначе None.
        """
        return await self.data_repository.get_model_orm(tenant_id=tenant_id, name=name)

    @retry(RetryConfig())
    async def get_model_list(self, tenant_id: str, pagination: Optional[Pagination] = None) -> list[Model]:
        """
        Возвращает список моделей для указанного тенанта с опциональной пагинацией.

        Args:
            tenant_id (str): Идентификатор арендатора (тенанта), чьи модели запрашиваются.
            pagination (Optional[Pagination]): Объект пагинации с параметрами limit и offset.
                                               Если None — возвращаются все модели без ограничений.

        Returns:
            list[Model]: Список моделей, соответствующих критериям. Может быть пустым, если моделей нет.

        Raises:
            HTTPException: Возможна ошибка 500 при проблемах доступа к репозиторию.
        """
        return await self.data_repository.get_list(tenant_id=tenant_id, pagination=pagination)

    async def get_model_list_by_names(self, tenant_id: str, names: list[str]) -> list[ModelORM]:
        """
        Возвращает список моделей по их именам для указанного тенанта.

        Если какие-либо из запрошенных имён не существуют, они игнорируются — возвращаются только существующие модели.

        Args:
            tenant_id (str): Идентификатор арендатора (тенанта).
            names (list[str]): Список имён моделей, которые необходимо получить.

        Returns:
            list[ModelORM]: Список объектов моделей, найденных по переданным именам.
                            Может быть пустым, если ни одна модель не найдена.

        Raises:
            HTTPException: Возможна ошибка 500 при внутренней ошибке репозитория.
        """

        return await self.data_repository.get_list_by_names(tenant_id=tenant_id, names=names)

    async def delete_model_by_name(self, tenant_id: str, model_name: str, send_to_aor: bool = True) -> None:
        """Удалить модель."""
        result = await self.data_repository.get_by_name(tenant_id=tenant_id, name=model_name)
        command = await self.create_and_send_command_to_aor_by_model(
            tenant_id, result, deleted=True, with_parents=False, send_command=False
        )
        await self.data_repository.delete_by_name(tenant_id=tenant_id, name=model_name)
        await CacheRepository.clear_models_cache(tenant_id=tenant_id)
        await CacheRepository.clear_model_cache_by_name(tenant_id=tenant_id, name=model_name)
        await CacheRepository.clear_tenants_cache()
        await CacheRepository.clear_tenant_cache_by_name(name=tenant_id)
        if send_to_aor:
            await self.aor_client.send_request(command)
        return None

    async def create_model_by_schema(
        self, tenant_id: str, model: ModelCreateRequestModel, send_to_aor: bool = True
    ) -> Model:
        """Создать модель."""
        result = await self.data_repository.create_by_schema(tenant_id=tenant_id, model=model)
        await CacheRepository.clear_models_cache(tenant_id=tenant_id)
        await CacheRepository.clear_tenants_cache()
        await CacheRepository.clear_tenant_cache_by_name(name=tenant_id)
        if send_to_aor:
            await self.create_and_send_command_to_aor_by_model(tenant_id, result)
        return result

    async def update_model_by_name_and_schema(
        self,
        tenant_id: str,
        model_name: str,
        model: ModelEditRequestModel,
        enable_recreate_not_empty_tables: bool = False,
        send_to_aor: bool = True,
    ) -> Model:
        """Обновить модель."""
        original_model = await self.data_repository.get_by_name(tenant_id=tenant_id, name=model_name)
        original_dimension_tech_fields = original_model.dimension_tech_fields
        result = await self.data_repository.update_by_schema_and_name(tenant_id=tenant_id, name=model_name, model=model)
        if original_dimension_tech_fields != result.dimension_tech_fields:
            await self.meta_sync_repository.upate_datastorages_in_database_from_meta(
                tenant_id,
                model_name,
                ignore=[],
                enable_delete_column=True,
                enable_delete_not_empty=enable_recreate_not_empty_tables,
                white_list_types={DataStorageEnum.DIMENSION_VALUES},
            )
            await CacheRepository.clear_data_storages_cache_by_model_name(tenant_id=tenant_id, model_name=model_name)
            await CacheRepository.clear_data_storage_cache_by_name_and_model_name(
                tenant_id=tenant_id, model_name=model_name, name="*"
            )
        await CacheRepository.clear_models_cache(tenant_id=tenant_id)
        await CacheRepository.clear_model_cache_by_name(tenant_id=tenant_id, name=model_name)
        await CacheRepository.clear_tenants_cache()
        await CacheRepository.clear_tenant_cache_by_name(name=tenant_id)
        if send_to_aor:
            await self.create_and_send_command_to_aor_by_model(tenant_id, result)
        return result

    async def get_updated_fields(self, tenant_id: str, name: str, model: ModelEditRequestModel) -> dict:
        """Получить поля, которые были изменены"""
        original_model = await self.get_model_by_name(tenant_id=tenant_id, name=name)
        original_model = original_model.model_dump(mode="json", by_alias=True)
        model_dict = model.model_dump(mode="json", by_alias=True, exclude_unset=True)
        return get_updated_fields_object(original_model, model_dict)

    async def create_and_send_command_to_aor_by_model(
        self,
        tenant_id: str,
        model: Model,
        deleted: bool = False,
        custom_uuid: Optional[UUID] = None,
        with_parents: bool = True,
        send_command: bool = True,
        version_suffix: str = "",
        parent_version_suffix: str = "",
        name_suffix: str = "",
        parent_name_suffix: str = "",
    ) -> Optional[CreateAorCommand]:
        try:
            parents = (await self.aor_repository.get_model_parents_by_names(tenant_id, [model.name])).get(
                model.name, []
            )
            data_json = JsonData(
                is_deleted=deleted, tenant=tenant_id, data_json=model.model_dump(mode="json", by_alias=True)
            )
            for parent in parents:
                parent.parent_name += parent_name_suffix
                parent.parent_external_id += parent_name_suffix
                parent.parent_version += parent_version_suffix
            command = {
                "type": AorType.MODEL,
                "name": model.name + name_suffix,
                "data_json": data_json,
                "description": model.name,
                "version": (
                    str(model.version) + version_suffix if not deleted else f"{model.version}-deleted" + version_suffix
                ),
                "external_object_id": model.name + name_suffix,
                "deployed_by": get_user_login_by_token(context.get(AuthorizationPlugin.key) or None),
                "parents": parents if with_parents else [],
                "space_id": model.aor_space_id,
            }
            if custom_uuid:
                command["space_id"] = custom_uuid
            command_model = CreateAorCommand.model_validate(command)
        except Exception:
            logger.exception("Create command model failed.")
            command_model = None
        if send_command and command_model:
            await self.aor_client.send_request(command_model)
        return command_model

    async def send_to_aor_by_name(
        self,
        tenant_id: str,
        name: str,
        deleted: bool = False,
        custom_uuid: Optional[UUID] = None,
        with_parents: bool = True,
        dim_with_attributes: bool = True,
        depends_no_attrs_versions: bool = False,
        version_suffix: str = "",
        parent_version_suffix: str = "",
        name_suffix: str = "",
        parent_name_suffix: str = "",
    ) -> None:
        model = await self.get_model_by_name(tenant_id, name)
        await self.create_and_send_command_to_aor_by_model(
            tenant_id,
            model,
            deleted,
            custom_uuid,
            with_parents,
            version_suffix=version_suffix,
            parent_version_suffix=parent_version_suffix,
            name_suffix=name_suffix,
            parent_name_suffix=parent_name_suffix,
        )

    def __repr__(self) -> str:
        return "ModelService"
