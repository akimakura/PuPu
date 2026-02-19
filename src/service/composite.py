"""
Сервис Композита.
Тут реализована вся логика работы сервиса.
Работа с БД инкапсулирована через DataRepository.
"""

from typing import Optional
from uuid import UUID

from py_common_lib.logger import EPMPYLogger
from py_common_lib.starlette_context_plugins import AuthorizationPlugin
from sqlalchemy.orm.exc import NoResultFound
from starlette_context import context

from src.integration.aor.client import ClientAOR
from src.integration.aor.model import AorType, CreateAorCommand, JsonData
from src.integration.worker_manager import ClientWorkerManager
from src.models.composite import Composite, CompositeCreateRequest, CompositeEditRequest
from src.models.copy_model import DetailsObjectCopyReponse, ObjectCopyResponse, ObjectCopyResult
from src.models.request_params import Pagination
from src.repository.aor import AorRepository
from src.repository.cache import CacheRepository
from src.repository.composite import CompositeRepository
from src.repository.model_relations import ModelRelationsRepository
from src.service.utils import get_updated_fields_object
from src.utils.auth import get_user_login_by_token
from src.utils.backoff import RetryConfig, retry

logger = EPMPYLogger(__name__)


class CompositeService:
    def __init__(
        self,
        data_repository: CompositeRepository,
        model_relations_repo: ModelRelationsRepository,
        worker_manager_client: ClientWorkerManager,
        aor_client: ClientAOR,
        aor_repository: AorRepository,
    ) -> None:
        self.data_repository = data_repository
        self.model_relations_repo = model_relations_repo
        self.worker_manager_client = worker_manager_client
        self.aor_client = aor_client
        self.aor_repository = aor_repository

    @retry(RetryConfig())
    async def get_composite_by_name(self, tenant_id: str, model_name: Optional[str], name: str) -> Composite:
        """Получить композит по имени."""
        result = await self.data_repository.get_by_name(tenant_id=tenant_id, model_name=model_name, name=name)
        return result

    async def get_composite_by_name_or_null(
        self, tenant_id: str, model_name: Optional[str], name: str
    ) -> Optional[Composite]:
        """Получить композит по имени."""
        try:
            return await self.get_composite_by_name(tenant_id=tenant_id, model_name=model_name, name=name)
        except NoResultFound:
            return None

    @retry(RetryConfig())
    async def get_composite_list_by_model_name(
        self, tenant_id: str, model_name: str, pagination: Optional[Pagination] = None
    ) -> list[Composite]:
        """Получить список всех композитов."""
        result = await self.data_repository.get_list(tenant_id=tenant_id, model_name=model_name, pagination=pagination)
        return result

    async def create_composite_by_schema(
        self,
        tenant_id: str,
        model_name: str,
        composite: CompositeCreateRequest,
        generate_on_db: bool = True,
        replace: bool = False,
        send_to_aor: bool = True,
        check_possible_delete: bool = True,
    ) -> Composite:
        """Создать композит."""
        result = await self.data_repository.create_by_schema(
            tenant_id=tenant_id,
            model_name=model_name,
            composite=composite,
            generate_on_db=generate_on_db,
            replace=replace,
        )
        await CacheRepository.clear_composites_cache_by_model_name(tenant_id=tenant_id, model_name=model_name)
        if not generate_on_db:
            await self.worker_manager_client.create_composite(tenant_id, [model_name], [composite.name], replace)
        if send_to_aor:
            await self.create_and_send_command_to_aor_by_composite(tenant_id, result)
        return result

    async def delete_composite_by_name(
        self,
        tenant_id: str,
        model_name: str,
        name: str,
        send_to_aor: bool = True,
        if_exists: bool = True,
        check_possible_delete: bool = True,
    ) -> None:
        """Удалить композит."""
        try:
            composite_related_objects = await self.model_relations_repo.get_composite_related_objects(
                tenant_name=tenant_id, model_name=model_name, object_name=name
            )
            composite_related_objects.raise_if_not_empty()
        except NoResultFound:
            logger.info(
                "There are no relations for composite %s.%s.%s in model relations table"
                "or There are no such composite in database",
                tenant_id,
                model_name,
                name,
            )
        result = await self.get_composite_by_name(tenant_id, model_name, name)
        if len(result.models_statuses) <= 1:
            command = await self.create_and_send_command_to_aor_by_composite(
                tenant_id,
                result,
                with_parents=False,
                send_command=False,
                deleted=True,
            )
        await self.data_repository.delete_by_name(tenant_id=tenant_id, model_name=model_name, name=name)
        if len(result.models_statuses) > 1:
            result = await self.get_composite_by_name(tenant_id, None, name)
            command = await self.create_and_send_command_to_aor_by_composite(
                tenant_id,
                result,
                send_command=False,
            )
        await CacheRepository.clear_composites_cache_by_model_name(tenant_id=tenant_id, model_name=model_name)
        await CacheRepository.clear_composite_cache_by_name(tenant_id=tenant_id, name=name)
        if send_to_aor:
            await self.aor_client.send_request(command)
        return None

    async def update_composite_by_name_and_schema(
        self,
        tenant_id: str,
        model_name: str,
        name: str,
        composite: CompositeEditRequest,
        generate_on_db: bool = True,
        send_to_aor: bool = True,
    ) -> Composite:
        """Обновить композит."""
        result = await self.data_repository.update_by_name_and_schema(
            tenant_id=tenant_id,
            model_name=model_name,
            name=name,
            composite=composite,
            generate_on_db=generate_on_db,
        )
        await CacheRepository.clear_composites_cache_by_model_name(tenant_id=tenant_id, model_name=model_name)
        await CacheRepository.clear_composite_cache_by_name(tenant_id=tenant_id, name=name)
        if not generate_on_db:
            await self.worker_manager_client.update_composite(
                tenant_id, [model_status.name for model_status in result.models_statuses], [name]
            )
        if send_to_aor:
            await self.create_and_send_command_to_aor_by_composite(tenant_id, result)
        return result

    async def copy_model_composite(
        self,
        tenant_id: str,
        name: str,
        model_names: list[str],
        generate_on_db: bool = True,
        replace: bool = True,
    ) -> Composite:
        """Обновить модель у data_storage."""
        prev_composite = await self.get_composite_by_name(tenant_id=tenant_id, model_name=None, name=name)
        result = await self.data_repository.copy_model_composite(
            tenant_id=tenant_id,
            name=name,
            model_names=model_names,
            generate_on_db=generate_on_db,
            replace=replace,
        )
        model_names_to_clear = model_names.copy()
        prev_model_names = [model_status.name for model_status in prev_composite.models_statuses]
        result_model_names = [model_status.name for model_status in result.models_statuses]
        model_names_to_clear.extend(prev_model_names)
        if prev_model_names != result_model_names:
            for model_name in model_names_to_clear:
                await CacheRepository.clear_composites_cache_by_model_name(tenant_id=tenant_id, model_name=model_name)
            await CacheRepository.clear_composite_cache_by_name(tenant_id=tenant_id, name=name)
        return result

    async def copy_model_composites(
        self,
        tenant_id: str,
        model_names: list[str],
        names: list[str],
        generate_on_db: bool = True,
        replace: bool = True,
        send_to_aor: bool = True,
        check_possible_delete: bool = True,
        raise_if_error: bool = False,
    ) -> tuple[DetailsObjectCopyReponse, bool]:
        response = []
        has_error = False
        for name in names:
            try:
                _ = await self.copy_model_composite(
                    tenant_id, name, model_names, generate_on_db=generate_on_db, replace=replace
                )
                response.append(
                    ObjectCopyResponse(
                        object_name=name,
                        tenant_id=tenant_id,
                        result=ObjectCopyResult.SUCCESS,
                        models=model_names,
                    )
                )
                if send_to_aor:
                    await self.send_to_aor_by_name(tenant_id, name)
            except Exception as exc:  # noqa
                logger.exception("Error copy %s.%s composite to models %s", tenant_id, name, model_names)
                has_error = True
                response.append(
                    ObjectCopyResponse(
                        object_name=name,
                        tenant_id=tenant_id,
                        result=ObjectCopyResult.FAILURE,
                        msg=str(exc),
                        models=model_names,
                    )
                )
        if not generate_on_db:
            await self.worker_manager_client.create_composite(tenant_id, model_names, names, replace)
        if raise_if_error and has_error:
            raise Exception(f"Error copy composites: {response}")
        return DetailsObjectCopyReponse(detail=response), has_error

    async def get_updated_fields(
        self, tenant_id: str, model_name: Optional[str], name: str, composite: CompositeEditRequest
    ) -> dict:
        """Получить поля, которые были изменены"""
        original_composite = await self.get_composite_by_name(tenant_id=tenant_id, model_name=model_name, name=name)
        original_composite = original_composite.model_dump(mode="json", by_alias=True)
        composite_dict = composite.model_dump(mode="json", by_alias=True, exclude_unset=True)
        return get_updated_fields_object(original_composite, composite_dict)

    async def create_and_send_command_to_aor_by_composite(
        self,
        tenant_id: str,
        composite: Composite,
        deleted: bool = False,
        custom_uuid: Optional[UUID] = None,
        with_parents: bool = True,
        send_command: bool = True,
        depends_no_attrs_versions: bool = False,
        version_suffix: str = "",
        parent_version_suffix: str = "",
        name_suffix: str = "",
        parent_name_suffix: str = "",
    ) -> Optional[CreateAorCommand]:
        try:
            data_json = JsonData(
                is_deleted=deleted, tenant=tenant_id, data_json=composite.model_dump(mode="json", by_alias=True)
            )
            parents = (
                await self.aor_repository.get_composite_parents_by_schema(tenant_id, composite) if with_parents else []
            )
            for parent in parents:
                parent.parent_name += parent_name_suffix
                parent.parent_external_id += parent_name_suffix
                if parent.parent_type in {AorType.MODEL, AorType.DATABASE}:
                    continue
                parent.parent_version += parent_version_suffix
            if depends_no_attrs_versions:
                for parent in parents:
                    if parent.parent_type != AorType.DIMENSION:
                        continue
                    if not parent_version_suffix:
                        parent.parent_version += "-no-attrs"
            command = {
                "type": AorType.COMPOSITE,
                "name": composite.name + name_suffix,
                "data_json": data_json,
                "description": composite.name + name_suffix,
                "version": (
                    str(composite.version) + version_suffix
                    if not deleted
                    else f"{composite.version}-deleted" + version_suffix
                ),
                "external_object_id": composite.name + name_suffix,
                "deployed_by": get_user_login_by_token(context.get(AuthorizationPlugin.key) or None),
                "parents": parents,
                "space_id": custom_uuid
                or (await self.aor_repository.get_composite_aor_space_by_names(tenant_id, [composite.name])).get(
                    composite.name
                ),
            }
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
        composite = await self.get_composite_by_name(tenant_id, None, name)
        await self.create_and_send_command_to_aor_by_composite(
            tenant_id,
            composite,
            deleted,
            custom_uuid,
            with_parents,
            depends_no_attrs_versions=depends_no_attrs_versions,
            version_suffix=version_suffix,
            parent_version_suffix=parent_version_suffix,
            name_suffix=name_suffix,
            parent_name_suffix=parent_name_suffix,
        )

    def __repr__(self) -> str:
        return "CompositeService"
