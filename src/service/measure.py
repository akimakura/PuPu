"""
Сервис показателей.
Тут реализована вся логика работы сервиса.
Работа с БД инкапсулирована через DataRepository.
"""

from typing import Optional
from uuid import UUID

from py_common_lib.logger import EPMPYLogger
from py_common_lib.starlette_context_plugins import AuthorizationPlugin
from sqlalchemy.exc import NoResultFound
from starlette_context import context

from src.integration.aor.client import ClientAOR
from src.integration.aor.model import AorType, CreateAorCommand, JsonData
from src.models.copy_model import DetailsObjectCopyReponse, ObjectCopyResponse, ObjectCopyResult
from src.models.measure import Measure, MeasureCreateRequest, MeasureEditRequest
from src.models.request_params import Pagination
from src.repository.aor import AorRepository
from src.repository.cache import CacheRepository
from src.repository.measure import MeasureRepository
from src.repository.model_relations import ModelRelationsRepository
from src.service.utils import get_updated_fields_object
from src.utils.auth import get_user_login_by_token
from src.utils.backoff import RetryConfig, retry

logger = EPMPYLogger(__name__)


class MeasureService:
    def __init__(
        self,
        data_repository: MeasureRepository,
        model_relationships_repo: ModelRelationsRepository,
        aor_client: ClientAOR,
        aor_repository: AorRepository,
    ) -> None:
        self.data_repository: MeasureRepository = data_repository
        self.model_relationships_repo = model_relationships_repo
        self.aor_client = aor_client
        self.aor_repository = aor_repository

    @retry(RetryConfig())
    async def get_measure_list_by_model_name(
        self, tenant_id: str, model_name: str, pagination: Optional[Pagination] = None
    ) -> list[Measure]:
        """Получить список всех показателей."""
        return await self.data_repository.get_list(tenant_id=tenant_id, model_name=model_name, pagination=pagination)

    @retry(RetryConfig())
    async def get_measure_list_by_names(
        self, tenant_id: str, model_name: str, names: list[str], pagination: Optional[Pagination] = None
    ) -> list[Measure]:
        """Получить список показателей с именами names."""
        return await self.data_repository.get_list(
            tenant_id=tenant_id, model_name=model_name, names=names, pagination=pagination
        )

    @retry(RetryConfig())
    async def get_measure_by_measure_name(self, tenant_id: str, name: str, model_name: Optional[str] = None) -> Measure:
        """Получить показатель по имени."""
        return await self.data_repository.get_by_name(tenant_id=tenant_id, name=name, model_name=model_name)

    async def get_measure_by_measure_name_or_null(
        self, tenant_id: str, name: str, model_name: Optional[str] = None
    ) -> Optional[Measure]:
        """Получить показатель по имени."""
        try:
            return await self.get_measure_by_measure_name(tenant_id=tenant_id, name=name, model_name=model_name)
        except NoResultFound:
            return None

    async def delete_measure_by_name(
        self,
        tenant_id: str,
        model_name: str,
        name: str,
        send_to_aor: bool = True,
        check_possible_delete: bool = True,
    ) -> None:
        """Удалить показатель."""
        measure = await self.get_measure_by_measure_name(tenant_id, name, model_name)
        try:
            measure_related_objects = await self.model_relationships_repo.get_measure_related_objects(
                tenant_name=tenant_id, model_name=model_name, object_name=name
            )
            measure_related_objects.raise_if_not_empty()
        except NoResultFound:
            logger.info(
                "There are no relations with measure %s.%s.%s." "Or there are no such measure in database",
                tenant_id,
                model_name,
                name,
            )
        if len(measure.models_statuses) <= 1:
            command = await self.create_and_send_command_to_aor_by_measure(
                tenant_id, measure, deleted=True, with_parents=False, send_command=False
            )
        await self.data_repository.delete_by_name(
            tenant_id=tenant_id,
            model_name=model_name,
            name=name,
        )
        if len(measure.models_statuses) > 1:
            measure = await self.get_measure_by_measure_name(tenant_id, name, None)
            command = await self.create_and_send_command_to_aor_by_measure(tenant_id, measure, send_command=False)
        await CacheRepository.clear_measures_cache_by_model_name(tenant_id=tenant_id, model_name=model_name)
        await CacheRepository.clear_measure_cache_by_name_and_model_name(
            tenant_id=tenant_id, model_name=model_name, name=name
        )
        if send_to_aor:
            await self.aor_client.send_request(command)
        return None

    async def create_measure_by_schema(
        self,
        tenant_id: str,
        model_name: str,
        measure: MeasureCreateRequest,
        send_to_aor: bool = True,
        check_possible_delete: bool = True,
    ) -> Measure:
        """Создать показатель."""
        result = await self.data_repository.create_by_schema(
            tenant_id=tenant_id,
            model_name=model_name,
            measure=measure,
        )
        await CacheRepository.clear_measures_cache_by_model_name(tenant_id=tenant_id, model_name=model_name)
        if send_to_aor:
            await self.create_and_send_command_to_aor_by_measure(tenant_id, result)
        return result

    async def update_measure_by_name_and_schema(
        self,
        tenant_id: str,
        model_name: str,
        name: str,
        measure: MeasureEditRequest,
        send_to_aor: bool = True,
    ) -> Measure:
        """Обновить показатель."""
        result = await self.data_repository.update_by_name_and_schema(
            tenant_id=tenant_id, model_name=model_name, name=name, measure=measure
        )
        await CacheRepository.clear_measures_cache_by_model_name(tenant_id=tenant_id, model_name=model_name)
        await CacheRepository.clear_measure_cache_by_name_and_model_name(
            tenant_id=tenant_id, model_name=model_name, name=name
        )
        if send_to_aor:
            await self.create_and_send_command_to_aor_by_measure(tenant_id, result)
        return result

    async def get_updated_fields(self, tenant_id: str, model_name: str, name: str, measure: MeasureEditRequest) -> dict:
        """Получить поля, которые были изменены"""
        original_measure = await self.get_measure_by_measure_name(tenant_id=tenant_id, model_name=model_name, name=name)
        original_measure = original_measure.model_dump(mode="json", by_alias=True)
        measure_dict = measure.model_dump(mode="json", by_alias=True, exclude_unset=True)
        return get_updated_fields_object(original_measure, measure_dict)

    async def copy_model_measure(self, tenant_id: str, name: str, model_names: list[str]) -> Measure:
        """Обновить модель у measure."""
        prev_measure = await self.get_measure_by_measure_name(tenant_id=tenant_id, name=name)
        result = await self.data_repository.copy_model_measure(tenant_id=tenant_id, name=name, model_names=model_names)
        models_names_to_clear = model_names.copy()
        prev_measure_models_names = [model_status.name for model_status in prev_measure.models_statuses]
        result_models_names = [model_status.name for model_status in result.models_statuses]
        models_names_to_clear.extend(prev_measure_models_names)
        if prev_measure_models_names != result_models_names:
            for model_name in models_names_to_clear:
                await CacheRepository.clear_measures_cache_by_model_name(tenant_id=tenant_id, model_name=model_name)
            await CacheRepository.clear_measure_cache_by_name(tenant_id=tenant_id, name=name)
        return result

    async def copy_model_measures(
        self,
        tenant_id: str,
        model_names: list[str],
        names: list[str],
        send_to_aor: bool = True,
        check_possible_delete: bool = True,
        raise_if_error: bool = False,
    ) -> tuple[DetailsObjectCopyReponse, bool]:
        """Обновить модель у нескольких measure."""
        response = []
        has_error = False
        for name in names:
            try:
                _ = await self.copy_model_measure(tenant_id, name, model_names)
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
                logger.exception("Error copy %s.%s measure to models %s", tenant_id, name, model_names)
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
        if raise_if_error and has_error:
            raise Exception(f"Error copy datastorages: {response}")
        return DetailsObjectCopyReponse(detail=response), has_error

    async def create_and_send_command_to_aor_by_measure(
        self,
        tenant_id: str,
        measure: Measure,
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
                is_deleted=deleted, tenant=tenant_id, data_json=measure.model_dump(mode="json", by_alias=True)
            )
            parents = (
                await self.aor_repository.get_measure_parents_by_schema(tenant_id, measure) if with_parents else []
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
                "type": AorType.MEASURE,
                "name": measure.name + name_suffix,
                "data_json": data_json,
                "description": measure.name,
                "version": (
                    str(measure.version) + version_suffix
                    if not deleted
                    else f"{measure.version}-deleted" + version_suffix
                ),
                "external_object_id": measure.name + name_suffix,
                "deployed_by": get_user_login_by_token(context.get(AuthorizationPlugin.key) or None),
                "parents": parents,
                "space_id": custom_uuid
                or (await self.aor_repository.get_measure_aor_space_by_names(tenant_id, [measure.name])).get(
                    measure.name
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
        measure_model = await self.get_measure_by_measure_name(tenant_id, name)
        await self.create_and_send_command_to_aor_by_measure(
            tenant_id,
            measure_model,
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
        return "MeasureService"
