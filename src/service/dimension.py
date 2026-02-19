"""
Сервис измерений.
Тут реализована вся логика работы сервиса.
Работа с БД инкапсулирована через DataRepository.
"""

from collections import defaultdict
from typing import Optional
from uuid import UUID

from fastapi import UploadFile
from pandas import Series
from py_common_lib.logger import EPMPYLogger
from py_common_lib.starlette_context_plugins import AuthorizationPlugin
from sqlalchemy.orm.exc import NoResultFound
from starlette_context import context

from src.config import settings
from src.db.dimension import Dimension as OrmDimension
from src.events.dimension import DimensionEventsProcessor
from src.integration.aor.client import ClientAOR
from src.integration.aor.model import AorType, CreateAorCommand, JsonData
from src.integration.pv_dictionaries.client import ClientPVDictionaries
from src.integration.pv_dictionaries.models import PVDictionary
from src.integration.worker_manager import ClientWorkerManager
from src.models.any_field import AnyField
from src.models.consts import (
    DIMENSION_ATTRIBUTES_FILE_COLUMNS,
    DIMENSIONS_ATTRIBUTES_FILE_COLUMNS_TYPE,
    DIMENSIONS_FILE_COLUMNS,
    DIMENSIONS_FILE_COLUMNS_TYPE,
)
from src.models.copy_model import DetailsObjectCopyReponse, ObjectCopyResponse, ObjectCopyResult
from src.models.data_storage import DataStorageLogsFieldEnum
from src.models.dimension import (
    ChangeDictionaryStuctureActionsEnum,
    Dimension,
    DimensionAttribute,
    DimensionAttributeRequest,
    DimensionCreateRequest,
    DimensionEditRequest,
    TextEnum,
)
from src.models.field import BaseFieldType, BaseFieldTypeEnum
from src.models.model_import import ImportFromFileResponse
from src.models.request_params import DimensionAttributesFileColumnEnum, DimensionsFileColumnEnum, Pagination
from src.repository.aor import AorRepository
from src.repository.cache import CacheRepository
from src.repository.dimension import DimensionRepository
from src.repository.model_relations import ModelRelationsRepository
from src.repository.utils import is_ignore_dimension
from src.service.utils import get_updated_fields_object, labels_by_row, read_upload_file_as_dataframe
from src.utils.auth import get_user_login_by_token
from src.utils.backoff import RetryConfig, retry

logger = EPMPYLogger(__name__)


class DimensionService:
    def __init__(
        self,
        data_repository: DimensionRepository,
        model_relations_repo: ModelRelationsRepository,
        worker_manager_client: ClientWorkerManager,
        aor_client: ClientAOR,
        aor_repository: AorRepository,
    ) -> None:
        self.data_repository: DimensionRepository = data_repository
        self.events_processor = DimensionEventsProcessor()
        self.model_relations_repo = model_relations_repo
        self.worker_manager_client = worker_manager_client
        self.aor_client = aor_client
        self.aor_repository = aor_repository

    async def create_dimension_attributes_in_pvd(
        self,
        tenant_id: str,
        attributes: (
            list[DimensionAttribute]
            | list[DimensionAttributeRequest]
            | list[DimensionAttribute | DimensionAttributeRequest]
        ),
    ) -> None:
        """Создать атрибуты в PV Dictionary."""
        if not settings.ENABLE_PV_DICTIONARIES:
            return None
        for attribute in attributes:
            if attribute.ref_type.ref_object_type != BaseFieldTypeEnum.DIMENSION or not isinstance(
                attribute.ref_type.ref_object, str
            ):
                continue
            await self.create_pv_dictionary_by_dimension(
                tenant_id, attribute.ref_type.ref_object, with_error=False, commit=True
            )

    @retry(RetryConfig())
    async def get_dimension_by_dimension_name(self, tenant_id: str, name: str, model_name: Optional[str]) -> Dimension:
        """Получить измерение по имени."""
        return await self.data_repository.get_by_name(tenant_id=tenant_id, name=name, model_name=model_name)

    async def get_dimension_by_dimension_name_or_null(
        self, tenant_id: str, name: str, model_name: Optional[str]
    ) -> Optional[Dimension]:
        """Получить измерение по имени."""
        try:
            return await self.get_dimension_by_dimension_name(tenant_id=tenant_id, name=name, model_name=model_name)
        except NoResultFound:
            return None

    @retry(RetryConfig())
    async def get_dimension_list_by_names(
        self, tenant_id: str, model_name: str, names: list[str], pagination: Optional[Pagination] = None
    ) -> list[Dimension]:
        """Получить список всех полей из списка"""
        return await self.data_repository.get_list(
            tenant_id=tenant_id, model_name=model_name, names=names, pagination=pagination
        )

    @retry(RetryConfig())
    async def get_dimension_list_by_model_name(
        self, tenant_id: str, model_name: str, pagination: Optional[Pagination] = None
    ) -> list[Dimension]:
        """Получить список всех измерений."""
        return await self.data_repository.get_list(tenant_id=tenant_id, model_name=model_name, pagination=pagination)

    async def create_dimension_by_schema(
        self,
        tenant_id: str,
        model_name: str,
        dimension: DimensionCreateRequest,
        if_not_exists: bool = False,
        generate_on_db: bool = True,
        send_to_aor: bool = True,
        check_possible_delete: bool = True,
    ) -> Optional[Dimension]:
        """Создать измерение."""
        if is_ignore_dimension(model_name, dimension.name, dimension.is_virtual):
            logger.warning("Dimension %s.%s.%s is not allowed to create.", tenant_id, model_name, dimension.name)
            return None
        await self.data_repository.create_not_virtual_dimensions(
            tenant_id=tenant_id,
            model_names=[model_name],
            generate_on_db=generate_on_db,
            check_possible_delete=check_possible_delete,
        )
        await self.create_dimension_attributes_in_pvd(tenant_id, dimension.attributes)

        result = await self.data_repository.create_by_schema(
            tenant_id=tenant_id,
            model_name=model_name,
            dimension=dimension,
            if_not_exists=if_not_exists,
            generate_on_db=generate_on_db,
            check_possible_delete=check_possible_delete,
        )
        await CacheRepository.clear_dimensions_cache_by_model_name(tenant_id=tenant_id, model_name=model_name)
        await CacheRepository.clear_data_storages_cache_by_model_name(tenant_id=tenant_id, model_name=model_name)
        _ = await self.events_processor.change_dictionary_structure(
            tenant_id, model_name, ChangeDictionaryStuctureActionsEnum.CREATE, result
        )
        if not generate_on_db:
            await self.worker_manager_client.create_dimension(
                tenant_id,
                [model_name],
                [DataStorageLogsFieldEnum.OPERATION],
                if_not_exists=True,
            )
            await self.worker_manager_client.create_dimension(
                tenant_id,
                [model_name],
                [dimension.name],
                if_not_exists=if_not_exists,
            )
        if send_to_aor:
            await self.create_and_send_command_to_aor_by_dimension(tenant_id, result)
        return result

    async def get_dimension_orm_model(self, tenant_id: str, model_name: str, name: str) -> OrmDimension:
        """
        Получает объект ORM измерения по его имени, имени модели и идентификатору арендатора.

        Args:
            tenant_id (str): Идентификатор арендатора, к которому относится измерение.
            model_name (str): Имя модели, к которой привязано измерение.
            name (str): Имя самого измерения.

        Returns:
            OrmDimension: Объект ORM измерения, соответствующий указанным параметрам.
        """
        return await self.data_repository.get_dimension_orm_model(tenant_id=tenant_id, model_name=model_name, name=name)

    async def delete_dimension_in_pvd_by_pv_name(self, pv_name: str) -> None:
        """Удалить измерение из PVD."""
        if not settings.ENABLE_PV_DICTIONARIES:
            return None
        client_pv = ClientPVDictionaries({})
        return await client_pv.delete_dictionary(pv_name)

    async def delete_dimension_in_pvd_by_dimension(self, dimension: Dimension) -> None:
        if len(dimension.models_statuses) <= 1:
            logger.info("Deleted last model... Try delete %s from PVD", dimension.name)
            if dimension.pv_dictionary and dimension.pv_dictionary.object_name:
                await self.delete_dimension_in_pvd_by_pv_name(dimension.pv_dictionary.object_name)
            else:
                logger.info("There is no PVDictionary field. Skip deleting %s from PVD", dimension.name)
        else:
            logger.info("Dimension %s has more than one model. Cannot be deleted from PVD", dimension.name)
        return None

    async def delete_dimension_in_pvd_by_tenant_model_name(
        self, tenant_id: str, dimension_name: str, model_name: str | None = None
    ) -> None:
        """
        Удалить измерение из PVD.
        """
        dimension = await self.data_repository.get_dimension_orm_model(
            tenant_id=tenant_id,
            name=dimension_name,
            model_name=model_name,
        )
        if dimension is None:
            raise NoResultFound(
                f"Dimension with tenant_id={tenant_id}, model_name={model_name} and name={dimension_name} not found."
            )
        await self.delete_dimension_in_pvd_by_dimension(Dimension.model_validate(dimension))
        dimension.pv_dictionary = None
        await self.data_repository.session.commit()
        for model in dimension.models:
            await CacheRepository.clear_dimensions_cache_by_model_name(tenant_id=tenant_id, model_name=model.name)
        await CacheRepository.clear_dimension_cache_by_name(tenant_id=tenant_id, name=dimension.name)
        await self.clear_data_storage_cache(
            tenant_id=tenant_id,
            model_name="*",
            dimension=dimension,
        )
        return None

    async def delete_dimension_by_name(
        self,
        tenant_id: str,
        model_name: str,
        name: str,
        if_exists: bool = False,
        send_to_aor: bool = True,
        check_possible_delete: bool = True,
    ) -> bool:
        """Удалить измерение."""
        try:
            dimension_related_objects = await self.model_relations_repo.get_dimension_related_objects(
                tenant_name=tenant_id, object_name=name, model_name=model_name
            )
            dimension_related_objects.raise_if_not_empty()
        except NoResultFound:
            logger.info(
                "There are no related objects for dimension  %s.%s.%s." "Or there are no such dimension in database",
                tenant_id,
                model_name,
                name,
            )
        dimension: Dimension = await self.get_dimension_by_dimension_name(
            tenant_id=tenant_id,
            name=name,
            model_name=model_name,
        )
        if is_ignore_dimension(model_name, name, dimension.is_virtual):
            logger.warning("Dimension %s.%s.%s is not allowed to delete.", tenant_id, model_name, name)
            return False
        if len(dimension.models_statuses) <= 1:
            command = await self.create_and_send_command_to_aor_by_dimension(
                tenant_id, dimension, deleted=True, with_parents=False, send_command=False
            )
        await self.data_repository.delete_by_name(
            tenant_id=tenant_id,
            model_name=model_name,
            name=name,
            if_exists=if_exists,
            check_possible_delete=check_possible_delete,
        )
        await self.delete_dimension_in_pvd_by_dimension(dimension)
        await self.data_repository.session.commit()
        if len(dimension.models_statuses) > 1:
            dimension = await self.get_dimension_by_dimension_name(
                tenant_id=tenant_id,
                name=name,
                model_name=None,
            )
            command = await self.create_and_send_command_to_aor_by_dimension(tenant_id, dimension, send_command=False)
        await CacheRepository.clear_dimensions_cache_by_model_name(tenant_id=tenant_id, model_name=model_name)
        await CacheRepository.clear_dimension_cache_by_name(tenant_id=tenant_id, name=name)
        await self.clear_data_storage_cache(tenant_id=tenant_id, model_name="*", dimension=dimension)
        _ = await self.events_processor.change_dictionary_structure(
            tenant_id, model_name, ChangeDictionaryStuctureActionsEnum.DELETE, dimension
        )
        if send_to_aor:
            await self.aor_client.send_request(command)
        return True

    async def update_dimension_by_name_and_schema(
        self,
        tenant_id: str,
        model_name: str,
        name: str,
        dimension: DimensionEditRequest,
        generate_on_db: bool = True,
        enable_delete_column: bool = True,
        enable_delete_not_empty: bool = False,
        send_to_aor: bool = True,
    ) -> Optional[Dimension]:
        """Обновить измерение."""
        prev_dimension: Dimension = await self.get_dimension_by_dimension_name(
            tenant_id=tenant_id, name=name, model_name=model_name
        )
        all_models_in_blacklist = True
        for models_status in prev_dimension.models_statuses:
            if models_status.name not in settings.MODELS_BLACKLIST:
                all_models_in_blacklist = False
                break
        if all_models_in_blacklist and is_ignore_dimension(
            model_name,
            name,
            dimension.is_virtual if dimension.is_virtual is not None else prev_dimension.is_virtual,
        ):
            logger.warning("Dimension %s.%s.%s is not allowed to create.", tenant_id, model_name, name)
            return None
        await self.data_repository.create_not_virtual_dimensions(
            tenant_id=tenant_id,
            model_names=[model_name],
            generate_on_db=generate_on_db,
            check_possible_delete=not enable_delete_not_empty,
        )
        await self.create_dimension_attributes_in_pvd(
            tenant_id, dimension.attributes if dimension.attributes else prev_dimension.attributes
        )
        result = await self.data_repository.update_by_name_and_schema(
            tenant_id=tenant_id,
            name=name,
            model_name=model_name,
            dimension=dimension,
            generate_on_db=generate_on_db,
            enable_delete_not_empty=enable_delete_not_empty,
        )
        result_models_names = [model_status.name for model_status in result.models_statuses]
        for dimension_model in result_models_names:
            await CacheRepository.clear_dimensions_cache_by_model_name(tenant_id=tenant_id, model_name=dimension_model)
        await CacheRepository.clear_dimension_cache_by_name(tenant_id=tenant_id, name=name)
        await self.clear_data_storage_cache(
            tenant_id=tenant_id, model_name="*", dimension=prev_dimension, dimension_edit=dimension
        )
        if not generate_on_db:
            await self.worker_manager_client.create_dimension(
                tenant_id,
                [model_name],
                [DataStorageLogsFieldEnum.OPERATION],
                if_not_exists=True,
            )
            await self.worker_manager_client.update_dimension(
                tenant_id,
                [model_status.name for model_status in result.models_statuses],
                [name],
                enable_delete_column,
                enable_delete_not_empty,
            )
        _ = await self.events_processor.change_dictionary_structure(
            tenant_id, model_name, ChangeDictionaryStuctureActionsEnum.UPDATE, result
        )
        if send_to_aor:
            await self.create_and_send_command_to_aor_by_dimension(tenant_id, result)
        return result

    async def clear_data_storage_cache(
        self,
        tenant_id: str,
        model_name: str,
        dimension: Dimension,
        dimension_edit: Optional[DimensionEditRequest] = None,
    ) -> None:
        """Очищает кэш у привязанных к dimension data storage, если они были изменены или удалены."""
        dimension_model_names = [model_status.name for model_status in dimension.models_statuses]
        text_dso = dimension.text_table_name
        attributes_dso = dimension.attributes_table_name
        values_dso: Optional[str | int] = dimension.values_table_name
        if isinstance(text_dso, int) or isinstance(attributes_dso, int) or isinstance(values_dso, int):
            text_dso = None
            attributes_dso = None
            values_dso = None
        # Очищаем DataStorage, если удалили Dimension
        if not dimension_edit and (text_dso or attributes_dso or values_dso):
            for dimension_model in dimension_model_names:
                await CacheRepository.clear_data_storages_cache_by_model_name(
                    tenant_id=tenant_id, model_name=dimension_model
                )
            await CacheRepository.clear_data_storage_cache_by_name_and_model_name(
                tenant_id=tenant_id, name=text_dso, model_name=model_name
            )
            await CacheRepository.clear_data_storage_cache_by_name_and_model_name(
                tenant_id=tenant_id, name=attributes_dso, model_name=model_name
            )
            await CacheRepository.clear_data_storage_cache_by_name_and_model_name(
                tenant_id=tenant_id, name=values_dso, model_name=model_name
            )
        # Очищаем DataStorage attribute, если было изменение в атрибутах
        if getattr(dimension_edit, "attributes", None) is not None:
            for dimension_model in dimension_model_names:
                await CacheRepository.clear_data_storages_cache_by_model_name(
                    tenant_id=tenant_id, model_name=dimension_model
                )
            await CacheRepository.clear_data_storage_cache_by_name_and_model_name(
                tenant_id=tenant_id, name=attributes_dso, model_name=model_name
            )
        # Очищаем DataStorage texts, если было изменение в текстах
        if getattr(dimension_edit, "texts", None) is not None:
            for dimension_model in dimension_model_names:
                await CacheRepository.clear_data_storages_cache_by_model_name(
                    tenant_id=tenant_id, model_name=dimension_model
                )
            await CacheRepository.clear_data_storage_cache_by_name_and_model_name(
                tenant_id=tenant_id, name=text_dso, model_name=model_name
            )
        # Очищаем DataStorage values, если было изменение в DimensionRef
        # Случай если не было dimensionRef и он появился.
        if (
            dimension_edit
            and "dimension_id" in dimension_edit.model_fields_set
            and values_dso is not None
            and dimension_edit.dimension_id is not None
        ):
            for dimension_model in dimension_model_names:
                await CacheRepository.clear_data_storages_cache_by_model_name(
                    tenant_id=tenant_id, model_name=dimension_model
                )
            await CacheRepository.clear_data_storage_cache_by_name_and_model_name(
                tenant_id=tenant_id, name=values_dso, model_name=model_name
            )
        # Случай если был dimensionRef и его убрали.
        if (
            dimension_edit
            and "dimension_id" in dimension_edit.model_fields_set
            and dimension_edit.dimension_id is None
            and dimension.dimension_name is not None
        ):
            for dimension_model in dimension_model_names:
                await CacheRepository.clear_data_storages_cache_by_model_name(
                    tenant_id=tenant_id, model_name=dimension_model
                )

    async def get_updated_fields(
        self, tenant_id: str, model_name: str, name: str, dimension: DimensionEditRequest
    ) -> dict:
        """Получить поля, которые были изменены"""
        original_dimension = await self.get_dimension_by_dimension_name(
            tenant_id=tenant_id, name=name, model_name=model_name
        )
        original_dimension = original_dimension.model_dump(mode="json", by_alias=True)
        dimension_dict = dimension.model_dump(mode="json", by_alias=True, exclude_unset=True)
        return get_updated_fields_object(original_dimension, dimension_dict)

    @staticmethod
    async def _clear_dimension_cache(tenant_id: str, dimension: Dimension, name: str) -> None:
        """
        Очищает кэшированные данные измерения (`Dimension`) для указанного арендатора.

        Args:
            tenant_id (str): Идентификатор арендатора.
            dimension (Dimension): Измерение, чей кэш очищается.
            name (str): Имя измерения, используемое для идентификации в кэше.

        Returns:
            None
        """
        if dimension.text_table_name:
            await CacheRepository.clear_data_storages_cache_by_name(tenant_id=tenant_id, name=dimension.text_table_name)
        if dimension.attributes_table_name:
            await CacheRepository.clear_data_storages_cache_by_name(
                tenant_id=tenant_id, name=dimension.attributes_table_name
            )
        if dimension.values_table_name:
            await CacheRepository.clear_data_storages_cache_by_name(
                tenant_id=tenant_id, name=dimension.values_table_name
            )

        await CacheRepository.clear_dimension_cache_by_name(tenant_id=tenant_id, name=name)

    async def _clear_dimension_cache_after_coping(
        self,
        copied_dimension: Dimension,
        prev_dimension: Dimension,
        tenant_id: str,
        destination_models: list[str],
        cleared_models_set: set[str],
        cleared_dimension_set: set[str],
    ) -> None:
        """
        Очищает кэш измерений после процедуры копирования.

        Args:
            copied_dimension (Dimension): Скопированное измерение.
            prev_dimension (Dimension): Предыдущее (исходное) измерение.
            name (str): Имя измерения.
            tenant_id (str): Идентификатор арендатора.
            destination_models (list[str]): Список моделей назначения, в которые производилось копирование.

        Returns:
            None
        """
        models_for_clear_cache = destination_models.copy()
        prev_models_names = [model_status.name for model_status in prev_dimension.models_statuses]
        copied_dimension_models_names = [model_status.name for model_status in copied_dimension.models_statuses]
        models_for_clear_cache.extend(prev_models_names)
        if prev_models_names != copied_dimension_models_names:
            for model_name in models_for_clear_cache:
                if model_name not in cleared_models_set:
                    cleared_models_set.add(model_name)
                    await CacheRepository.clear_dimensions_cache_by_model_name(
                        tenant_id=tenant_id, model_name=model_name
                    )
                    await CacheRepository.clear_data_storages_cache_by_model_name(
                        tenant_id=tenant_id, model_name=model_name
                    )
            if copied_dimension.name not in cleared_dimension_set:
                cleared_dimension_set.add(copied_dimension.name)
                await self._clear_dimension_cache(
                    tenant_id=tenant_id, dimension=copied_dimension, name=copied_dimension.name
                )

    async def copy_model_dimension(
        self,
        tenant_id: str,
        name: str,
        model_names: list[str],
        copy_attributes: bool,
        generate_on_db: bool = True,
        if_not_exists: bool = False,
        check_possible_delete: bool = True,
    ) -> tuple[list[Dimension], dict[str, str], dict[str, str]]:
        """
        Копирует измерение (`Dimension`) в указанные модели с возможностью выбора копирования атрибутов.

        Args:
            tenant_id (str): Идентификатор арендатора.
            name (str): Имя исходного измерения (`dimension`), которое нужно скопировать.
            model_names (list[str]): Список названий моделей, куда будет выполнено копирование.
            copy_attributes (bool): Флаг, указывающий, нужно ли копировать атрибуты измерения.

        Returns:
            Dimension: Новый экземпляр измерения после успешного копирования.
        """
        prev_dimension: Dimension = await self.get_dimension_by_dimension_name(
            tenant_id=tenant_id, model_name=None, name=name
        )
        not_ignored_models: list[str] = []
        for model_name in model_names:
            if is_ignore_dimension(model_name, name, prev_dimension.is_virtual):
                logger.warning("You can't copy a dimension %s to a model %s.", model_name, name)
                continue
            not_ignored_models.append(model_name)
        if not not_ignored_models:
            return [], {}, {}
        result, not_copied_dimensions, not_copied_measures = await self.data_repository.copy_model_dimension(
            tenant_id=tenant_id,
            name=name,
            model_names=not_ignored_models,
            copy_attributes=copy_attributes,
            generate_on_db=generate_on_db,
            if_not_exists=if_not_exists,
            check_possible_delete=check_possible_delete,
        )
        if not not_copied_dimensions and not not_copied_measures:
            cleared_models_set: set[str] = set()
            cleared_dimensions_set: set[str] = set()
            for dim in result:
                await self._clear_dimension_cache_after_coping(
                    copied_dimension=dim,
                    prev_dimension=prev_dimension,
                    tenant_id=tenant_id,
                    destination_models=not_ignored_models,
                    cleared_models_set=cleared_models_set,
                    cleared_dimension_set=cleared_dimensions_set,
                )
        return result, not_copied_dimensions, not_copied_measures

    async def copy_model_dimensions(
        self,
        tenant_id: str,
        model_names: list[str],
        names: list[str],
        copy_attributes: bool = True,
        generate_on_db: bool = True,
        if_not_exists: bool = False,
        send_to_aor: bool = True,
        check_possible_delete: bool = True,
        raise_if_error: bool = False,
    ) -> tuple[DetailsObjectCopyReponse, bool]:
        """
        Копирует несколько измерений (`Dimensions`) в указанные модели с выбором копирования атрибутов.

        Args:
            tenant_id (str): Идентификатор арендатора.
            model_names (list[str]): Список названий моделей, куда будут скопированы измерения.
            names (list[str]): Список имен измерений, подлежащих копированию.
            copy_attributes (bool): Флаг, определяющий необходимость копирования атрибутов измерений.

        Returns:
            tuple[list[ObjectCopyResponse], bool]: Кортеж, содержащий:
                - Список результатов копирования (`ObjectCopyResponse`) для каждого измерения.
                - Булевый флаг, показывающий успешность всей операции.
        """

        await self.data_repository.create_not_virtual_dimensions(
            tenant_id=tenant_id,
            model_names=model_names,
            generate_on_db=generate_on_db,
            check_possible_delete=check_possible_delete,
        )
        response: list[ObjectCopyResponse] = []
        result: dict[str, list[tuple]] = defaultdict(list)
        has_error = False
        copied_dimensions: list[Dimension] = []
        ignored: dict[str, list] = defaultdict(list)
        for model_name in model_names:
            for name in names:
                try:
                    copied_dimensions = []
                    copied_dimensions, not_copied_dimensions, not_copied_measures = await self.copy_model_dimension(
                        tenant_id,
                        name,
                        [model_name],
                        copy_attributes=copy_attributes,
                        generate_on_db=generate_on_db,
                        if_not_exists=if_not_exists,
                        check_possible_delete=check_possible_delete,
                    )
                    if not_copied_dimensions or not_copied_measures:
                        for not_copied_dimension_name, not_copied_dimension_msg in not_copied_dimensions.items():
                            result[not_copied_dimension_name].append((model_name, not_copied_dimension_msg))
                        for not_copied_measure_name, not_copied_measure_msg in not_copied_measures.items():
                            result[not_copied_measure_name].append((model_name, not_copied_measure_msg))
                        raise ValueError(
                            f"There are related entities that cannot be copied. Not copied dimensions = {list(not_copied_dimensions.keys())},"
                            + f" not copied measures = {list(not_copied_measures.keys())}"
                        )
                    if not copied_dimensions and not not_copied_dimensions and not not_copied_measures:
                        ignored[name].append(model_name)
                    else:
                        result[name].append((model_name, None))
                        for copied_dimension in copied_dimensions:
                            result[copied_dimension.name].append((model_name, None))
                except Exception as exc:  # noqa
                    logger.exception("Error copy %s.%s dimension to models %s", tenant_id, name, [model_name])
                    has_error = True
                    result[name].append((model_name, str(exc)))
                    for copied_dimension in copied_dimensions:
                        result[copied_dimension.name].append(
                            (
                                model_name,
                                "It can be copied, but the copy is ignored due to errors in other objects in model",
                            )
                        )
        for object_name, status_copy in result.items():
            success_models = []
            for status in status_copy:
                if status[1] is None:
                    success_models.append(status[0])
                else:
                    response.append(
                        ObjectCopyResponse(
                            tenant_id=tenant_id,
                            object_name=object_name,
                            result=ObjectCopyResult.FAILURE,
                            msg=status[1],
                            models=[status[0]],
                        )
                    )
            if success_models:
                response.append(
                    ObjectCopyResponse(
                        tenant_id=tenant_id,
                        object_name=object_name,
                        result=ObjectCopyResult.SUCCESS,
                        models=list(set(success_models)),
                        msg=None,
                    )
                )
                if send_to_aor:
                    await self.send_to_aor_by_name(tenant_id, object_name)
        for ignored_dimension, models in ignored.items():
            response.append(
                ObjectCopyResponse(
                    tenant_id=tenant_id,
                    object_name=ignored_dimension,
                    result=ObjectCopyResult.IGNORED,
                    models=models,
                    msg=None,
                )
            )
        logger.debug("Copied status: %s", response)
        dimensions_for_create = list(
            {
                dimension_to_create.object_name
                for dimension_to_create in response
                if dimension_to_create.result == ObjectCopyResult.SUCCESS
            }
        )
        if not generate_on_db and dimensions_for_create:
            await self.worker_manager_client.create_dimension(
                tenant_id,
                model_names,
                [DataStorageLogsFieldEnum.OPERATION],
                if_not_exists=True,
            )
            await self.worker_manager_client.create_dimension(
                tenant_id,
                model_names,
                dimensions_for_create,
                if_not_exists=if_not_exists,
            )
        if raise_if_error and has_error:
            raise Exception(f"Error copy dimensions: {response}")
        return DetailsObjectCopyReponse(detail=response), has_error

    def __repr__(self) -> str:
        return "DataStorageService"

    async def create_pv_dictionary_by_dimension(
        self,
        tenant_id: str,
        name: str,
        pv_dictionary: Optional[PVDictionary] = None,
        with_error: bool = True,
        commit: bool = False,
    ) -> Dimension:
        """Добавить PV Dictionary в dimension."""
        result: Dimension = await self.data_repository.create_pv_dictionary_by_dimension(
            tenant_id,
            name,
            pv_dictionary,
            with_error,
            commit,
        )
        if result:
            for model_status in result.models_statuses:
                await CacheRepository.clear_dimensions_cache_by_model_name(
                    tenant_id=tenant_id, model_name=model_status.name
                )
            await CacheRepository.clear_dimension_cache_by_name(tenant_id=tenant_id, name=result.name)
            await self.clear_data_storage_cache(
                tenant_id=tenant_id,
                model_name="*",
                dimension=result,
            )
        return result

    async def update_pv_dictionary_by_dimension(
        self,
        tenant_id: str,
        name: str,
        pv_dictionary: Optional[PVDictionary] = None,
    ) -> Dimension:
        """Обновить PV Dictionary в dimension."""
        result = await self.data_repository.update_pv_dictionary_by_dimension(
            tenant_id,
            name,
            pv_dictionary,
        )
        return result

    def convert_dimension_df_row_to_pydantic(self, row: Series) -> DimensionCreateRequest:
        """Конвертирует строку DataFrame в DimensionCreateRequest."""
        try:
            texts = []
            for text_type in (
                (DimensionsFileColumnEnum.SHORT_TEXT, TextEnum.SHORT),
                (DimensionsFileColumnEnum.MEDIUM_TEXT, TextEnum.MEDIUM),
                (DimensionsFileColumnEnum.LONG_TEXT, TextEnum.LONG),
            ):
                if row[text_type[0]]:
                    texts.append(text_type[1])
            dimension_for_create = DimensionCreateRequest(
                name=row[DimensionsFileColumnEnum.NAME],
                labels=labels_by_row(row, DimensionsFileColumnEnum),
                dimension_id=row[DimensionsFileColumnEnum.REF],
                precision=row[DimensionsFileColumnEnum.LENGTH],
                type=row[DimensionsFileColumnEnum.DATA_TYPE],
                is_virtual=row[DimensionsFileColumnEnum.VIRTUAL],
                texts_time_dependency=row[DimensionsFileColumnEnum.TEXT_TIME_DEPENDENCY],
                texts_language_dependency=row[DimensionsFileColumnEnum.TEXT_LANG_DEPENDENCY],
                auth_relevant=row[DimensionsFileColumnEnum.AUTH_RELEVANT],
                texts=texts,
                case_sensitive=row[DimensionsFileColumnEnum.CASE_SENSITIVE],
            )
        except Exception as err:
            logger.exception("Parsing Error:")
            raise ValueError(str(err) + ", Row: " + str(list(row)))
        return dimension_for_create

    def convert_dimension_attribute_df_row_to_pydantic(self, row: Series) -> DimensionAttributeRequest:
        """Конвертирует строку DataFrame в DimensionAttributeRequest."""
        try:
            if not row[DimensionAttributesFileColumnEnum.REF]:
                any_field = AnyField(
                    name=row[DimensionAttributesFileColumnEnum.NAME],
                    precision=row[DimensionAttributesFileColumnEnum.LENGTH],
                    scale=row[DimensionAttributesFileColumnEnum.SCALE],
                    type=row[DimensionAttributesFileColumnEnum.DATA_TYPE],
                    aggregation_type=row[DimensionAttributesFileColumnEnum.AGGREGATION_TYPE],
                    labels=[],
                )
                ref_type = BaseFieldType(
                    ref_object=any_field,
                    ref_object_type=BaseFieldTypeEnum.ANYFIELD,
                )
            else:
                ref_type = BaseFieldType(
                    ref_object=row[DimensionAttributesFileColumnEnum.REF],
                    ref_object_type=row[DimensionAttributesFileColumnEnum.SEMANTIC_TYPE],
                )

            attribute_for_create = DimensionAttributeRequest(
                name=row[DimensionAttributesFileColumnEnum.NAME],
                time_dependency=row[DimensionAttributesFileColumnEnum.TIME_DEPENDENCY],
                labels=labels_by_row(row, DimensionAttributesFileColumnEnum),
                semantic_type=row[DimensionAttributesFileColumnEnum.SEMANTIC_TYPE],
                ref_type=ref_type,
            )
        except Exception as err:
            logger.exception("Parsing Error:")
            raise ValueError(str(err) + ", Row: " + str(list(row)))
        return attribute_for_create

    async def try_create_dimensions_while_not_all_errors_or_created(
        self, tenant_id: str, model_name: str, dimensions: dict[str, DimensionCreateRequest]
    ) -> dict[str, list[str]]:
        """
        Создание нескольких dimension.
        Выполняется следующий алгоритм:
        1) Выбираем из словаря очередной dimension и пытаемся создать его.
        2) В случае ошибки переходим к следующему Dimension в словаре.
        3) После прохождения по всему словарю удаляем из него уже созданные dimension.
        4) Если предыдущее общее количество созданных dimension != текущему общему количеству
        созданных dimension после этой итерации, то возвращаемся к П.1.
        5) Если предыдущее общее количество созданных dimension == текущему общему количеству
        созданных dimension после этой итерации, то возвращаем результат работы функции.
        """
        if not dimensions:
            return {"created": [], "not_created": []}
        return_created = set()
        return_not_created = set()
        exists_dimensions = await self.data_repository.get_list_by_tenant(tenant_id)
        exists = set()
        for exist_dimension in exists_dimensions:
            exists.add(exist_dimension.name)
        created: list[str] = []
        prev_created_len = -1
        curr_created_len = 0
        while curr_created_len != prev_created_len:
            created = []
            for dimension_name, dimension in dimensions.items():
                if dimension_name in exists:
                    return_not_created.add(dimension_name)
                    logger.debug("Dimension %s already exist.", dimension_name)
                    continue
                logger.debug("Try create %s.", dimension_name)
                try:
                    _ = await self.create_dimension_by_schema(tenant_id, model_name, dimension)
                    logger.debug("Dimension %s created.", dimension_name)
                    return_created.add(dimension_name)
                    created.append(dimension_name)
                except Exception:  # noqa
                    logger.exception("Create dimension %s failed.", dimension_name)
                    return_not_created.add(dimension_name)
            for created_dimension in created:
                dimensions.pop(created_dimension)
            prev_created_len = curr_created_len
            curr_created_len = len(return_created)
        logger.debug("Stop creating dimension.")
        return {"created": list(return_created), "not_created": list(return_not_created - return_created)}

    async def try_update_attributes_dimension_while_not_all_errors_or_updated(
        self, tenant_id: str, model_name: str, dimensions_edited: dict[str, DimensionEditRequest]
    ) -> dict[str, list[str]]:
        """
        Обновление нескольких dimension.
        Выполняется следующий алгоритм:
        1) Выбираем из словаря очередной dimension и пытаемся обновить его.
        2) В случае ошибки переходим к следующему Dimension в словаре.
        3) После прохождения по всему словарю удаляем из него уже обновленные dimension.
        4) Если предыдущее общее количество обновленных dimension != текущему общему количеству
        обновленных dimension после этой итерации, то возвращаемся к П.1.
        5) Если предыдущее общее количество обновленных dimension == текущему общему количеству
        обновленных dimension после этой итерации, то возвращаем результат работы функции.
        """
        return_updated = set()
        return_not_updated = set()
        updated: list[str] = []
        prev_updated_len = -1
        curr_updated_len = 0
        while prev_updated_len != curr_updated_len:
            updated = []
            for dimension_name, dimension_update_body in dimensions_edited.items():
                logger.debug("Try update %s.", dimension_name)
                try:
                    _ = await self.update_dimension_by_name_and_schema(
                        tenant_id, model_name, dimension_name, dimension_update_body
                    )
                    logger.debug("Dimension %s updated.", dimension_name)
                    return_updated.add(dimension_name)
                    updated.append(dimension_name)
                except Exception:  # noqa
                    logger.exception("Update dimension %s failed.", dimension_name)
                    return_not_updated.add(dimension_name)
            for updated_dimension in updated:
                dimensions_edited.pop(updated_dimension)
            prev_updated_len = curr_updated_len
            curr_updated_len = len(return_updated)
        logger.debug("Stop updating dimension.")
        return {"updated": list(return_updated), "not_updated": list(return_not_updated - return_updated)}

    async def create_dimensions_by_files(
        self,
        tenant_id: str,
        model_name: str,
        dimensions_file: Optional[UploadFile] = None,
        attributes_file: Optional[UploadFile] = None,
    ) -> ImportFromFileResponse:
        """Массовый импорт измерений."""
        await self.data_repository.create_not_virtual_dimensions(tenant_id=tenant_id, model_names=[model_name])
        created_dimensions = {}
        updated_dimensions = {}
        if dimensions_file:
            dimensions_df = read_upload_file_as_dataframe(
                dimensions_file,
                DIMENSIONS_FILE_COLUMNS,
                DIMENSIONS_FILE_COLUMNS_TYPE,
            )
            dimensions_create = {}
            for _, row in dimensions_df.iterrows():
                dimensions_create[row[DimensionsFileColumnEnum.NAME]] = self.convert_dimension_df_row_to_pydantic(row)
            created_dimensions = await self.try_create_dimensions_while_not_all_errors_or_created(
                tenant_id, model_name, dimensions_create
            )
        if attributes_file:
            attributes_df = read_upload_file_as_dataframe(
                attributes_file,
                DIMENSION_ATTRIBUTES_FILE_COLUMNS,
                DIMENSIONS_ATTRIBUTES_FILE_COLUMNS_TYPE,
            )
            dimensions_edited = {}
            for _, row in attributes_df.iterrows():
                if row[DimensionAttributesFileColumnEnum.DIMENSION_NAME] not in dimensions_edited:
                    dimensions_edited[row[DimensionAttributesFileColumnEnum.DIMENSION_NAME]] = DimensionEditRequest(
                        attributes=[]
                    )
                dimension_edited = dimensions_edited[row[DimensionAttributesFileColumnEnum.DIMENSION_NAME]]
                if dimension_edited.attributes is not None:
                    dimension_edited.attributes.append(self.convert_dimension_attribute_df_row_to_pydantic(row))
                else:
                    raise ValueError("dimension_edited.attributes not must be null.")
            updated_dimensions = await self.try_update_attributes_dimension_while_not_all_errors_or_updated(
                tenant_id, model_name, dimensions_edited
            )
        if updated_dimensions.get("updated") or created_dimensions.get("created"):
            await CacheRepository.clear_dimensions_cache_by_model_name(tenant_id=tenant_id, model_name=model_name)
            await CacheRepository.clear_data_storages_cache_by_model_name(tenant_id=tenant_id, model_name=model_name)
            if updated_dimensions.get("updated"):
                dimensions: list[Dimension] = await self.get_dimension_list_by_names(
                    tenant_id, model_name, updated_dimensions.get("updated")
                )
                for dimension in dimensions:
                    await CacheRepository.clear_dimension_cache_by_name(tenant_id=tenant_id, name=dimension.name)
                    await self.clear_data_storage_cache(tenant_id, "*", dimension)

        return ImportFromFileResponse(
            created=created_dimensions.get("created", []),
            not_created=created_dimensions.get("not_created", []),
            updated=updated_dimensions.get("updated", []),
            not_updated=updated_dimensions.get("not_updated", []),
        )

    async def create_and_send_command_to_aor_by_dimension(
        self,
        tenant_id: str,
        dimension: Dimension,
        deleted: bool = False,
        custom_uuid: Optional[UUID] = None,
        with_parents: bool = True,
        send_command: bool = True,
        dim_with_attributes: bool = True,
        depends_no_attrs_versions: bool = False,
        version_suffix: str = "",
        parent_version_suffix: str = "",
        name_suffix: str = "",
        parent_name_suffix: str = "",
    ) -> Optional[CreateAorCommand]:
        try:
            if not dim_with_attributes:
                dimension.attributes = []
                if not version_suffix:
                    version_suffix += "-no-attrs"
            data_json = JsonData(
                is_deleted=deleted, tenant=tenant_id, data_json=dimension.model_dump(mode="json", by_alias=True)
            )
            parents = (
                await self.aor_repository.get_dimension_parents_by_schema(tenant_id, dimension) if with_parents else []
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
                "type": AorType.DIMENSION,
                "name": dimension.name + name_suffix,
                "data_json": data_json,
                "description": dimension.name,
                "version": (
                    str(dimension.version) + version_suffix
                    if not deleted
                    else f"{dimension.version}-deleted" + version_suffix
                ),
                "external_object_id": dimension.name + name_suffix,
                "deployed_by": get_user_login_by_token(context.get(AuthorizationPlugin.key) or None),
                "parents": parents,
                "space_id": custom_uuid
                or (await self.aor_repository.get_dimension_aor_space_by_names(tenant_id, [dimension.name])).get(
                    dimension.name
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
        dimension = await self.get_dimension_by_dimension_name(tenant_id, name, model_name=None)
        await self.create_and_send_command_to_aor_by_dimension(
            tenant_id,
            dimension,
            deleted,
            custom_uuid,
            with_parents,
            dim_with_attributes=dim_with_attributes,
            depends_no_attrs_versions=depends_no_attrs_versions,
            version_suffix=version_suffix,
            parent_version_suffix=parent_version_suffix,
            name_suffix=name_suffix,
            parent_name_suffix=parent_name_suffix,
        )
