"""
Сервис DSO.
Тут реализована вся логика работы сервиса.
Работа с БД инкапсулирована через DataRepository.
"""

from copy import deepcopy
from typing import Any, Optional, cast
from uuid import UUID

from fastapi import UploadFile
from pandas import Series
from py_common_lib.logger import EPMPYLogger
from py_common_lib.starlette_context_plugins import AuthorizationPlugin
from sqlalchemy import select
from sqlalchemy.exc import NoResultFound
from starlette_context import context

from src.config import settings
from src.db.data_storage import DataStorage as DataStorageORM
from src.db.database_object import DatabaseObject as DatabaseObjectORM
from src.db.model import Model as ModelORM
from src.integration.aor.client import ClientAOR
from src.integration.aor.model import AorKafkaObjectParent, AorType, CreateAorCommand, JsonData, PushAorCommand
from src.integration.worker_manager import ClientWorkerManager
from src.models.any_field import AnyField
from src.models.aor import CreateModelAorRequest
from src.models.consts import (
    DATA_STORAGE_FIELDS_FILE_COLUMN,
    DATA_STORAGE_FIELDS_FILE_COLUMNS_TYPE,
    DATA_STORAGE_FILE_COLUMNS,
    DATA_STORAGE_FILE_COLUMNS_TYPE,
)
from src.models.copy_model import DetailsObjectCopyReponse, ObjectCopyResponse, ObjectCopyResult
from src.models.data_storage import (
    DataStorage,
    DataStorageCreateRequest,
    DataStorageEditRequest,
    DataStorageFieldRequest,
    DataStorageLogsFieldEnum,
)
from src.models.database import Database as DatabaseModel, DatabaseTypeEnum
from src.models.database_object import DatabaseObjectRelationTypeEnum, DatabaseObjectRequest
from src.models.field import BaseFieldType, BaseFieldTypeEnum, SemanticType
from src.models.model import Model
from src.models.model_import import ImportFromFileResponse
from src.models.request_params import DataStorageFieldsFileColumnEnum, DataStorageFileColumnEnum, Pagination
from src.models.tenant import SemanticObjectsTypeEnum
from src.repository.aor import AorRepository
from src.repository.cache import CacheRepository
from src.repository.data_storage import DataStorageRepository
from src.repository.database_object import DatabaseObjectRepository
from src.repository.database_object_relations import DatabaseObjectRelationsRepository
from src.repository.dimension import DimensionRepository
from src.repository.generators.utils import get_generator
from src.repository.model import ModelRepository
from src.repository.model_relations import ModelRelationsRepository
from src.repository.utils import get_database_object_names, get_filtred_database_object_by_data_storage
from src.service.utils import get_updated_fields_object, labels_by_row, read_upload_file_as_dataframe
from src.utils.auth import get_user_login_by_token
from src.utils.backoff import RetryConfig, retry
from src.utils.view_parser import build_view_sql_expression, contains_sql_identifier, parse_view_ddl

logger = EPMPYLogger(__name__)


def _get_view_dependency_names(view_json: dict[str, Any]) -> set[str]:
    """Возвращает набор имен таблиц, от которых зависит VIEW."""
    dependency_names: set[str] = set()
    for dependency in view_json.get("dependencies") or []:
        if dependency.get("type") != "table":
            continue
        dependency_name = dependency.get("name")
        if dependency_name:
            dependency_names.add(dependency_name)
    return dependency_names


def _view_depends_on_tables(view_definition: str, table_names: list[str], dependency_names: set[str]) -> bool:
    """Проверяет зависимость VIEW от таблиц по точному имени."""
    if dependency_names:
        return any(table_name in dependency_names for table_name in table_names)
    return any(contains_sql_identifier(view_definition, table_name) for table_name in table_names)


class DataStorageService:
    def __init__(
        self,
        data_repository: DataStorageRepository,
        dimension_repository: DimensionRepository,
        model_relations_repo: ModelRelationsRepository,
        model_repository: ModelRepository,
        database_object_repository: DatabaseObjectRepository,
        database_object_relations_repository: DatabaseObjectRelationsRepository,
        worker_manager_client: ClientWorkerManager,
        aor_client: ClientAOR,
        aor_repository: AorRepository,
    ) -> None:
        self.model_relations_repo = model_relations_repo
        self.data_repository = data_repository
        self.dimension_repository = dimension_repository
        self.model_repository = model_repository
        self.worker_manager_client = worker_manager_client
        self.aor_client = aor_client
        self.aor_repository = aor_repository
        self.database_object_repository = database_object_repository
        self.database_object_relations_repository = database_object_relations_repository

    @retry(RetryConfig())
    async def get_data_storage_by_name(
        self,
        tenant_id: str,
        model_name: Optional[str],
        name: str,
        change_name: bool = True,
    ) -> Optional[DataStorage]:
        """Получить DSO по имени."""
        data_storage = await self.data_repository.get_by_name(tenant_id=tenant_id, model_name=model_name, name=name)
        return data_storage

    async def get_data_storage_by_name_or_null(
        self,
        tenant_id: str,
        model_name: Optional[str],
        name: str,
        change_name: bool = True,
    ) -> Optional[DataStorage]:
        """Получить DSO по имени."""
        try:
            return await self.get_data_storage_by_name(tenant_id=tenant_id, model_name=model_name, name=name)
        except NoResultFound:
            return None

    async def get_orm_by_name(self, tenant_id: str, model_name: Optional[str], name: str) -> DataStorageORM:
        """
        Получает объект ORM хранилища данных по его имени и идентификатору арендатора.

        Args:
            tenant_id (str): Идентификатор арендатора, к которому относится хранилище данных.
            model_name (Optional[str]): Необязательное имя модели, связанной с хранилищем данных.
            name (str): Имя хранилища данных, по которому оно должно быть найдено.

        Returns:
            DataStorageORM: Объект ORM хранилища данных, соответствующий указанным параметрам.
        """

        return await self.data_repository.get_orm_by_name(tenant_id, model_name, name)

    @retry(RetryConfig())
    async def get_data_storage_by_db_object(
        self,
        tenant_id: str,
        model_name: str,
        db_object: DatabaseObjectRequest,
        change_name: bool = True,
    ) -> Optional[DataStorage]:
        """Получить DSO по объекту в бд."""
        data_storage = await self.data_repository.get_datastorage_by_db_object(
            tenant_id=tenant_id, model_name=model_name, db_object=db_object
        )
        return data_storage

    @retry(RetryConfig())
    async def get_data_storage_list_by_model_name(
        self,
        tenant_id: str,
        model_name: str,
        change_name: bool = True,
        pagination: Optional[Pagination] = None,
    ) -> list[DataStorage]:
        """Получить список всех DSO."""
        data_storages = await self.data_repository.get_list(
            tenant_id=tenant_id,
            model_name=model_name,
            pagination=pagination,
        )
        return data_storages

    async def create_data_storage_by_schema(
        self,
        tenant_id: str,
        model_name: str,
        data_storage: DataStorageCreateRequest,
        if_not_exists: bool = False,
        generate_on_db: bool = True,
        send_to_aor: bool = True,
        check_possible_delete: bool = True,
    ) -> DataStorage:
        """
        Создает новое хранилище данных на основе переданного запроса.

        Args:
            tenant_id (str): Идентификатор арендатора, к которому привязывается хранилище данных.
            model_name (str): Имя модели, к которой привязывается хранилище данных.
            data_storage (DataStorageCreateRequest): Запрос на создание нового хранилища данных.
            generate_physical_if_not_exists (bool, optional): Флаг, определяющий необходимость автоматического создания физического представления хранилища, если оно еще не создано. По умолчанию False.

        Returns:
            DataStorage: Новый объект хранилища данных.
        """
        await self.dimension_repository.create_not_virtual_dimensions(
            tenant_id,
            model_names=[model_name],
            generate_on_db=generate_on_db,
            check_possible_delete=check_possible_delete,
        )
        result = await self.data_repository.create_and_get_by_schema(
            tenant_id=tenant_id,
            model_name=model_name,
            data_storage=data_storage,
            generate_physical_if_not_exists=if_not_exists,
            generate_on_db=generate_on_db,
            check_possible_delete=check_possible_delete,
        )
        await CacheRepository.clear_data_storages_cache_by_model_name(tenant_id=tenant_id, model_name=model_name)
        if not generate_on_db:
            await self.worker_manager_client.create_dimension(
                tenant_id,
                [model_name],
                [DataStorageLogsFieldEnum.OPERATION],
                if_not_exists=True,
            )
            await self.worker_manager_client.create_data_storage(
                tenant_id, [model_name], [data_storage.name], if_not_exists
            )
        if send_to_aor:
            await self.create_and_send_command_to_aor_by_datastorage(tenant_id, result)
        return result

    async def create_data_storage_by_schema_if_not_exists(
        self,
        tenant_id: str,
        model_name: str,
        data_storage: DataStorageCreateRequest,
        generate_physical_if_not_exists: bool = False,
        check_possible_delete: bool = True,
    ) -> DataStorageORM:
        """
        Создает хранилище данных на основе переданного запроса, если такое хранилище еще не существует.

        Args:
            tenant_id (str): Идентификатор арендатора, к которому привязывается хранилище данных.
            model_name (str): Имя модели, к которой привязывается хранилище данных.
            data_storage (DataStorageCreateRequest): Запрос на создание нового хранилища данных.
            generate_physical_if_not_exists (bool, optional): Флаг, определяющий необходимость автоматического создания физического представления хранилища, если оно еще не создано. По умолчанию False.

        Returns:
            DataStorageORM: Объект ORM хранилища данных, созданный или существующий ранее.
        """
        try:
            existing_ds = await self.get_orm_by_name(tenant_id=tenant_id, name=data_storage.name, model_name=None)
        except NoResultFound:
            _, _, data_storage_orm = await self.data_repository.create_by_schema(
                tenant_id=tenant_id,
                model_name=model_name,
                data_storage=data_storage,
                generate_physical_if_not_exists=generate_physical_if_not_exists,
                commit_changes=False,
                check_possible_delete=check_possible_delete,
            )
            return data_storage_orm
        else:
            return existing_ds

    async def delete_data_storage_by_name(
        self,
        tenant_id: str,
        model_name: str,
        name: str,
        if_exists: bool = False,
        send_to_aor: bool = True,
        check_possible_delete: bool = True,
    ) -> None:
        """Удалить DSO."""
        try:
            dso_related_objects = await self.model_relations_repo.get_datastorage_related_objects(
                tenant_name=tenant_id, model_name=model_name, object_name=name
            )
            dso_related_objects.raise_if_not_empty()
        except NoResultFound:
            logger.info(
                "Not found related objects for datastorage %s.%s.%s. \nOr there are no such datastorage in database.",
                tenant_id,
                model_name,
                name,
            )
        datastorage = await self.get_data_storage_by_name(tenant_id, None, name)
        if len(datastorage.models_statuses) <= 1:
            command = await self.create_and_send_command_to_aor_by_datastorage(
                tenant_id,
                datastorage,
                with_parents=False,
                send_command=False,
                deleted=True,
            )
        await self.data_repository.delete_by_name(
            tenant_id=tenant_id,
            model_name=model_name,
            name=name,
            if_exists=if_exists,
            check_possible_delete=check_possible_delete,
        )
        if len(datastorage.models_statuses) > 1:
            datastorage = await self.get_data_storage_by_name(tenant_id, None, name)
            command = await self.create_and_send_command_to_aor_by_datastorage(
                tenant_id,
                datastorage,
                send_command=False,
            )
        await CacheRepository.clear_data_storages_cache_by_model_name(tenant_id=tenant_id, model_name=model_name)
        await CacheRepository.clear_data_storage_cache_by_name_and_model_name(
            tenant_id=tenant_id, model_name=model_name, name=name
        )
        if send_to_aor:
            await self.aor_client.send_request(command)
        return None

    async def update_data_storage_by_name_and_schema(
        self,
        tenant_id: str,
        model_name: str,
        name: str,
        data_storage: DataStorageEditRequest,
        generate_on_db: bool = True,
        enable_delete_column: bool = True,
        enable_delete_not_empty: bool = False,
        send_to_aor: bool = True,
    ) -> DataStorage:
        """Обновить DSO."""
        await self.dimension_repository.create_not_virtual_dimensions(
            tenant_id=tenant_id,
            model_names=[model_name],
            generate_on_db=generate_on_db,
            check_possible_delete=not enable_delete_not_empty,
        )
        result = await self.data_repository.update_by_name_and_schema(
            tenant_id=tenant_id,
            model_name=model_name,
            name=name,
            data_storage_edit_model=data_storage,
            generate_on_db=generate_on_db,
            enable_delete_not_empty=enable_delete_not_empty,
        )
        await CacheRepository.clear_data_storages_cache_by_model_name(tenant_id=tenant_id, model_name=model_name)
        await CacheRepository.clear_data_storage_cache_by_name_and_model_name(
            tenant_id=tenant_id, model_name=model_name, name=name
        )
        if not generate_on_db:
            await self.worker_manager_client.create_dimension(
                tenant_id,
                [model_name],
                [DataStorageLogsFieldEnum.OPERATION],
                if_not_exists=True,
            )
            await self.worker_manager_client.update_data_storage(
                tenant_id,
                [model_status.name for model_status in result.models_statuses],
                [name],
                enable_delete_column,
                enable_delete_not_empty,
            )
        if send_to_aor:
            await self.create_and_send_command_to_aor_by_datastorage(tenant_id, result)
        data_storage_with_models = await self.get_data_storage_by_name(tenant_id=tenant_id, model_name=None, name=name)
        model_names = {model_status.name for model_status in data_storage_with_models.models_statuses}
        for target_model_name in model_names:
            await self.collect_views_for_model(tenant_id, target_model_name, [result.name])
        return result

    async def get_updated_fields(
        self,
        tenant_id: str,
        model_name: Optional[str],
        name: str,
        data_storage: DataStorageEditRequest,
    ) -> dict:
        """Получить поля, которые были изменены"""
        original_data_storage = await self.get_data_storage_by_name(
            tenant_id=tenant_id, model_name=None, name=name, change_name=False
        )
        original_data_storage = original_data_storage.model_dump(mode="json", by_alias=True)
        data_storage_dict = data_storage.model_dump(mode="json", by_alias=True, exclude_unset=True)
        return get_updated_fields_object(original_data_storage, data_storage_dict)

    async def copy_model_data_storage(
        self,
        tenant_id: str,
        name: str,
        model_names: list[str],
        generated_in_db: bool = True,
        if_not_exist: bool = False,
        check_possible_delete: bool = True,
    ) -> DataStorage:
        """Обновить модель у data_storage."""
        prev_data_storage = await self.get_data_storage_by_name(tenant_id=tenant_id, model_name=None, name=name)
        result = await self.data_repository.copy_model_data_storage(
            tenant_id=tenant_id,
            name=name,
            model_names=model_names,
            generated_in_db=generated_in_db,
            if_not_exist=if_not_exist,
            check_possible_delete=check_possible_delete,
        )
        model_names_to_clear = model_names.copy()
        prev_models_names = [model_status.name for model_status in prev_data_storage.models_statuses]
        result_model_names = [model_status.name for model_status in result.models_statuses]
        model_names_to_clear.extend(prev_models_names)
        if prev_models_names != result_model_names:
            for model_name in model_names_to_clear:
                await CacheRepository.clear_data_storages_cache_by_model_name(
                    tenant_id=tenant_id, model_name=model_name
                )
            await CacheRepository.clear_data_storages_cache_by_name(tenant_id=tenant_id, name=name)
        if not generated_in_db:
            await self.worker_manager_client.create_dimension(
                tenant_id,
                model_names,
                [DataStorageLogsFieldEnum.OPERATION],
                if_not_exists=True,
            )
            await self.worker_manager_client.create_data_storage(
                tenant_id,
                model_names,
                [name],
                if_not_exists=if_not_exist,
            )
        return result

    async def copy_model_data_storages(
        self,
        tenant_id: str,
        model_names: list[str],
        names: list[str],
        generated_in_db: bool = True,
        if_not_exist: bool = False,
        send_to_aor: bool = True,
        check_possible_delete: bool = True,
        raise_if_error: bool = False,
    ) -> tuple[DetailsObjectCopyReponse, bool]:
        """Обновить модель у нескольких data_storage."""
        await self.dimension_repository.create_not_virtual_dimensions(
            tenant_id=tenant_id,
            model_names=model_names,
            generate_on_db=generated_in_db,
            check_possible_delete=check_possible_delete,
        )
        response = []
        has_error = False
        for name in names:
            try:
                _ = await self.copy_model_data_storage(
                    tenant_id,
                    name,
                    model_names,
                    generated_in_db=generated_in_db,
                    if_not_exist=if_not_exist,
                    check_possible_delete=check_possible_delete,
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
                logger.exception(
                    "Error copy %s.%s data_storage to models %s",
                    tenant_id,
                    name,
                    model_names,
                )
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

    def convert_data_storage_df_row_to_pydantic(self, row: Series) -> DataStorageCreateRequest:
        """
        Создание объекта DataStorageCreateRequest по записи из файла.

        Args:
            row (str): строка в DataFrame с DataStorage

        Returns:
            DataStorageCreateRequest: модель на создание DataStorage в pydantic.
        """
        try:
            datastorage_for_create = DataStorageCreateRequest(
                name=row[DataStorageFileColumnEnum.NAME],
                labels=labels_by_row(row, DataStorageFileColumnEnum),
                planning_enabled=row[DataStorageFileColumnEnum.PLAN],
                type=row[DataStorageFileColumnEnum.TYPE],
                fields=[
                    DataStorageFieldRequest(
                        name="empty",
                        ref_type=BaseFieldType(
                            ref_object="empty",
                            ref_object_type=BaseFieldTypeEnum.DIMENSION,
                        ),
                        semantic_type=SemanticType.DIMENSION,
                        sql_name="empty",
                    )
                ],
            )
        except Exception as err:
            logger.exception("Parsing Error:")
            raise ValueError(str(err) + ", Row: " + str(list(row)))

        return datastorage_for_create

    def convert_data_storage_field_df_row_to_pydantic(self, row: Series) -> DataStorageFieldRequest:
        """
        Создание объекта DataStorageFieldRequest по записи из файла.

        Args:
            row (str): строка в DataFrame с полями DataStorage

        Returns:
            DataStorageFieldRequest: модель на создание поля dataStorage в pydantic.
        """
        try:
            if not row[DataStorageFieldsFileColumnEnum.REF]:
                any_field = AnyField(
                    name=row[DataStorageFieldsFileColumnEnum.FIELD_NAME],
                    precision=row[DataStorageFieldsFileColumnEnum.LENGTH],
                    scale=row[DataStorageFieldsFileColumnEnum.SCALE],
                    type=row[DataStorageFieldsFileColumnEnum.DATA_TYPE],
                    aggregation_type=row[DataStorageFieldsFileColumnEnum.AGGREGATION_TYPE],
                    labels=[],
                )
                ref_type = BaseFieldType(
                    ref_object=any_field,
                    ref_object_type=BaseFieldTypeEnum.ANYFIELD,
                )
            else:
                ref_type = BaseFieldType(
                    ref_object=row[DataStorageFieldsFileColumnEnum.REF],
                    ref_object_type=row[DataStorageFieldsFileColumnEnum.SEMANTIC_TYPE],
                )
            field = DataStorageFieldRequest(
                name=row[DataStorageFieldsFileColumnEnum.FIELD_NAME],
                labels=labels_by_row(row, DataStorageFieldsFileColumnEnum),
                ref_type=ref_type,
                semantic_type=row[DataStorageFieldsFileColumnEnum.SEMANTIC_TYPE],
                sql_name=row[DataStorageFieldsFileColumnEnum.FIELD_NAME],
                is_key=row[DataStorageFieldsFileColumnEnum.KEY],
                is_sharding_key=row[DataStorageFieldsFileColumnEnum.SHARDING_KEY],
            )
        except Exception as err:
            logger.exception("Parsing Error:")
            raise ValueError(str(err) + ", Row: " + str(list(row)))
        return field

    async def try_create_data_storages_by_dict_for_create_and_fields(
        self,
        tenant_id: str,
        model_name: str,
        fields: dict[str, list[DataStorageFieldRequest]],
        data_storages_for_create: dict[str, DataStorageCreateRequest],
    ) -> dict[str, list[str]]:
        """
        Создание нескольких DataStorage из словаря data_storages_for_create и словаря fields.
        Функция итерируется по словарям data_storages_for_create (забирает DataStorageCreateRequest) и fields (забирает list[DataStorageFieldRequest])
        и 'вставляет' соответсвующие поля из fields в DataStorageCreateRequest. Те поля из fields, которые удалось сопоставить с data_storages_for_create
        удаляются из fields. Далее происходит попытка создания полученных DataStorage в базе.

        Args:
            tenant_id (str): тенант, где создать DataStorage
            model_name (str): имя модели в которой создать DataStorage
            fields (dict[str, list[DataStorageFieldRequest]]): словарь полей на создание в data_storage, где:
                1) ключ - имя data_storage,
                2) значение - список полей
            data_storages_for_create (dict[str, DataStorageCreateRequest]): словарь data_storage на создание, где:
                1) ключ - имя data_storage
                2) значение - data_storage на создание

        Returns:
            dict[str,list[str]]: словарь вида {"created": [], "not_created": []} со списками успешно и неуспешно созданных data_storage
        """
        created_data_storage: dict[str, list[str]] = {"created": [], "not_created": []}
        for data_storage_name, data_storage_model in data_storages_for_create.items():
            fields_for_create = fields.pop(data_storage_name, None)
            if fields_for_create is None:
                raise ValueError(f"Fields for dataStorage {data_storage_name} cannot be empty")
            data_storage_model.fields = fields_for_create
            try:
                _ = await self.create_data_storage_by_schema(tenant_id, model_name, data_storage_model)
                created_data_storage["created"].append(data_storage_name)
            except Exception:  # noqa
                logger.exception("Create dataStorage %s failed.", data_storage_name)
                created_data_storage["not_created"].append(data_storage_name)
        return created_data_storage

    async def try_update_data_storages_by_dict_fields_for_update(
        self,
        tenant_id: str,
        model_name: str,
        data_storages_fields: dict[str, list[DataStorageFieldRequest]],
    ) -> dict[str, list[str]]:
        """
        Обновление атрибута fields в нескольких объектах DataStorage, имена которых указаны в качестве ключа в словаре data_storages_fields, а
        значения атрибутов fields в качестве значений словаря data_storages_fields.
        Args:
            tenant_id (str): тенант, где обновить DataStorage
            model_name (str): имя модели в которой обновить DataStorage
            data_storages_fields (dict[str, list[DataStorageFieldRequest]]): словарь полей на обновление в data_storage, где:
                1) ключ - имя data_storage,
                2) значение - значение атрибута fields для обновления.
        Returns:
            dict[str,list[str]]: словарь вида {"updated": [], "not_updated": []} со списками успешно и неуспешно обновленных data_storage

        """
        updated_data_storage: dict[str, list[str]] = {"updated": [], "not_updated": []}
        for data_storage_name, fields_for_update in data_storages_fields.items():
            data_storage_for_update = DataStorageEditRequest(fields=fields_for_update)
            try:
                _ = await self.update_data_storage_by_name_and_schema(
                    tenant_id, model_name, data_storage_name, data_storage_for_update
                )
                updated_data_storage["updated"].append(data_storage_name)
            except Exception:  # noqa
                logger.exception("Create dataStorage %s failed.", data_storage_name)
                updated_data_storage["not_updated"].append(data_storage_name)
        return updated_data_storage

    async def create_or_update_data_storages_by_files(
        self,
        tenant_id: str,
        model_name: str,
        data_storages_file: Optional[UploadFile] = None,
        fields_file: Optional[UploadFile] = None,
    ) -> ImportFromFileResponse:
        """
        Массовый импорт DataStorage.

        Args:
            tenant_id (str): тенант, где обновить/создать DataStorage
            model_name (str): имя модели в которой обновить/создать DataStorage
            data_storages_file (Optional[UploadFile]): файл формата csv или xlsx с списком DataStorage на создание.
            fields_file (Optional[UploadFile]): файл формата csv или xlsx с списком полей DataStorage на обновление.
        Returns:
            ImportFromFileResponse: Модель, содержащая списки успешно/неуспешно созданных/обновленных DataStorage
        """
        fields: dict[str, list[DataStorageFieldRequest]] = {}
        data_storages_for_create: dict[str, DataStorageCreateRequest] = {}
        if data_storages_file:
            data_storages_df = read_upload_file_as_dataframe(
                data_storages_file,
                DATA_STORAGE_FILE_COLUMNS,
                DATA_STORAGE_FILE_COLUMNS_TYPE,
            )
            for _, row in data_storages_df.iterrows():
                data_storages_for_create[row[DataStorageFileColumnEnum.NAME]] = (
                    self.convert_data_storage_df_row_to_pydantic(row)
                )
        if fields_file:
            fields_df = read_upload_file_as_dataframe(
                fields_file,
                DATA_STORAGE_FIELDS_FILE_COLUMN,
                DATA_STORAGE_FIELDS_FILE_COLUMNS_TYPE,
            )
            for _, row in fields_df.iterrows():
                if row[DataStorageFieldsFileColumnEnum.DATA_STORAGE_NAME] not in fields:
                    fields[row[DataStorageFieldsFileColumnEnum.DATA_STORAGE_NAME]] = []
                fields[row[DataStorageFieldsFileColumnEnum.DATA_STORAGE_NAME]].append(
                    self.convert_data_storage_field_df_row_to_pydantic(row)
                )
        created_data_storage = await self.try_create_data_storages_by_dict_for_create_and_fields(
            tenant_id, model_name, fields, data_storages_for_create
        )
        updated_data_storage = await self.try_update_data_storages_by_dict_fields_for_update(
            tenant_id, model_name, fields
        )
        if created_data_storage.get("created") or updated_data_storage.get("updated"):
            await CacheRepository.clear_data_storages_cache_by_model_name(tenant_id=tenant_id, model_name=model_name)
            for ds_name in updated_data_storage.get("updated", []):
                await CacheRepository.clear_data_storage_cache_by_name_and_model_name(
                    tenant_id=tenant_id, model_name=model_name, name=ds_name
                )
        return ImportFromFileResponse(
            created=created_data_storage.get("created", []),
            not_created=created_data_storage.get("not_created", []),
            updated=updated_data_storage.get("updated", []),
            not_updated=updated_data_storage.get("not_updated", []),
        )

    async def create_and_send_command_to_aor_by_datastorage(
        self,
        tenant_id: str,
        datastorage: DataStorage,
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
                is_deleted=deleted, tenant=tenant_id, data_json=datastorage.model_dump(mode="json", by_alias=True)
            )
            parents = (
                await self.aor_repository.get_datastorage_parents_by_schema(tenant_id, datastorage)
                if with_parents
                else []
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
                "type": AorType.DATASTORAGE,
                "name": datastorage.name + name_suffix,
                "data_json": data_json,
                "description": datastorage.name,
                "version": (
                    str(datastorage.version) + version_suffix
                    if not deleted
                    else f"{datastorage.version}-deleted" + version_suffix
                ),
                "external_object_id": datastorage.name + name_suffix,
                "deployed_by": get_user_login_by_token(context.get(AuthorizationPlugin.key) or None),
                "parents": parents,
                "space_id": custom_uuid
                or (await self.aor_repository.get_datastorage_aor_space_by_names(tenant_id, [datastorage.name])).get(
                    datastorage.name
                ),
            }
            command_model = CreateAorCommand.model_validate(command)
        except Exception:
            logger.exception("Create command model failed.")
            command_model = None
        if send_command and command_model:
            await self.aor_client.send_request(command_model)
        return command_model

    async def create_and_send_command_to_aor_by_database_object(
        self,
        tenant_id: str,
        db_object: DatabaseObjectORM,
        deleted: bool = False,
        custom_uuid: Optional[UUID] = None,
        with_parents: bool = True,
        send_command: bool = True,
        send_delete: bool = True,
        version_suffix: str = "",
        parent_version_suffix: str = "",
        name_suffix: str = "",
        parent_name_suffix: str = "",
    ) -> Optional[CreateAorCommand]:
        """
        Формирует и отправляет команду в AOR для VIEW (database_object).
        """
        if db_object.json_definition is None:
            logger.warning("DatabaseObject %s has empty json_definition.", db_object.name)
            return None
        try:
            parents_info = (
                await self.database_object_relations_repository.get_datastorage_parents_by_database_object(
                    tenant_id, db_object.id, db_object.version
                )
                if with_parents
                else []
            )
            parents: list[AorKafkaObjectParent] = []
            parent_names: list[str] = []
            for parent_name, parent_version in parents_info:
                parent_names.append(parent_name)
                parents.append(
                    AorKafkaObjectParent(
                        parent_type=AorType.DATASTORAGE,
                        parent_name=parent_name + parent_name_suffix,
                        parent_version=str(parent_version) + parent_version_suffix,
                        parent_external_id=parent_name + parent_name_suffix,
                    )
                )
            space_id = custom_uuid
            if space_id is None and parent_names:
                space_id = (await self.aor_repository.get_datastorage_aor_space_by_names(tenant_id, parent_names)).get(
                    parent_names[0]
                )
            if space_id is None:
                logger.warning("AOR space not found for database_object %s.", db_object.name)
                return None
            data_json = JsonData(is_deleted=deleted, tenant=tenant_id, data_json=db_object.json_definition or {})
            schema_name = db_object.schema_name or ""
            external_object_id = (
                f"{schema_name}.{db_object.name}{name_suffix}" if schema_name else f"{db_object.name}{name_suffix}"
            )
            command = {
                "type": AorType.DATABASEOBJECT,
                "name": db_object.name + name_suffix,
                "data_json": data_json,
                "description": db_object.name,
                "version": (
                    str(db_object.version) + version_suffix
                    if not deleted
                    else f"{db_object.version}-deleted" + version_suffix
                ),
                "external_object_id": external_object_id,
                "deployed_by": get_user_login_by_token(context.get(AuthorizationPlugin.key) or None),
                "parents": parents,
                "space_id": space_id,
            }
            command_model = CreateAorCommand.model_validate(command)
        except Exception:
            logger.exception("Create command model failed.")
            command_model = None
        if send_command and command_model:
            if not deleted and send_delete:
                delete_data_json = JsonData(
                    is_deleted=True, tenant=tenant_id, data_json=db_object.json_definition or {}
                )
                delete_command = {
                    "type": AorType.DATABASEOBJECT,
                    "name": db_object.name + name_suffix,
                    "data_json": delete_data_json,
                    "description": db_object.name,
                    "version": f"{db_object.version}-deleted" + version_suffix,
                    "external_object_id": external_object_id,
                    "deployed_by": get_user_login_by_token(context.get(AuthorizationPlugin.key) or None),
                    "parents": parents,
                    "space_id": space_id,
                }
                delete_command_model = CreateAorCommand.model_validate(delete_command)
                await self.aor_client.send_request(delete_command_model)
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
        datastorage = await self.get_data_storage_by_name(tenant_id, None, name)
        await self.create_and_send_command_to_aor_by_datastorage(
            tenant_id,
            datastorage,
            deleted,
            custom_uuid,
            with_parents,
            depends_no_attrs_versions=depends_no_attrs_versions,
            version_suffix=version_suffix,
            parent_version_suffix=parent_version_suffix,
            name_suffix=name_suffix,
            parent_name_suffix=parent_name_suffix,
        )

    async def send_database_object_to_aor_by_name(
        self,
        tenant_id: str,
        name: str,
        deleted: bool = False,
        custom_uuid: Optional[UUID] = None,
        with_parents: bool = True,
        version_suffix: str = "",
        parent_version_suffix: str = "",
        name_suffix: str = "",
        parent_name_suffix: str = "",
    ) -> None:
        """
        Отправляет VIEW (database_object) в AOR по имени объекта.
        """
        db_object = await self.database_object_repository.get_view_by_name(tenant_id, name)
        if db_object is None:
            raise ValueError(f"DatabaseObject VIEW {tenant_id}.{name} not found.")
        await self.create_and_send_command_to_aor_by_database_object(
            tenant_id=tenant_id,
            db_object=db_object,
            deleted=deleted,
            custom_uuid=custom_uuid,
            with_parents=with_parents,
            version_suffix=version_suffix,
            parent_version_suffix=parent_version_suffix,
            name_suffix=name_suffix,
            parent_name_suffix=parent_name_suffix,
        )

    async def send_database_objects_to_model(
        self, tenant_id: str, model: Model, aor_model_request: CreateModelAorRequest
    ) -> None:
        """
        Отправляет все VIEW модели в AOR.
        """
        db_objects = await self.database_object_repository.get_views_by_model(tenant_id, model.name)
        if not db_objects:
            raise ValueError(f"Not found databaseObjects for tenant_id={tenant_id}, model_name={model.name}")
        for db_object in db_objects:
            await self.create_and_send_command_to_aor_by_database_object(
                tenant_id=tenant_id,
                db_object=db_object,
                custom_uuid=aor_model_request.space_id,
                with_parents=aor_model_request.with_parents,
                version_suffix=aor_model_request.version_suffix,
                parent_version_suffix=aor_model_request.parent_version_suffix,
                name_suffix=aor_model_request.name_suffix,
                parent_name_suffix=aor_model_request.parent_name_suffix,
            )
        logger.info("%s databaseObjects sended", len(db_objects))

    async def drop_dependent_views_for_datastorage(
        self,
        tenant_id: str,
        model_name: str,
        data_storage_name: str,
    ) -> None:
        """Удаляет зависимые VIEW для хранилища данных перед его установкой."""
        try:
            data_storage_id, data_storage_version = await self.data_repository.get_id_version_by_name(
                tenant_id=tenant_id,
                model_name=model_name,
                name=data_storage_name,
            )
        except NoResultFound:
            logger.info(
                "DataStorage %s.%s.%s not found. Skip drop dependent views.",
                tenant_id,
                model_name,
                data_storage_name,
            )
            return
        model_orm = await self.model_repository.get_model_orm_by_session_with_error(tenant_id, model_name)
        database = DatabaseModel.model_validate(model_orm.database)
        views = await self.database_object_relations_repository.get_views_by_datastorage(
            tenant_id=tenant_id,
            data_storage_id=data_storage_id,
            data_storage_version=data_storage_version,
        )
        if not views:
            return
        schema_name = model_orm.schema_name
        if not schema_name:
            logger.warning("Model %s has empty schema_name. Skip drop dependent views.", model_name)
            return
        views = [view for view in views if view.schema_name == schema_name]
        if not views:
            return
        generator = get_generator(model_orm)
        cluster_name = database.default_cluster_name
        sql_expressions: list[str] = []
        seen: set[tuple[str, str]] = set()
        for view in views:
            if not view.schema_name:
                logger.warning("DatabaseObject %s has empty schema_name. Skip drop.", view.name)
                continue
            key = (view.schema_name, view.name)
            if key in seen:
                continue
            seen.add(key)
            sql_expressions.append(generator._get_delete_view_sql(view.schema_name, view.name, cluster_name))
        if not sql_expressions:
            return
        await generator._execute_DDL(sql_expressions, database)
        logger.info(
            "Dropped %s dependent views for datastorage %s.%s.%s",
            len(sql_expressions),
            tenant_id,
            model_name,
            data_storage_name,
        )

    async def deploy_database_object_from_aor(self, push_command: PushAorCommand) -> None:
        """Раскатывает VIEW из AOR: преобразует JSON в SQL, создает представление и обновляет связи."""
        prepared = await self._prepare_database_object_deploy(push_command)
        if not prepared:
            return
        tenant_id, view_name, view_json, parent_names = prepared
        view_schema = self._get_view_schema(view_json)
        targets = await self._build_database_object_targets(
            tenant_id=tenant_id,
            view_name=view_name,
            parent_names=parent_names,
            view_schema=view_schema,
        )
        if not targets:
            logger.warning("No targets found for database_object deploy. view=%s", view_name)
            return
        await self._apply_database_object_to_targets(
            tenant_id=tenant_id,
            view_name=view_name,
            view_json=view_json,
            is_deleted=push_command.data_json.is_deleted,
            targets=targets,
        )

    async def _prepare_database_object_deploy(
        self, push_command: PushAorCommand
    ) -> Optional[tuple[str, str, dict[str, Any], list[str]]]:
        """Подготавливает данные для раскатки database_object из AOR."""
        tenant_id = push_command.data_json.tenant
        view_json = push_command.data_json.data_json
        view_name = view_json.get("name") or push_command.name
        if not view_name:
            logger.warning("AOR database_object without name. tenant=%s", tenant_id)
            return None
        parents = push_command.parents or []
        parent_names = [parent.parent_name for parent in parents if parent.parent_type == AorType.DATASTORAGE]
        if not parent_names:
            logger.warning("AOR database_object without DATA_STORAGE parents. view=%s", view_name)
            return None
        return tenant_id, view_name, view_json, parent_names

    async def _build_database_object_targets(
        self,
        tenant_id: str,
        view_name: str,
        parent_names: list[str],
        view_schema: Optional[str],
    ) -> dict[tuple[str, str], dict[str, Any]]:
        """Собирает цели раскатки database_object по родительским хранилищам."""
        targets: dict[tuple[str, str], dict[str, Any]] = {}
        for parent_name in parent_names:
            try:
                data_storage_orm = await self.data_repository.get_orm_by_name(
                    tenant_id=tenant_id,
                    model_name=None,
                    name=parent_name,
                )
            except NoResultFound:
                logger.warning("DataStorage %s not found. view=%s", parent_name, view_name)
                continue
            for model_orm in data_storage_orm.models:
                if not model_orm.schema_name:
                    logger.warning("Model %s has empty schema_name. view=%s", model_orm.name, view_name)
                    continue
                if view_schema and model_orm.schema_name != view_schema:
                    continue
                database = DatabaseModel.model_validate(model_orm.database)
                key = (database.name, model_orm.schema_name)
                target = targets.setdefault(
                    key,
                    {
                        "database": database,
                        "schema_name": model_orm.schema_name,
                        "model_orm": model_orm,
                        "models": [],
                        "data_storages": [],
                    },
                )
                cast(list[ModelORM], target["models"]).append(model_orm)
                cast(list[DataStorageORM], target["data_storages"]).append(data_storage_orm)
        return targets

    async def _apply_database_object_to_targets(
        self,
        tenant_id: str,
        view_name: str,
        view_json: dict[str, Any],
        is_deleted: bool,
        targets: dict[tuple[str, str], dict[str, Any]],
    ) -> None:
        """Применяет раскатку database_object для выбранных целей."""
        session = self.data_repository.session
        for target in targets.values():
            database = cast(DatabaseModel, target["database"])
            schema_name = cast(str, target["schema_name"])
            model_orm = cast(ModelORM, target["model_orm"])
            generator = get_generator(model_orm)
            cluster_name = database.default_cluster_name
            target_view_json = self._rewrite_view_json_schema(view_json, schema_name)
            sql_expression = build_view_sql_expression(target_view_json)
            sql_expressions: list[str] = [generator._get_delete_view_sql(schema_name, view_name, cluster_name)]
            if not sql_expression and not is_deleted:
                logger.warning("AOR database_object without SQL expression. view=%s. Drop only.", view_name)
                await generator._execute_DDL(sql_expressions, database)
                continue
            if not is_deleted and sql_expression:
                sql_expressions.extend(
                    generator._get_create_view_sql(
                        schema_name,
                        view_name,
                        sql_expression,
                        cluster_name,
                        replace=False,
                    )
                )
            await generator._execute_DDL(sql_expressions, database)
            if is_deleted:
                continue
            db_object = await self.database_object_repository.upsert_view(
                tenant_id=tenant_id,
                schema_name=schema_name,
                name=view_name,
                json_definition=target_view_json,
                models=cast(list[ModelORM], target["models"]),
            )
            if db_object is None:
                continue
            for data_storage_orm in cast(list[DataStorageORM], target["data_storages"]):
                await self.database_object_relations_repository.ensure_relation(
                    semantic_object_type=SemanticObjectsTypeEnum.DATA_STORAGE,
                    semantic_object_id=data_storage_orm.id,
                    semantic_object_version=data_storage_orm.version,
                    database_object_id=db_object.id,
                    database_object_version=db_object.version,
                    relation_type=DatabaseObjectRelationTypeEnum.PARENT,
                )
        await session.commit()

    @staticmethod
    def _rewrite_view_json_schema(view_json: dict[str, Any], schema_name: str) -> dict[str, Any]:
        """Копирует описание VIEW и подставляет целевую схему для всех источников."""
        rewritten = deepcopy(view_json)
        for query in rewritten.get("queries", []) or []:
            for source in query.get("from", []) or []:
                if isinstance(source, dict) and "schema" in source:
                    source["schema"] = schema_name
            for join in query.get("joins", []) or []:
                if isinstance(join, dict):
                    table = join.get("table")
                    if isinstance(table, dict) and "schema" in table:
                        table["schema"] = schema_name
        for dep in rewritten.get("dependencies", []) or []:
            if isinstance(dep, dict) and "schema" in dep:
                dep["schema"] = schema_name
        return rewritten

    @staticmethod
    def _get_view_schema(view_json: dict[str, Any]) -> Optional[str]:
        """Пытается извлечь схему VIEW из описания."""
        for dep in view_json.get("dependencies", []) or []:
            if isinstance(dep, dict) and dep.get("schema"):
                return dep.get("schema")
        for query in view_json.get("queries", []) or []:
            for source in query.get("from", []) or []:
                if isinstance(source, dict) and source.get("schema"):
                    return source.get("schema")
        return None

    async def collect_views_for_model(
        self,
        tenant_id: str,
        model_name: str,
        data_storage_names: Optional[list[str]] = None,
        send_to_aor: bool = True,
    ) -> list[int]:
        """
        Собирает VIEW для модели: получает DDL, преобразует в JSON и сохраняет в БД.
        """
        if not settings.ENABLE_COLLECT_VIEW_FOR_DS:
            return []
        session = self.data_repository.session
        model = await self.model_repository.get_model_orm_by_session_with_error(tenant_id, model_name)
        database = DatabaseModel.model_validate(model.database)
        generator = get_generator(model)
        if data_storage_names:
            data_storages = await self.data_repository.get_list_by_names(tenant_id, model_name, data_storage_names)
        else:
            data_storages = await self.data_repository.get_datastorage_orm_list_by_session(tenant_id, model_name)
        collected_ids: list[int] = []
        db_objects_to_send: dict[int, DatabaseObjectORM] = {}
        dialect = self._get_sqlglot_dialect(database.type)
        storages_by_schema: dict[str, list[tuple[DataStorageORM, list[str]]]] = {}
        table_names_by_schema: dict[str, set[str]] = {}
        for data_storage in data_storages:
            database_objects = get_filtred_database_object_by_data_storage(data_storage, model.name)
            database_object_names = get_database_object_names(database_objects)
            table_names = [
                name
                for name in (
                    database_object_names.table_name,
                    database_object_names.distributed_name,
                    database_object_names.dictionary_name,
                )
                if name
            ]
            if not database_object_names.table_schema or not table_names:
                continue
            storages_by_schema.setdefault(database_object_names.table_schema, []).append((data_storage, table_names))
            table_names_by_schema.setdefault(database_object_names.table_schema, set()).update(table_names)
        db_object_cache: dict[tuple[str, str], DatabaseObjectORM | None] = {}
        for schema_name, schema_table_names in table_names_by_schema.items():
            views = await generator.find_views_by_table(database, schema_name, sorted(schema_table_names))
            if not views:
                continue
            schema_storages = storages_by_schema.get(schema_name, [])
            for view in views:
                view_name = view["view_name"]
                view_schema = view["view_schema"]
                view_definition = view["view_definition"]
                if not view_definition:
                    continue
                view_json_definition = parse_view_ddl(view_definition, view_name, dialect=dialect)
                dependency_names = _get_view_dependency_names(view_json_definition)
                matched_storages = [
                    data_storage
                    for data_storage, ds_table_names in schema_storages
                    if _view_depends_on_tables(view_definition, ds_table_names, dependency_names)
                ]
                if not matched_storages:
                    continue
                cache_key = (view_schema, view_name)
                db_object = db_object_cache.get(cache_key)
                if cache_key not in db_object_cache:
                    related_models = await self._get_models_by_schema(tenant_id, model.database_id, view_schema)
                    db_object = await self.database_object_repository.upsert_view(
                        tenant_id=tenant_id,
                        schema_name=view_schema,
                        name=view_name,
                        json_definition=view_json_definition,
                        models=related_models,
                    )
                    db_object_cache[cache_key] = db_object
                if db_object is None:
                    continue
                for data_storage in matched_storages:
                    await self.database_object_relations_repository.ensure_relation(
                        semantic_object_type=SemanticObjectsTypeEnum.DATA_STORAGE,
                        semantic_object_id=data_storage.id,
                        semantic_object_version=data_storage.version,
                        database_object_id=db_object.id,
                        database_object_version=db_object.version,
                        relation_type=DatabaseObjectRelationTypeEnum.PARENT,
                    )
                    collected_ids.append(db_object.id)
                if send_to_aor:
                    db_objects_to_send.setdefault(db_object.id, db_object)
        await session.commit()
        unique_ids = sorted(set(collected_ids))
        if send_to_aor and db_objects_to_send:
            for db_object in db_objects_to_send.values():
                await self.create_and_send_command_to_aor_by_database_object(
                    tenant_id=tenant_id,
                    db_object=db_object,
                    with_parents=True,
                    send_delete=False,
                )
        return unique_ids

    async def _get_models_by_schema(self, tenant_id: str, database_id: int, schema_name: str) -> list[ModelORM]:
        """
        Возвращает модели по базе и схеме для связи VIEW с model.
        """
        query = select(ModelORM).where(
            ModelORM.tenant_id == tenant_id,
            ModelORM.database_id == database_id,
            ModelORM.schema_name == schema_name,
        )
        return list((await self.data_repository.session.execute(query)).scalars().all())

    @staticmethod
    def _get_sqlglot_dialect(db_type: DatabaseTypeEnum) -> str:
        """
        Возвращает диалект sqlglot для типа базы данных.
        """
        if db_type == DatabaseTypeEnum.CLICKHOUSE:
            return "clickhouse"
        return "postgres"

    def __repr__(self) -> str:
        return "DataStorageService"
