"""
Репозиторий для DSO
"""

import copy
from typing import Any, Optional

from py_common_lib.logger import EPMPYLogger
from py_common_lib.utils import timeit
from sqlalchemy import select, update
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload, with_loader_criteria

from src.config import settings
from src.db.any_field import AnyField
from src.db.data_storage import (
    DataStorage as DataStorageOrm,
    DataStorageField,
    DataStorageFieldLabel,
    DataStorageLabel,
    DataStorageModelRelation as DataStorageModelRelationOrm,
)
from src.db.database_object import DatabaseObject
from src.db.dimension import Dimension, DimensionLabel
from src.db.measure import Measure
from src.db.model import Model as ModelOrm
from src.models.any_field import AnyFieldTypeEnum
from src.models.data_storage import (
    DataStorage as DataStorageModel,
    DataStorageCreateRequest as DataStorageCreateRequestModel,
    DataStorageEditRequest as DataStorageEditRequestModel,
    DataStorageEnum,
    DataStorageFieldRequest as DataStorageFieldRequestModel,
    DataStorageLogsFieldEnum,
)
from src.models.database import Database as DatabaseModel, DatabaseTypeEnum
from src.models.database_object import (
    DatabaseObject as DatabaseObjectModel,
    DatabaseObjectRequest as DatabaseObjectRequestModel,
    DbObjectTypeEnum,
)
from src.models.enum import InformationCategoryEnum
from src.models.field import BaseFieldType, BaseFieldTypeEnum, SemanticType
from src.models.label import LabelType, Language
from src.models.model import Model as ModelModel, ModelStatusEnum
from src.models.request_params import Pagination
from src.repository.database_object import DatabaseObjectRepository
from src.repository.generators.utils import get_generator
from src.repository.history.data_storage import DataStorageHistoryRepository
from src.repository.model import ModelRepository
from src.repository.utils import (
    add_missing_labels,
    convert_field_to_orm,
    convert_labels_list_to_orm,
    convert_ref_type_to_orm,
    get_database_schema_database_object_mapping,
    get_filtred_database_object_by_data_storage,
    get_list_dimension_orm_by_session,
    get_object_filtred_by_model_name,
    get_select_query_with_offset_limit_order,
    update_database_objects_schema_for_model,
)

DATA_STORAGE_LOG_TABLE_LEGTH = {
    DataStorageLogsFieldEnum.TIMESTAMP: 50,
    DataStorageLogsFieldEnum.BATCHID: 36,
    DataStorageLogsFieldEnum.ACTION: 255,
    DataStorageLogsFieldEnum.OPERATION: 255,
    DataStorageLogsFieldEnum.USERID: 255,
}

DATA_STORAGE_LOGA_TABLE_FIELDS = [
    DataStorageLogsFieldEnum.TIMESTAMP,
    DataStorageLogsFieldEnum.BATCHID,
    DataStorageLogsFieldEnum.ACTION,
    DataStorageLogsFieldEnum.OPERATION,
    DataStorageLogsFieldEnum.USERID,
]

logger = EPMPYLogger(__name__)


class DataStorageRepository:
    def __init__(
        self,
        session: AsyncSession,
        model_repository: ModelRepository,
        database_object_repository: DatabaseObjectRepository,
    ) -> None:
        self.session = session
        self.model_repository = model_repository
        self.database_object_repository = database_object_repository
        self.data_storage_history_repository = DataStorageHistoryRepository(session)

    async def _convert_field_object_list_to_orm(
        self,
        tenant_id: str,
        model_names: list[str],
        fields: list[dict[str, Any]],
        ignore_field_name: Optional[str] = None,
    ) -> list[DataStorageField]:
        """
        Конвертирует field для dso из формата list[dict] в list[DataStorageField].
        """
        result_fields = []
        for field in fields:
            if field["name"] == ignore_field_name:
                continue
            model_field = await convert_field_to_orm(self.session, field, tenant_id, model_names, DataStorageField)
            if isinstance(model_field, DataStorageField):
                result_fields.append(model_field)
            else:
                raise ValueError("Failed to cast model_field to DataStorageField")
        return result_fields

    async def _update_field_attrs(
        self,
        field: DataStorageField,
        field_dict: dict[str, Any],
        tenant_id: str,
        model_names: list[str],
    ) -> None:
        """
        Обновляет атрибуты существующего ORM-объекта DataStorageField из словаря,
        сохраняя id строки в БД (без DELETE + INSERT).

        Args:
            field (DataStorageField): Существующий ORM-объект поля.
            field_dict (dict[str, Any]): Словарь с новыми значениями атрибутов поля.
            tenant_id (str): Идентификатор тенанта.
            model_names (list[str]): Список имён моделей для резолва ссылок.
        """
        old_any_field = field.any_field

        ref_type = field_dict.pop("ref_type")
        field_dict.pop("sql_column_type", None)
        object_field = await convert_ref_type_to_orm(self.session, tenant_id, model_names, ref_type)

        labels = field_dict.pop("labels", None)
        if labels is not None:
            converted_labels = convert_labels_list_to_orm(labels, DataStorageFieldLabel)
            current_labels_values = {(label.language, label.type, label.text) for label in field.labels}
            new_labels_values = {(label.language, label.type, label.text) for label in converted_labels}
            if current_labels_values != new_labels_values:
                field.labels = converted_labels
        field.semantic_type = field_dict.get("semantic_type", field.semantic_type)
        field.sql_name = field_dict.get("sql_name", field.sql_name)
        is_key = field_dict.get("is_key")
        if is_key is not None:
            field.is_key = is_key
        is_sharding_key = field_dict.get("is_sharding_key")
        if is_sharding_key is not None:
            field.is_sharding_key = is_sharding_key
        is_tech_field = field_dict.get("is_tech_field")
        if is_tech_field is not None:
            field.is_tech_field = is_tech_field
        field.field_type = ref_type["ref_object_type"]

        field.dimension_id = None
        field.dimension = None
        field.measure_id = None
        field.measure = None
        field.any_field = None

        if field.field_type == BaseFieldTypeEnum.MEASURE and isinstance(object_field, Measure):
            field.measure_id = object_field.id
            field.measure = object_field
        elif field.field_type == BaseFieldTypeEnum.DIMENSION and isinstance(object_field, Dimension):
            field.dimension_id = object_field.id
            field.dimension = object_field
        elif field.field_type == BaseFieldTypeEnum.ANYFIELD and isinstance(object_field, AnyField):
            field.any_field = object_field
        elif object_field is not None:
            raise ValueError("object_field has unknown type.")

        if old_any_field is not None and old_any_field is not field.any_field:
            await self.session.delete(old_any_field)

    async def _update_fields_in_place(
        self,
        data_storage_orm: DataStorageOrm,
        new_fields_dicts: list[dict[str, Any]],
        tenant_id: str,
        model_names: list[str],
        ignore_field_name: Optional[str] = None,
    ) -> None:
        """
        Обновляет поля DataStorage на месте в InstrumentedList, без замены коллекции.
        Сопоставляет существующие поля по имени и обновляет их атрибуты,
        новые поля добавляет через append, отсутствующие — удаляет через remove.
        Сохраняет id существующих строк, что предотвращает нарушение FK из composite.

        Args:
            data_storage_orm (DataStorageOrm): ORM-объект хранилища, чья коллекция fields мутируется.
            new_fields_dicts (list[dict[str, Any]]): Список словарей с новыми значениями полей.
            tenant_id (str): Идентификатор тенанта.
            model_names (list[str]): Список имён моделей для резолва ссылок.
            ignore_field_name (Optional[str]): Имя поля, которое нужно пропустить.
        """
        existing_by_name: dict[str, DataStorageField] = {f.name: f for f in data_storage_orm.fields}
        new_field_names: set[str] = set()

        for field_dict in new_fields_dicts:
            field_name = field_dict["name"]
            if field_name == ignore_field_name:
                continue
            new_field_names.add(field_name)

            if field_name in existing_by_name:
                await self._update_field_attrs(existing_by_name[field_name], field_dict, tenant_id, model_names)
            else:
                model_field = await convert_field_to_orm(
                    self.session, field_dict, tenant_id, model_names, DataStorageField
                )
                if not isinstance(model_field, DataStorageField):
                    raise ValueError("Failed to cast model_field to DataStorageField")
                data_storage_orm.fields.append(model_field)

        for field_name, field in existing_by_name.items():
            if field_name not in new_field_names and field_name != ignore_field_name:
                data_storage_orm.fields.remove(field)

    async def create_or_update_data_storage_by_session(
        self,
        tenant_id: str,
        model_name: str,
        name: str,
        data_storage_model: Any,
        is_create: bool,
    ) -> DataStorageOrm:
        """
        Создает или обновляет хранилище данных в базе данных.

        Args:
            tenant_id (str): Идентификатор арендатора (тенанта).
            model_name (str): Имя модели, к которой привязывается хранилище данных.
            name (str): Имя хранилища данных.
            data_storage_model (Any): Модель хранилища данных (создания или редактирования).
            is_create (bool): Флаг, указывающий, нужно ли создать новое хранилище данных или обновить существующее.

        Returns:
            DataStorageOrm: Объект хранилища данных ORM.
        """
        if is_create:
            data_storage = await self._create_data_storage_orm(
                tenant_id=tenant_id,
                model_name=model_name,
                data_storage=data_storage_model,
                without_relations=True,
            )
            self.session.add(data_storage)
            return data_storage
        else:
            data_storage = await self.update_datastorage_orm(
                tenant_id=tenant_id,
                model_name=model_name,
                name=name,
                data_storage=data_storage_model,
            )
            return data_storage

    def get_data_storage_with_unique_database_objects(self, data_storage: DataStorageOrm) -> DataStorageModel:
        """
        Возвращает хранилище данных с уникальными объектами базы данных.

        Args:
            data_storage (DataStorageOrm): Объект хранилища данных ORM.

        Returns:
            DataStorageModel: Объект хранилища данных с уникальными объектами базы данных.
        """
        data_storage_model = DataStorageModel.model_validate(data_storage)
        database_objects_set = set()
        database_objects_created = []
        for database_object in data_storage_model.database_objects:
            if database_object.name not in database_objects_set:
                database_objects_set.add(database_object.name)
                database_objects_created.append(database_object)
        data_storage_model.database_objects = database_objects_created
        return data_storage_model

    def get_database_objects_by_models(
        self, ds_name: str, ds_type: DataStorageEnum | str, model: ModelOrm
    ) -> list[DatabaseObjectModel]:
        """
        Возвращает список объектов базы данных, связанных с хранилищем данных и моделью.

        Args:
            ds_name (str): Имя хранилища данных.
            ds_type (DataStorageEnum | str): Тип хранилища данных.
            model (ModelOrm): Модель, к которой привязаны объекты базы данных.

        Returns:
            list[DatabaseObjectModel]: Список объектов базы данных.
        """
        database_objects = []
        database_model = DatabaseModel.model_validate(model.database)
        model_model = ModelModel.model_validate(model)
        db_model_type = database_model.type

        if db_model_type in [DatabaseTypeEnum.GREENPLUM, DatabaseTypeEnum.POSTGRESQL]:
            # otherwise it's Clickhouse type
            return [self._get_usual_database_object(ds_name, model_model)]

        if DataStorageEnum.is_ment_to_be_replicated(ds_type):
            database_objects.append(self._get_replicated_database_object(ds_name, model_model))
        else:
            database_objects.append(self._get_usual_database_object(ds_name, model_model))

        if not DataStorageEnum.is_ment_to_be_replicated(ds_type) and database_model.default_cluster_name:
            database_objects.append(self._get_distributed_database_object(ds_name, model_model))

        if DataStorageEnum.is_dimension_related(ds_type):
            database_objects.append(self._get_dictionary_database_object(ds_name, model_model))

        return database_objects

    @staticmethod
    def _get_replicated_database_object(ds_name: str, model_model: ModelModel) -> DatabaseObjectModel:
        """
        Возвращает реплицированный объект базы данных для хранилища данных.

        Args:
            ds_name (str): Имя хранилища данных.
            model_model (ModelModel): Модель, к которой привязана база данных.

        Returns:
            DatabaseObjectModel: Реплицированный объект базы данных.
        """
        return DatabaseObjectModel.model_validate(
            {
                "name": ds_name,
                "schema_name": model_model.schema_name,
                "type": DbObjectTypeEnum.REPLICATED_TABLE,
                "specific_attributes": [],
            }
        )

    @staticmethod
    def _get_usual_database_object(ds_name: str, model_model: ModelModel) -> DatabaseObjectModel:
        """
        Возвращает обычный объект базы данных для хранилища данных.

        Args:
            ds_name (str): Имя хранилища данных.
            model_model (ModelModel): Модель, к которой привязана база данных.

        Returns:
            DatabaseObjectModel: Обычный объект базы данных.
        """
        return DatabaseObjectModel.model_validate(
            {
                "name": ds_name,
                "schema_name": model_model.schema_name,
                "type": DbObjectTypeEnum.TABLE,
                "specific_attributes": [],
            }
        )

    @staticmethod
    def _get_distributed_database_object(ds_name: str, model_model: ModelModel) -> DatabaseObjectModel:
        """
        Возвращает распределенный объект базы данных для хранилища данных.

        Args:
            ds_name (str): Имя хранилища данных.
            model_model (ModelModel): Модель, к которой привязана база данных.

        Returns:
            DatabaseObjectModel: Распределенный объект базы данных.
        """
        return DatabaseObjectModel.model_validate(
            {
                "name": settings.DISTRIBUTED_TABLE_PATTERN % ds_name,
                "schema_name": model_model.schema_name,
                "type": DbObjectTypeEnum.DISTRIBUTED_TABLE,
                "specific_attributes": [],
            }
        )

    @staticmethod
    def _get_dictionary_database_object(ds_name: str, model_model: ModelModel) -> DatabaseObjectModel:
        """
        Возвращает объект базы данных для словаря хранилища данных.

        Args:
            ds_name (str): Имя хранилища данных.
            model_model (ModelModel): Модель, к которой привязана база данных.

        Returns:
            DatabaseObjectModel: Объект базы данных.
        """
        return DatabaseObjectModel.model_validate(
            {
                "name": settings.DICTIONARY_TABLE_PATTERN % ds_name,
                "schema_name": model_model.schema_name,
                "type": DbObjectTypeEnum.DICTIONARY,
                "specific_attributes": [],
            }
        )

    def get_data_storage_schema(
        self,
        ds_name: str,
        ds_fields: list,
        data_storage_type: DataStorageEnum,
        model: ModelOrm,
        is_create: bool,
    ) -> DataStorageEditRequestModel | DataStorageCreateRequestModel:
        """
        Возвращает схему хранилища данных для создания или редактирования.

        Args:
            ds_name (str): Имя хранилища данных.
            ds_fields (list): Список полей хранилища данных.
            data_storage_type (DataStorageEnum): Тип хранилища данных.
            model (ModelOrm): Модель, к которой привязывается хранилище данных.
            is_create (bool): Флаг, указывающий, нужно ли создать новую схему или подготовить схему для редактирования.

        Returns:
            DataStorageEditRequestModel | DataStorageCreateRequestModel: Схема хранилища данных.
        """
        data_storage_dict: dict[str, Any]
        data_storage: DataStorageEditRequestModel | DataStorageCreateRequestModel
        if not is_create:
            data_storage = DataStorageEditRequestModel.model_validate({"fields": ds_fields})
        else:
            data_storage_dict = {
                "fields": ds_fields,
                "name": ds_name,
                "type": data_storage_type,
                "table": {
                    "name": ds_name,
                    "schema_name": model.schema_name,
                },
            }
            data_storage = DataStorageCreateRequestModel.model_validate(data_storage_dict)
        return data_storage

    def get_any_field_dict(
        self,
        field_type: AnyFieldTypeEnum,
        name: str,
        precision: int,
        labels: Optional[list] = None,
    ) -> dict:
        """Создать словарь - AnyField."""
        if labels is None:
            labels = []
        return {
            "type": field_type,
            "name": name,
            "labels": labels,
            "precision": precision,
        }

    def get_ref_type_dict(self, ref_object_type: BaseFieldTypeEnum, ref_object: str | dict) -> dict:
        """Создать словарь - type."""
        return {
            "ref_object_type": ref_object_type,
            "ref_object": ref_object,
        }

    def get_field_dict(
        self,
        name: str,
        ref_type: dict,
        semantic_type: SemanticType,
        is_key: bool = False,
        labels: Optional[list] = None,
        is_tech_field: bool = False,
    ) -> dict:
        """Создать словарь - поле DSO."""
        if labels is None:
            labels = []
        return {
            "name": name,
            "ref_type": ref_type,
            "is_key": is_key,
            "labels": labels,
            "sql_name": name,
            "semantic_type": semantic_type,
            "is_tech_field": is_tech_field,
        }

    def create_dimension_data_storage_field_dict(
        self,
        dimension_name: str,
        is_key: bool = True,
        is_tech_field: bool = False,
        ds_field_name: str | None = None,
    ) -> dict:
        """
        Создает словарь поля хранилища данных типа DIMENSION.

        Args:
            dimension_name (str): Имя измерения, соответствующее полю хранилища данных.
            is_key (bool, optional): Указывает, является ли поле ключом. По умолчанию True.
            is_tech_field (bool, optional): Указывает, является ли поле техническим полем. По умолчанию False.
            ds_field_name (str | None, optional): Имя поля хранилища данных. Если не указано, используется имя измерения.

        Returns:
            dict: Словарь, представляющий поле хранилища данных типа DIMENSION.
        """
        if not ds_field_name:
            ds_field_name = dimension_name
        ref_type = self.get_ref_type_dict(
            ref_object_type=BaseFieldTypeEnum.DIMENSION,
            ref_object=dimension_name,
        )
        return self.get_field_dict(
            name=ds_field_name,
            ref_type=ref_type,
            semantic_type=SemanticType.DIMENSION,
            is_key=is_key,
            is_tech_field=is_tech_field,
        )

    def create_anyfield_data_storage_field(
        self,
        field_name: str,
        field_type: AnyFieldTypeEnum,
        precision: int,
        semantic_type: SemanticType,
        is_key: bool = False,
        labels: Optional[list] = None,
    ) -> dict:
        """Создать словарь - поле DSO типа AnyField"""
        if labels is None:
            labels = []
        any_field = self.get_any_field_dict(
            field_type=field_type,
            name=field_name,
            precision=precision,
            labels=labels,
        )
        ref_type = self.get_ref_type_dict(
            ref_object_type=BaseFieldTypeEnum.ANYFIELD,
            ref_object=any_field,
        )
        return self.get_field_dict(
            name=any_field["name"],
            ref_type=ref_type,
            semantic_type=semantic_type,
            labels=labels,
            is_key=is_key,
        )

    async def create_fields_dimensions_if_not_exists(
        self, model: ModelOrm, fields_to_create: dict, is_virtual: bool
    ) -> None:
        """
        Создает поля-измерения, если они еще не существуют.

        Args:
            model (ModelOrm): Модель, к которой привязываются измерения.
            fields_to_create (dict): Словарь с полями, которые нужно создать.
            is_virtual (bool): Являются ли создаваемые измерения виртуальными.
        """
        names = list(fields_to_create.keys())
        fields = await get_list_dimension_orm_by_session(self.session, model.tenant_id, None, names)
        existed_names = {field.name: field for field in fields}
        names_to_create = []
        for name in names:
            if name in existed_names and not any(
                exist_model.name == model.name for exist_model in existed_names[name].models
            ):
                existed_names[name].models.append(model)
                continue
            elif name in existed_names:
                continue
            names_to_create.append(name)
        for name_to_create in names_to_create:
            if fields_to_create[name_to_create].get("dimension_ref"):
                dimension_ref = await get_list_dimension_orm_by_session(
                    self.session, model.tenant_id, None, [fields_to_create[name_to_create].get("dimension_ref")]
                )
                if not dimension_ref:
                    logger.debug(
                        "Dimension %s not found. Ignore creating dimension: %s",
                        fields_to_create[name_to_create].get("dimension_ref"),
                        name_to_create,
                    )
                    continue
                fields_to_create[name_to_create]["dimension_ref"] = dimension_ref[0]

            dimension = Dimension(
                name=name_to_create,
                tenant_id=model.tenant_id,
                information_category=InformationCategoryEnum.K3,
                dimension=fields_to_create[name_to_create].get("dimension_ref"),
                labels=[
                    DimensionLabel(
                        language=Language.EN,
                        type=LabelType.SHORT,
                        text=name_to_create or fields_to_create[name_to_create].get("name_eng_short"),
                    ),
                    DimensionLabel(
                        language=Language.EN,
                        type=LabelType.LONG,
                        text=name_to_create or fields_to_create[name_to_create].get("name_eng_long"),
                    ),
                ],
                precision=(
                    fields_to_create[name_to_create]["precision"]
                    if not fields_to_create[name_to_create].get("dimension_ref")
                    else None
                ),
                type=fields_to_create[name_to_create]["type"],
                is_virtual=is_virtual,
            )
            if fields_to_create[name_to_create].get("name_ru_short"):
                dimension.labels.append(
                    DimensionLabel(
                        language=Language.RU,
                        type=LabelType.SHORT,
                        text=fields_to_create[name_to_create]["name_ru_short"],
                    )
                )
            if fields_to_create[name_to_create].get("name_ru_long"):
                dimension.labels.append(
                    DimensionLabel(
                        language=Language.RU,
                        type=LabelType.LONG,
                        text=fields_to_create[name_to_create]["name_ru_long"],
                    )
                )
            dimension.models = [model]
            self.session.add(dimension)
        await self.session.flush()

    async def get_datastorage_orm_by_session(
        self, tenant_id: str, model_name: Optional[str], name: Any
    ) -> Optional[DataStorageOrm]:
        """
        Получает объект хранилища данных ORM по имени и модели.

        Args:
            tenant_id (str): Идентификатор арендатора (тенанта).
            model_name (Optional[str]): Имя модели, к которой относится хранилище данных (может быть None).
            name (Any): Имя хранилища данных.

        Returns:
            Optional[DataStorageOrm]: Объект хранилища данных ORM или None, если не найден.
        """
        query = select(DataStorageOrm).where(
            DataStorageOrm.tenant_id == tenant_id,
            DataStorageOrm.name == name,
        )
        query = (
            query.where(
                DataStorageOrm.models.any(ModelOrm.name == model_name),
            )
            .options(joinedload(DataStorageOrm.log_data_storage))
            .options(
                selectinload(DataStorageOrm.database_objects),
                with_loader_criteria(
                    DatabaseObject,
                    DatabaseObject.models.any(ModelOrm.name == model_name),
                ),
            )
            if model_name
            else query.options(joinedload(DataStorageOrm.log_data_storage)).options(
                selectinload(DataStorageOrm.database_objects)
            )
        )
        result = (await self.session.execute(query)).scalars().one_or_none()
        return result

    async def get_datastorage_orm_list_by_session(
        self, tenant_id: str, model_name: str, pagination: Optional[Pagination] = None
    ) -> Any:
        """
        Получает список хранилищ данных по сеансу.

        Args:
            tenant_id (str): Идентификатор арендатора (тенанта).
            model_name (str): Имя модели, к которой относятся хранилища данных.
            pagination (Optional[Pagination]): Параметры пагинации (если необходимы).

        Returns:
            Any: Результат запроса (обычно список объектов хранилищ данных).
        """
        query = (
            select(DataStorageOrm)
            .where(
                DataStorageOrm.tenant_id == tenant_id,
                DataStorageOrm.models.any(ModelOrm.name == model_name),
            )
            .options(joinedload(DataStorageOrm.log_data_storage))
            .options(
                selectinload(DataStorageOrm.database_objects),
                with_loader_criteria(DatabaseObject, DatabaseObject.models.any(ModelOrm.name == model_name)),
            )
        )
        query = get_select_query_with_offset_limit_order(query, DataStorageOrm.name, pagination)
        result = (await self.session.execute(query)).scalars().all()
        return result

    async def get_list_by_names(self, tenant_id: str, model_name: str, names: list[str]) -> list[DataStorageOrm]:
        """
        Возвращает список DataStorage по именам одним SQL запросом.

        Args:
            tenant_id (str): тенант.
            model_name (str): имя модели.
            names (list[str]): список имен DataStorage.

        Returns:
            list[DataStorageOrm]: список DataStorage ORM.
        """
        if not names:
            return []
        query = (
            select(DataStorageOrm)
            .where(
                DataStorageOrm.tenant_id == tenant_id,
                DataStorageOrm.name.in_(names),
                DataStorageOrm.models.any(ModelOrm.name == model_name),
            )
            .options(joinedload(DataStorageOrm.log_data_storage))
            .options(
                selectinload(DataStorageOrm.database_objects),
                with_loader_criteria(DatabaseObject, DatabaseObject.models.any(ModelOrm.name == model_name)),
            )
        )
        return list((await self.session.execute(query)).scalars().all())

    @timeit
    async def get_datastorage_by_db_object(
        self, tenant_id: str, model_name: str, db_object: DatabaseObjectRequestModel
    ) -> DataStorageModel:
        """
        Получает хранилище данных по объекту базы данных.

        Args:
            tenant_id (str): Идентификатор арендатора (тенанта).
            model_name (str): Имя модели, к которой относится хранилище данных.
            db_object (DatabaseObjectRequestModel): Объект базы данных, по которому ищется хранилище данных.

        Returns:
            DataStorageModel: Найденное хранилище данных.
        """
        query_database_object = select(DatabaseObject).where(
            DatabaseObject.tenant_id == tenant_id,
            DatabaseObject.schema_name == db_object.schema_name,
            DatabaseObject.models.any(ModelOrm.name == model_name),
            DatabaseObject.name == db_object.name,
        )
        database_object = (await self.session.execute(query_database_object)).scalars().one_or_none()
        if not database_object:
            raise NoResultFound("A DataStorage with this database_object has not been found.")
        query_datastorage = (
            select(DataStorageOrm)
            .where(DataStorageOrm.id == database_object.data_storage_id)
            .options(joinedload(DataStorageOrm.log_data_storage))
            .options(
                selectinload(DataStorageOrm.database_objects),
                with_loader_criteria(DatabaseObject, DatabaseObject.models.any(ModelOrm.name == model_name)),
            )
        )
        result = (await self.session.execute(query_datastorage)).scalars().one_or_none()
        model = await self.model_repository.get_model_orm_by_session_with_error(tenant_id, model_name)
        database = DatabaseModel.model_validate(model.database)
        return DataStorageModel.model_validate(
            result,
            context={
                "database_type": database.type,
                "ignore_tech_fields": not model.dimension_tech_fields,
            },
        )

    @timeit
    async def get_list(
        self, tenant_id: str, model_name: str, pagination: Optional[Pagination] = None
    ) -> list[DataStorageModel]:
        """Получить список всех DSO."""
        result = await self.get_datastorage_orm_list_by_session(tenant_id, model_name, pagination)
        model = await self.model_repository.get_model_orm_by_session_with_error(tenant_id, model_name)
        database = DatabaseModel.model_validate(model.database)
        return [
            DataStorageModel.model_validate(
                data_storage,
                context={
                    "database_type": database.type,
                    "ignore_tech_fields": not model.dimension_tech_fields,
                },
            )
            for data_storage in result
        ]

    async def get_orm_by_name(self, tenant_id: str, model_name: Optional[str], name: str) -> DataStorageOrm:
        """
            Получить DSO по имени.
        Args:
            tenant_id: id тенанта
            model_name: имя модели
            name: имя DSO
        Returns:
            DataStorage: модель DSO
        """
        result = await self.get_datastorage_orm_by_session(tenant_id=tenant_id, model_name=model_name, name=name)
        if not result:
            raise NoResultFound(
                f"DataStorage with tenant_id={tenant_id}, model_name={model_name} and name={name} not found."
            )

        return result

    async def get_by_name(self, tenant_id: str, model_name: Optional[str], name: str) -> DataStorageModel:
        """
            Получить DSO по имени.
        Args:
            tenant_id: id тенанта
            model_name: имя модели
            name: имя DSO
        Returns:
            DataStorageModel: модель DSO
        """
        result = await self.get_orm_by_name(tenant_id, model_name, name)
        context = {}
        if model_name:
            model = await self.model_repository.get_model_orm_by_session_with_error(tenant_id, model_name)
            database = DatabaseModel.model_validate(model.database)
            context = {
                "database_type": database.type,
                "ignore_tech_fields": not model.dimension_tech_fields,
            }

        return DataStorageModel.model_validate(result, context=context)

    @timeit
    async def get_id_by_name(self, tenant_id: str, model_name: str, name: str) -> int:
        """
            Получить id DSO по имени.
        Args:
            tenant_id: id тенанта
            model_name: имя модели
            name: имя DSO
        Returns:
            id DSO
        """
        result = await self.get_orm_by_name(tenant_id, model_name, name)
        return result.id

    async def get_id_version_by_name(self, tenant_id: str, model_name: str, name: str) -> tuple[int, int]:
        """
            Получить id и version DSO по имени.
        Args:
            tenant_id: id тенанта.
            model_name: имя модели.
            name: имя DSO.
        Returns:
            tuple[int, int]: (id, version)
        """
        query = select(DataStorageOrm.id, DataStorageOrm.version).where(
            DataStorageOrm.tenant_id == tenant_id,
            DataStorageOrm.name == name,
        )
        if model_name:
            query = query.where(DataStorageOrm.models.any(ModelOrm.name == model_name))
        result = (await self.session.execute(query)).one_or_none()
        if not result:
            raise NoResultFound(
                f"DataStorage with tenant_id={tenant_id}, model_name={model_name} and name={name} not found."
            )
        return result[0], result[1]

    def _generate_sharding_key_by_fields(
        self, fields: list[DataStorageFieldRequestModel] | list[DataStorageField]
    ) -> str:
        """Создать ключ шардирования по полям."""
        sharding_fields = ""
        for field in fields:
            if field.is_sharding_key and not sharding_fields:
                sharding_fields += field.sql_name if field.sql_name else field.name
            elif field.is_sharding_key:
                sharding_fields += f", {field.sql_name}" if field.sql_name else field.name
        if sharding_fields:
            sharding_key = f"cityHash64({sharding_fields})"
        else:
            sharding_key = "rand()"
        return sharding_key

    async def delete_without_commit_by_session(
        self, data_storage: DataStorageOrm, model_name: str, delete: bool = True
    ) -> tuple[Optional[list[DatabaseObjectModel]], Optional[list[DatabaseObjectModel]]]:
        """
        Удаляет хранилище данных без немедленного сохранения изменений в базе данных.

        Args:
            data_storage (DataStorageOrm): Объект хранилища данных ORM.
            model_name (str): Имя модели, связанной с хранилищем данных.
            delete (bool): Нужно ли удалять хранилище данных (по умолчанию True).

        Returns:
            tuple[Optional[list[DatabaseObjectModel]], Optional[list[DatabaseObjectModel]]]:
                Кортеж из двух опциональных списков объектов базы данных:
                - Первый список содержит объекты, которые были удалены.
                - Второй список содержит объекты, которые остались после удаления.
        """
        database_object_for_delete: list[DatabaseObject] = []
        database_object_logs_for_delete: list[DatabaseObject] = []
        if len(data_storage.models) > 1:
            models = list(filter(lambda model: model.name != model_name, data_storage.models))
            if models == data_storage.models:
                raise NoResultFound(
                    f"DataStorage with tenant_id={data_storage.tenant_id}, model_name={model_name} and name={data_storage.name} not found."
                )
            data_storage.models = models
            new_database_objects = []
            database_objects_with_model = get_object_filtred_by_model_name(
                data_storage.database_objects, model_name, True
            )
            database_objects_without_model = get_object_filtred_by_model_name(data_storage.database_objects, model_name)
            for database_object in database_objects_with_model:
                if len(database_object.models) > 1:
                    database_object_models = list(
                        filter(
                            lambda model: model.name != model_name,
                            database_object.models,
                        )
                    )
                    if database_object_models == database_object.models:
                        raise NoResultFound(
                            f"DatabaseObject with tenant_id={database_object.tenant_id}, model_name={model_name} and name={database_object.name} not found."
                        )
                    database_object.models = database_object_models
                    new_database_objects.append(database_object)
                else:
                    database_object_for_delete.append(database_object)
            new_database_objects.extend(database_objects_without_model)
            data_storage.database_objects = new_database_objects
        elif delete:
            if data_storage.log_data_storage:
                database_object_logs_for_delete = data_storage.log_data_storage.database_objects
            await self.session.delete(data_storage)
        await self.session.flush()
        if database_object_for_delete:
            return [
                DatabaseObjectModel.model_validate(database_object) for database_object in database_object_for_delete
            ], None
        if database_object_logs_for_delete:
            return None, [
                DatabaseObjectModel.model_validate(database_object)
                for database_object in database_object_logs_for_delete
            ]
        return None, None

    @timeit
    async def delete_by_name(
        self,
        tenant_id: str,
        model_name: str,
        name: str,
        if_exists: bool = False,
        check_possible_delete: bool = True,
    ) -> None:
        """
        Удаляет объект DataStorage по указанному имени в рамках клиента (tenant).

        Выполняет полное удаление хранилища данных, включая связанные логи и модели.
        При необходимости сохраняет историю изменений и обновляет версию хранилища.

        Args:
            tenant_id (str): Идентификатор тенанта
            model_name (str): Название модели, связанной с хранилищем
            name (str): Имя удаляемого хранилища данных
            if_exists (bool, optional): Если True, игнорирует ошибку при отсутствии хранилища.
                                    По умолчанию False.

        Raises:
            NoResultFound: Если хранилище с указанными параметрами не найдено
            Exception: При ошибках удаления базовых объектов (с логированием)

        Returns:
            None: Операция не возвращает значение, результат зафиксирован в БД
        """
        result: Optional[DataStorageOrm] = await self.get_datastorage_orm_by_session(
            tenant_id=tenant_id, model_name=None, name=name
        )
        if result is None:
            raise NoResultFound(
                f"DataStorage with tenant_id={tenant_id}, model_name={model_name} and name={name} not found."
            )
        update_version_flag = False
        log_data_storage = None
        if result.log_data_storage:
            log_data_storage = await self.get_datastorage_orm_by_session(
                tenant_id=tenant_id, model_name=None, name=result.log_data_storage.name
            )
            result.log_data_storage = log_data_storage
        if len(result.models) > 1:
            await self.data_storage_history_repository.save_history(result, forced=True)
            update_version_flag = True
        else:
            await self.data_storage_history_repository.save_history(result, deleted=True)
        database_object_logs_models = None
        if log_data_storage:
            (
                database_object_logs_models,
                _,
            ) = await self.delete_without_commit_by_session(log_data_storage, model_name, False)
        (
            database_object_models,
            logs_database_objects_for_delete,
        ) = await self.delete_without_commit_by_session(result, model_name)
        model = await self.model_repository.get_model_orm_by_session_with_error(tenant_id, model_name)
        if update_version_flag:
            await self.data_storage_history_repository.update_version(result)
        generator = get_generator(model)
        await generator.delete_datastorage(
            result,
            model,
            database_objects_model=database_object_models,
            exists=if_exists,
            check_possible_delete=check_possible_delete,
        )
        try:
            if database_object_logs_models and result.log_data_storage:
                await generator.delete_datastorage(
                    result.log_data_storage,
                    model,
                    database_objects_model=database_object_logs_models,
                    exists=if_exists,
                    check_possible_delete=check_possible_delete,
                )
            elif logs_database_objects_for_delete:
                await generator.delete_datastorage(
                    None,
                    model,
                    database_objects_model=logs_database_objects_for_delete,
                    exists=if_exists,
                    check_possible_delete=check_possible_delete,
                )
        except Exception as exc:
            logger.exception(
                "Error: deleting database_objects: %s. (%s)",
                logs_database_objects_for_delete,
                log_data_storage,
            )
            _ = await generator.create_datastorage(
                result, model, True, False, check_possible_delete=check_possible_delete
            )
            raise Exception(str(exc))
        await self.session.commit()

    def _get_log_data_storage_fields(self) -> list[DataStorageFieldRequestModel]:
        fields = []
        for column_name in DATA_STORAGE_LOGA_TABLE_FIELDS:
            fields.append(
                DataStorageFieldRequestModel(
                    name=column_name,
                    is_key=column_name
                    in {
                        DataStorageLogsFieldEnum.TIMESTAMP,
                        DataStorageLogsFieldEnum.BATCHID,
                        DataStorageLogsFieldEnum.USERID,
                    },
                    semantic_type=SemanticType.DIMENSION,
                    sql_name=column_name,
                    ref_type=BaseFieldType(
                        ref_object_type=BaseFieldTypeEnum.DIMENSION,
                        ref_object=column_name,
                    ),
                ),
            )
        return fields

    async def create_log_data_storage(
        self,
        data_storage_name: str,
        fields: list[DataStorageFieldRequestModel],
        model_name: str,
        tenant_id: str,
    ) -> DataStorageOrm:
        """
        Создает хранилище данных для журналов.

        Args:
            data_storage_name (str): Имя хранилища данных.
            fields (list[DataStorageFieldRequestModel]): Список полей хранилища данных.
            model_name (str): Имя модели, к которой привязывается хранилище данных.
            tenant_id (str): Идентификатор арендатора (тенанта).

        Returns:
            DataStorageOrm: Созданный объект хранилища данных ORM.
        """
        log_data_storage_name = settings.LOGS_TABLE_PATTERN % data_storage_name
        fields_for_create = self._get_log_data_storage_fields()
        fields_for_create.extend(fields.copy())
        data_storage = DataStorageCreateRequestModel(
            name=log_data_storage_name,
            planning_enabled=False,
            type=DataStorageEnum.TABLE,
            fields=fields_for_create,
        )
        data_storage_log = await self._create_data_storage_orm(
            tenant_id,
            model_name,
            data_storage,
        )
        self.session.add(data_storage_log)
        return data_storage_log

    async def create_virtual_dimensions_for_log_data_storage(self, models: list[ModelOrm]) -> None:
        """
        Создает виртуальные измерения для журналов хранилищ данных.

        Args:
            models (list[ModelOrm]): Список моделей, для которых нужно создать виртуальные измерения.
        """
        for model in models:
            await self.create_fields_dimensions_if_not_exists(
                model,
                {
                    DataStorageLogsFieldEnum.TIMESTAMP: {
                        "precision": DATA_STORAGE_LOG_TABLE_LEGTH[DataStorageLogsFieldEnum.TIMESTAMP],
                        "type": AnyFieldTypeEnum.TIMESTAMP,
                    },
                    DataStorageLogsFieldEnum.BATCHID: {
                        "precision": DATA_STORAGE_LOG_TABLE_LEGTH[DataStorageLogsFieldEnum.BATCHID],
                        "type": AnyFieldTypeEnum.UUID,
                    },
                    DataStorageLogsFieldEnum.ACTION: {
                        "precision": DATA_STORAGE_LOG_TABLE_LEGTH[DataStorageLogsFieldEnum.ACTION],
                        "type": AnyFieldTypeEnum.STRING,
                    },
                    DataStorageLogsFieldEnum.OPERATION: {
                        "precision": DATA_STORAGE_LOG_TABLE_LEGTH[DataStorageLogsFieldEnum.OPERATION],
                        "type": AnyFieldTypeEnum.STRING,
                    },
                    DataStorageLogsFieldEnum.USERID: {
                        "precision": DATA_STORAGE_LOG_TABLE_LEGTH[DataStorageLogsFieldEnum.USERID],
                        "type": AnyFieldTypeEnum.STRING,
                    },
                },
                is_virtual=True,
            )

    async def _create_data_storage_orm(
        self,
        tenant_id: str,
        model_name: str,
        data_storage: DataStorageCreateRequestModel,
        without_relations: bool = False,
    ) -> DataStorageOrm:
        """
        Создает объект хранилища данных ORM по схеме.

        Args:
            tenant_id (str): Идентификатор арендатора (тенанта).
            model_name (str): Имя модели, к которой относится хранилище данных.
            data_storage (DataStorageCreateRequestModel): Схема создания хранилища данных.

        Returns:
            DataStorageOrm: Созданный объект хранилища данных ORM.
        """
        model = await self.model_repository.get_model_orm_by_session_with_error(tenant_id, model_name)
        data_storage_dict = data_storage.model_dump(mode="json")
        data_storage_dict.pop("log_data_storage_name", None)
        data_storage_dict["tenant_id"] = tenant_id
        add_missing_labels(data_storage_dict["labels"], data_storage.name)
        model_names = [model_name]
        data_storage_dict["labels"] = convert_labels_list_to_orm(data_storage_dict["labels"], DataStorageLabel)
        data_storage_dict["fields"] = await self._convert_field_object_list_to_orm(
            tenant_id=tenant_id,
            model_names=model_names,
            fields=data_storage_dict["fields"],
        )
        await self.create_virtual_dimensions_for_log_data_storage([model])
        database_objects = data_storage_dict.pop("database_objects", [])
        data_storage_dict.pop("table", None)
        if not database_objects:
            data_storage.database_objects = self.get_database_objects_by_models(
                data_storage.name, data_storage.type, model
            )
        if not data_storage.database_objects:
            raise ValueError("dbObjects has not been None.")
        data_storage_dict["sharding_key"] = self._generate_sharding_key_by_fields(data_storage.fields)
        data_storage_orm = DataStorageOrm(**data_storage_dict)
        log_data_storage = None
        if data_storage.planning_enabled:
            log_data_storage = await self.create_log_data_storage(
                data_storage.name,
                data_storage.fields,
                model_name,
                tenant_id,
            )
        database_objects = await self.database_object_repository.create_orm_db_objects(
            tenant_id,
            data_storage.database_objects,
            [model],
        )
        data_storage_orm.log_data_storage = log_data_storage
        data_storage_orm.database_objects = database_objects
        if without_relations:
            data_storage_orm.model_relations = []
        data_storage_orm.models = [model]
        return data_storage_orm

    async def generate_physical(
        self,
        tenant_id: str,
        data_storage_orm: DataStorageOrm,
        model: ModelOrm,
        generate_physical_if_not_exists: bool = False,
        generate_on_db: bool = True,
        check_possible_delete: bool = True,
    ) -> None:
        """
        Генерирует физическую структуру хранилища данных для указанной модели.

        Args:
            tenant_id (str): Идентификатор арендатора (тенанта).
            data_storage_orm (DataStorageOrm): Объект хранилища данных ORM.
            model (ModelOrm): Модель, для которой нужно сгенерировать физическую структуру.
            generate_physical_if_not_exists (bool): Нужно ли создавать физическую структуру, если она отсутствует.
        """
        if not model.database:
            raise ValueError(f"Model with name={model.name} and tenant_id={tenant_id} has not been found in database.")
        generator = get_generator(model)
        _ = await generator.create_datastorage(
            data_storage_orm,
            model,
            True,
            check_possible_delete=check_possible_delete,
        )
        if data_storage_orm.log_data_storage:
            log_data_storage: Optional[DataStorageOrm] = await self.get_datastorage_orm_by_session(
                tenant_id=tenant_id, model_name=None, name=data_storage_orm.log_data_storage.name
            )
            try:
                if log_data_storage:
                    _ = await generator.create_datastorage(
                        log_data_storage,
                        model,
                        generate_physical_if_not_exists,
                        check_possible_delete=check_possible_delete,
                    )
            except Exception as exc:
                logger.debug(
                    "Try deleting datastorage name=%s, model=%s, tenant=%s.",
                    data_storage_orm.name,
                    model.name,
                    data_storage_orm.tenant_id,
                )
                await generator.delete_datastorage(
                    data_storage_orm, model, True, False, check_possible_delete=check_possible_delete
                )
                raise Exception(str(exc))

    async def create_by_schema(
        self,
        tenant_id: str,
        model_name: str,
        data_storage: DataStorageCreateRequestModel,
        commit_changes: bool,
        generate_physical_if_not_exists: bool = False,
        generate_on_db: bool = True,
        check_possible_delete: bool = True,
    ) -> tuple[ModelOrm, DatabaseModel, DataStorageOrm]:
        """
        Создает хранилище данных по схеме и возвращает объекты модели, базы данных и хранилища.

        Args:
            tenant_id (str): Идентификатор арендатора (тенанта).
            model_name (str): Имя модели, к которой относится хранилище данных.
            data_storage (DataStorageCreateRequestModel): Схема создания хранилища данных.
            commit_changes (bool): Подтверждать ли изменения в базе данных.
            generate_physical_if_not_exists (bool): Нужно ли создавать физическую структуру, если она отсутствует.

        Returns:
            tuple[ModelOrm, DatabaseModel, DataStorageOrm]: Кортеж из трех объектов: модель, база данных и хранилище данных.
        """
        model = await self.model_repository.get_model_orm_by_session_with_error(tenant_id, model_name)
        data_storage_orm = await self._create_data_storage_orm(
            tenant_id=tenant_id, model_name=model_name, data_storage=data_storage
        )
        self.session.add(data_storage_orm)
        await self.session.flush()
        await self.set_owner_model([data_storage_orm], model)
        await self.data_storage_history_repository.update_version(data_storage_orm, create=True)
        await self.session.flush()
        if generate_on_db:
            await self.generate_physical(
                tenant_id,
                model=model,
                data_storage_orm=data_storage_orm,
                generate_physical_if_not_exists=generate_physical_if_not_exists,
                check_possible_delete=check_possible_delete,
            )
        if commit_changes:
            await self.session.commit()
        returned_data_storage = await self.get_datastorage_orm_by_session(
            tenant_id=tenant_id, model_name=model_name, name=data_storage_orm.name
        )
        if returned_data_storage is None:
            raise ValueError("Datastorage has not been created.")
        database = DatabaseModel.model_validate(model.database)
        return model, database, returned_data_storage

    @timeit
    async def create_and_get_by_schema(
        self,
        tenant_id: str,
        model_name: str,
        data_storage: DataStorageCreateRequestModel,
        generate_physical_if_not_exists: bool = False,
        commit_changes: bool = True,
        generate_on_db: bool = True,
        check_possible_delete: bool = True,
    ) -> DataStorageModel:
        """
        Создает хранилище данных по схеме и возвращает созданный объект.

        Args:
            tenant_id (str): Идентификатор арендатора (тенанта).
            model_name (str): Имя модели, к которой относится хранилище данных.
            data_storage (DataStorageCreateRequestModel): Схема создания хранилища данных.
            generate_physical_if_not_exists (bool): Нужно ли создавать физическую структуру, если она отсутствует.
            commit_changes (bool): Подтверждать ли изменения в базе данных.

        Returns:
            DataStorageModel: Созданное хранилище данных.
        """
        model, database, returned_data_storage = await self.create_by_schema(
            tenant_id,
            model_name,
            data_storage,
            generate_physical_if_not_exists=generate_physical_if_not_exists,
            commit_changes=commit_changes,
            generate_on_db=generate_on_db,
            check_possible_delete=check_possible_delete,
        )
        return DataStorageModel.model_validate(
            returned_data_storage,
            context={
                "database_type": database.type,
                "ignore_tech_fields": not model.dimension_tech_fields,
            },
        )

    async def update_log_data_storage(
        self,
        tenant_id: str,
        original_model: ModelOrm,
        all_models: list[ModelOrm],
        data_storage_dict: dict,
        data_storage: DataStorageOrm,
        original_data_storage_model: DataStorageModel,
        edit_data_storage_model: DataStorageEditRequestModel,
    ) -> None:
        """
        Обновляет логи хранилища данных после внесения изменений.

        Args:
            tenant_id (str): Идентификатор арендатора (тенанта).
            original_model (ModelOrm): Исходная модель, для которой вносились изменения.
            all_models (list[ModelOrm]): Все доступные модели.
            data_storage_dict (dict): Словарь хранилищ данных.
            data_storage (DataStorageOrm): Объект хранилища данных ORM.
            original_data_storage_model (DataStorageModel): Исходная модель хранилища данных.
            edit_data_storage_model (DataStorageEditRequestModel): Новая модель хранилища данных с изменениями.
        """
        planning_enabled = data_storage_dict.get("planning_enabled")
        model_names = [model.name for model in all_models]
        """Обновление log_data_storage таблицы, привязанной к DataStorage."""
        if planning_enabled is not None and not planning_enabled:
            data_storage.log_data_storage = None
        elif planning_enabled and data_storage.log_data_storage is None:
            fields = (
                edit_data_storage_model.fields
                if edit_data_storage_model.fields
                else [
                    DataStorageFieldRequestModel.model_validate(field.model_dump(mode="json"))
                    for field in original_data_storage_model.fields
                ]
            )
            data_storage.log_data_storage = await self.create_log_data_storage(
                data_storage.name,
                fields,
                original_model.name,
                tenant_id,
            )
            all_models = list(filter(lambda model: model.name != original_model.name, all_models))
            _, _ = await self.update_models_datastorage(data_storage.log_data_storage, all_models)
        elif data_storage_dict.get("fields") is not None and planning_enabled and data_storage.log_data_storage:
            log_data_storage = data_storage.log_data_storage
            log_data_storage_fields = [
                field.model_dump(mode="json", exclude_none=True) for field in self._get_log_data_storage_fields()
            ]
            log_data_storage_fields.extend(data_storage_dict.get("fields", []))
            await self._update_fields_in_place(
                data_storage_orm=log_data_storage,
                new_fields_dicts=log_data_storage_fields,
                tenant_id=tenant_id,
                model_names=model_names,
            )
            log_data_storage.sharding_key = self._generate_sharding_key_by_fields(log_data_storage.fields)

    async def clear_datastorage_status(self, datastorage_ids: list[int]) -> None:
        await self.session.execute(
            update(DataStorageModelRelationOrm)
            .where(
                DataStorageModelRelationOrm.data_storage_id.in_(datastorage_ids),
            )
            .values({"status": ModelStatusEnum.PENDING})
        )

    async def set_owner_model(self, datastorages: list[DataStorageOrm], model: ModelOrm) -> None:
        """Обновляет состояние владельца модели."""
        datastorages_ids = []
        for datastorage in datastorages:
            datastorages_ids.append(datastorage.id)
            if datastorage.log_data_storage_id:
                datastorages_ids.append(datastorage.log_data_storage_id)
        await self.session.execute(
            update(DataStorageModelRelationOrm)
            .where(
                DataStorageModelRelationOrm.data_storage_id.in_(datastorages_ids),
            )
            .values({"is_owner": False})
        )
        await self.session.execute(
            update(DataStorageModelRelationOrm)
            .where(
                DataStorageModelRelationOrm.data_storage_id.in_(datastorages_ids),
                DataStorageModelRelationOrm.model_id == model.id,
            )
            .values({"is_owner": True})
        )

    async def update_datastorage_orm(
        self,
        tenant_id: str,
        model_name: str,
        name: str,
        data_storage: DataStorageEditRequestModel,
    ) -> DataStorageOrm:
        """
        Обновляет хранилище данных в базе данных по имени и схеме.

        Args:
            tenant_id (str): Идентификатор арендатора (тенанта).
            model_name (str): Имя модели, к которой относится хранилище данных.
            name (str): Имя хранилища данных, которое нужно обновить.
            data_storage (DataStorageEditRequestModel): Схема редактирования хранилища данных.

        Returns:
            DataStorageOrm: Обновленное хранилище данных.
        """
        data_storage_dict = data_storage.model_dump(mode="json", exclude_none=True, exclude_unset=True)
        data_storage_dict.pop("log_data_storage_name", None)
        original_data_storage = await self.get_datastorage_orm_by_session(
            tenant_id=tenant_id, model_name=None, name=name
        )
        if original_data_storage is None:
            raise NoResultFound(
                f"DataStorage with tenant_id={tenant_id}, model_name={model_name} and name={name} not found."
            )
        model = next(filter(lambda m: m.name == model_name, original_data_storage.models), None)
        if not model:
            raise ValueError("Model is None")

        data_storage_dict.pop("table", {})
        data_storage_dict.pop("database_objects", [])
        if data_storage.database_objects is not None:
            update_database_objects_schema_for_model(
                original_data_storage.database_objects,
                model_name,
                data_storage.database_objects,
            )
        fields_payload = data_storage_dict.get("fields")
        if fields_payload is not None and data_storage.fields is not None:
            original_data_storage.sharding_key = self._generate_sharding_key_by_fields(data_storage.fields)
            await self._update_fields_in_place(
                data_storage_orm=original_data_storage,
                new_fields_dicts=copy.deepcopy(fields_payload),
                tenant_id=tenant_id,
                model_names=[model.name for m in original_data_storage.models],
            )

        if data_storage_dict.get("labels") is not None:
            add_missing_labels(data_storage_dict["labels"], name)
            original_data_storage.labels = convert_labels_list_to_orm(
                data_storage_dict.pop("labels"),
                DataStorageLabel,
            )
        await self.update_log_data_storage(
            tenant_id,
            model,
            original_data_storage.models,
            data_storage_dict,
            original_data_storage,
            DataStorageModel.model_validate(original_data_storage),
            data_storage,
        )
        data_storage_dict.pop("fields", [])
        await self.clear_datastorage_status([original_data_storage.id])
        if data_storage_dict:
            await self.session.execute(
                update(DataStorageOrm)
                .where(
                    DataStorageOrm.tenant_id == tenant_id,
                    DataStorageOrm.name == name,
                    DataStorageOrm.models.any(ModelOrm.name == model_name),
                )
                .values(data_storage_dict)
            )
        return original_data_storage

    async def update_data_storage_in_db(
        self,
        models_dict: dict,
        model_name: str,
        data_storage: DataStorageOrm,
        enable_delete_not_empty: bool = False,
    ) -> None:
        """
        Обновляет хранилище данных в базе данных для указанной модели.

        Args:
            models_dict (dict): Словарь моделей, где ключ — имя модели, значение — объект модели.
            model_name (str): Имя модели, для которой нужно обновить хранилище данных.
            data_storage (DataStorageOrm): Объект хранилища данных, который нужно обновить.
        """
        generator = get_generator(models_dict[model_name])
        updated_db = set()
        await generator.update_datastorage(
            data_storage,
            models_dict[model_name],
            enable_delete_not_empty=enable_delete_not_empty,
        )
        model_model = ModelModel.model_validate(models_dict[model_name])
        if model_model.database is None:
            raise ValueError("Database is None")
        updated_db.add((model_model.schema_name, model_model.database.name))
        models_dict.pop(model_name)
        for _, model in models_dict.items():
            model_model = ModelModel.model_validate(models_dict[model.name])
            if model_model.database is None:
                raise ValueError("Database is None")
            if (model_model.schema_name, model_model.database.name) in updated_db:
                logger.debug(
                    "Datastorage %s for database %s and schema %s has already been updated",
                    data_storage.name,
                    model_model.database.name,
                    model_model.schema_name,
                )
                continue
            updated_db.add((model_model.schema_name, model_model.database.name))
            generator = get_generator(models_dict[model.name])
            await generator.update_datastorage(
                data_storage,
                model,
                enable_delete_not_empty=enable_delete_not_empty,
            )
        return None

    @timeit
    async def update_by_name_and_schema(
        self,
        tenant_id: str,
        model_name: str,
        name: str,
        data_storage_edit_model: DataStorageEditRequestModel,
        generate_on_db: bool = True,
        enable_delete_not_empty: bool = False,
    ) -> DataStorageModel:
        """
        Обновляет хранилище данных по имени и схеме.

        Args:
            tenant_id (str): Идентификатор арендатора (тенанта).
            model_name (str): Имя модели, к которой относится хранилище данных.
            name (str): Имя хранилища данных, которое нужно обновить.
            data_storage_edit_model (DataStorageEditRequestModel): Схема редактирования хранилища данных.

        Returns:
            DataStorageModel: Обновленное хранилище данных.
        """
        prev_field_types_by_database: dict[str, dict[str, str]] = {}
        data_storage_edit_model
        prev_data_storage = await self.get_datastorage_orm_by_session(tenant_id=tenant_id, model_name=None, name=name)
        if prev_data_storage is None:
            raise NoResultFound(
                f"DataStorage with tenant_id={tenant_id}, model_name={model_name} and name={name} not found."
            )
        data_storage_dict = data_storage_edit_model.model_dump(mode="json", exclude_none=True, exclude_unset=True)
        prev_log_data_storage = None
        if prev_data_storage.log_data_storage:
            prev_log_data_storage = await self.get_datastorage_orm_by_session(
                tenant_id=tenant_id,
                model_name=None,
                name=prev_data_storage.log_data_storage.name,
            )
            prev_data_storage.log_data_storage = prev_log_data_storage
        await self.data_storage_history_repository.save_history(prev_data_storage, edit_model=data_storage_dict)
        prev_data_storage_model = DataStorageModel.model_validate(prev_data_storage)
        models: list[ModelOrm] = await self.model_repository.get_list_orm_by_names_and_session(
            tenant_id,
            [model_status.name for model_status in prev_data_storage_model.models_statuses],
        )
        return_model = None
        for model in models:
            if model.name == model_name:
                return_model = model
        models_dict = {model.name: model for model in models}
        if generate_on_db:
            for model in models:
                database_model = DatabaseModel.model_validate(model.database)
                generator = get_generator(model)
                if database_model.name not in prev_field_types_by_database:
                    prev_field_types_by_database[database_model.name] = {}
                for field in prev_data_storage.fields:
                    prev_field_types_by_database[database_model.name][field.name] = generator._get_table_field_type(
                        field, database_model.type, without_null=True
                    )
        data_storage = await self.update_datastorage_orm(
            tenant_id=tenant_id,
            model_name=model_name,
            name=name,
            data_storage=data_storage_edit_model,
        )
        await self.session.flush()
        await self.data_storage_history_repository.update_version(data_storage)
        await self.session.flush()
        if generate_on_db:
            await self.update_data_storage_in_db(
                models_dict,
                model_name,
                data_storage,
                enable_delete_not_empty,
            )

            if prev_data_storage_model.log_data_storage_name is None and data_storage.log_data_storage:
                await self.create_data_storage_in_db_by_models(
                    data_storage.log_data_storage, models, check_possible_delete=not enable_delete_not_empty
                )
            elif (
                prev_data_storage_model.log_data_storage_name is not None
                and data_storage.log_data_storage is None
                and prev_log_data_storage
            ):
                deleted_db = set()
                for model in models:
                    model_model = ModelModel.model_validate(model)
                    if model_model.database is None:
                        raise ValueError("Database is None")
                    if (model_model.schema_name, model_model.database.name) in deleted_db:
                        logger.debug(
                            "Datastorage %s for database %s and schema %s has already been deleted",
                            data_storage.name,
                            model_model.database.name,
                            model_model.schema_name,
                        )
                        continue
                    deleted_db.add((model_model.schema_name, model_model.database.name))
                    generator = get_generator(model)
                    database_object_models = get_filtred_database_object_by_data_storage(
                        prev_log_data_storage, model.name
                    )
                    await generator.delete_datastorage(
                        prev_log_data_storage,
                        model,
                        database_objects_model=database_object_models,
                        check_possible_delete=not enable_delete_not_empty,
                    )
            elif data_storage.log_data_storage:
                for model in models:
                    if model.database is None:
                        raise ValueError("Database is None")
                    generator = get_generator(model)
                    await generator.update_datastorage(
                        data_storage.log_data_storage,
                        model,
                        enable_delete_not_empty=enable_delete_not_empty,
                    )

        await self.session.commit()
        result_data_storage = await self.get_datastorage_orm_by_session(
            tenant_id=tenant_id, model_name=model_name, name=name
        )
        if hasattr(result_data_storage, "log_data_storage_name"):
            delattr(result_data_storage, "log_data_storage_name")
        if not result_data_storage:
            raise ValueError("Error saving DataStorage")
        if return_model:
            database = DatabaseModel.model_validate(return_model.database)
            context = {
                "database_type": database.type,
                "ignore_tech_fields": not return_model.dimension_tech_fields,
            }
        filtered_database_objects = get_object_filtred_by_model_name(
            result_data_storage.database_objects, model_name, True
        )
        data_storage_model = DataStorageModel.model_validate(result_data_storage, context=context)
        data_storage_model.database_objects = [
            DatabaseObjectModel.model_validate(db_object) for db_object in filtered_database_objects
        ]
        return data_storage_model

    async def update_models_datastorage(
        self,
        data_storage: DataStorageOrm,
        models: list[ModelOrm],
    ) -> tuple[list, list]:
        """
        Обновляет хранилище данных для множества моделей.

        Args:
            data_storage (DataStorageOrm): Объект хранилища данных, которое нужно обновить.
            models (list[ModelOrm]): Список моделей, для которых нужно обновить хранилище данных.

        Returns:
            tuple[list, list]: Кортеж из двух списков:
                - Первый список содержит успешно обновленные модели.
                - Второй список содержит ошибки, возникшие при обновлении.
        """
        original_databases_database_objects_dict, appended_database_objects = (
            get_database_schema_database_object_mapping(data_storage)
        )
        data_storage.models.extend(models)
        not_ignored = []
        ignored = []
        tenant_id = data_storage.tenant_id
        for model in models:
            model_model = ModelModel.model_validate(model)
            if model_model.database is None:
                raise ValueError("Database is None")
            if (
                model_model.database.name,
                model_model.schema_name,
            ) not in original_databases_database_objects_dict:
                not_ignored.append(model)
                database_objects = self.get_database_objects_by_models(data_storage.name, data_storage.type, model)
                new_database_objects = await self.database_object_repository.create_orm_db_objects(
                    tenant_id,
                    database_objects,
                    [model],
                    data_storage.id,
                )
                data_storage.database_objects.extend(new_database_objects)
                for new_database_object in new_database_objects:
                    if (
                        new_database_object.name
                        not in appended_database_objects[(model_model.database.name, model_model.schema_name)]
                    ):
                        appended_database_objects[(model_model.database.name, model_model.schema_name)].add(
                            new_database_object.name
                        )
                        original_databases_database_objects_dict[
                            (model_model.database.name, model_model.schema_name)
                        ].append(new_database_object)
            else:
                ignored.append(model)
                original_database_objects = original_databases_database_objects_dict[
                    (model_model.database.name, model_model.schema_name)
                ]
                for database_object in original_database_objects:
                    if database_object.models is None:
                        raise ValueError("database_object.models is None")
                    database_object.models.append(model)
        return not_ignored, ignored

    async def copy_model_data_storage_orm_by_session(
        self,
        tenant_id: str,
        name: str,
        models: list[ModelOrm],
        data_storage: Optional[DataStorageOrm] = None,
        ignore_field_name: Optional[str] = None,
        save_history: bool = True,
        validate: bool = True,
    ) -> tuple[Optional[DataStorageOrm], list[ModelOrm]]:
        """
        Копирует хранилище данных (`DataStorage`) в указанные модели.

        Args:
            tenant_id (str): Идентификатор арендатора.
            name (str): Имя хранилища данных (`data storage`), которое нужно скопировать.
            models (list[Model]): Список моделей, куда будет выполнен перенос хранилища данных.
            data_storage (Optional[DataStorage]): Хранилище данных, используемое для копирования (если не указано, используется текущее).
            ignore_field_name (Optional[str]): Поле, которое игнорируется при копировании.
            save_history (bool): Сохранять ли историю изменения хранилища данных.
            validate (bool): Провести проверку валидности перед копированием.

        Returns:
            tuple[Optional[DataStorage], list[Model]]: Кортеж, содержащий:
                - Новое хранилище данных после копирования (или None, если копирование невозможно),
                - Список моделей, в которые было успешно произведено копирование.
        """

        if not data_storage:
            data_storage = await self.get_datastorage_orm_by_session(
                tenant_id=tenant_id,
                model_name=None,
                name=name,
            )
        if not data_storage:
            return None, []
        if data_storage.log_data_storage:
            log_data_storage = await self.get_datastorage_orm_by_session(
                tenant_id=tenant_id,
                model_name=None,
                name=data_storage.log_data_storage.name,
            )
            data_storage.log_data_storage = log_data_storage
        data_storage_model = DataStorageModel.model_validate(data_storage)
        data_storage_dict = data_storage_model.model_dump(mode="json")
        model_names = [model.name for model in models]
        if validate:
            _ = await self._convert_field_object_list_to_orm(
                tenant_id, model_names, data_storage_dict["fields"], ignore_field_name
            )
        orig_model_names = [model_status.name for model_status in data_storage_model.models_statuses]
        for model_name in model_names:
            if model_name in orig_model_names:
                raise ValueError(f"DataStorage already exists in Model with name={model_name}.")
        if save_history:
            await self.data_storage_history_repository.save_history(data_storage, forced=True)
        models = await self.model_repository.get_list_orm_by_names_and_session(tenant_id, model_names)
        not_ignored, ignored = await self.update_models_datastorage(data_storage, models)
        logger.debug(
            "Datastorage is copied with the creation of physical objects: %s. Copied without creation: %s",
            not_ignored,
            ignored,
        )
        return data_storage, not_ignored

    async def create_data_storage_in_db_by_models(
        self,
        data_storage: DataStorageOrm,
        models: list[ModelOrm],
        if_not_exist: bool = False,
        check_possible_delete: bool = True,
    ) -> None:
        """
        Создает физическое представление хранилища данных для каждой модели.

        Args:
            data_storage (DataStorageOrm): Объект хранилища данных, для которого генерируется физическая структура.
            models (list[ModelOrm]): Список моделей, для которых нужно создать физическую структуру хранилища.
        """
        for model in models:
            await self.generate_physical(
                tenant_id=data_storage.tenant_id,
                model=model,
                data_storage_orm=data_storage,
                generate_physical_if_not_exists=if_not_exist,
                check_possible_delete=check_possible_delete,
            )

    async def copy_model_data_storage(
        self,
        tenant_id: str,
        name: str,
        model_names: list[str],
        generated_in_db: bool = True,
        if_not_exist: bool = False,
        check_possible_delete: bool = True,
    ) -> DataStorageModel:
        """
        Копирует хранилище данных для указанных моделей.

        Args:
            tenant_id (str): Идентификатор арендатора (тенанта).
            name (str): Имя хранилища данных, которое нужно скопировать.
            model_names (list[str]): Список имен моделей, для которых копируется хранилище данных.

        Returns:
            DataStorageModel: Скопированное хранилище данных.
        """
        models: list[ModelOrm] = await self.model_repository.get_list_orm_by_names_and_session(tenant_id, model_names)
        exceptions = []
        exist_models = [model.name for model in models]
        for model_name in model_names:
            if model_name not in exist_models:
                exceptions.append(model_name)
        if exceptions:
            raise NoResultFound(f"Models not found: {', '.join(exceptions)}")
        models = sorted(models, key=lambda model: model_names.index(model.name))
        await self.create_virtual_dimensions_for_log_data_storage(models)
        (
            data_storage,
            not_ignored_models,
        ) = await self.copy_model_data_storage_orm_by_session(tenant_id, name, models)
        if not data_storage:
            raise NoResultFound(f"DataStorage with tenant_id={tenant_id} and name={name} not found.")
        await self.session.flush()
        log_data_storage = None
        if data_storage.log_data_storage:
            (
                log_data_storage,
                not_ignored_models_logs,
            ) = await self.copy_model_data_storage_orm_by_session(
                tenant_id,
                data_storage.log_data_storage.name,
                models,
                save_history=False,
            )
            await self.session.flush()
        await self.data_storage_history_repository.update_version(data_storage)
        if generated_in_db:
            await self.create_data_storage_in_db_by_models(
                data_storage,
                not_ignored_models,
                if_not_exist=if_not_exist,
                check_possible_delete=check_possible_delete,
            )
            if log_data_storage:
                await self.create_data_storage_in_db_by_models(
                    log_data_storage,
                    not_ignored_models_logs,
                    if_not_exist=if_not_exist,
                    check_possible_delete=check_possible_delete,
                )
        await self.session.commit()
        await self.session.refresh(data_storage)
        return DataStorageModel.model_validate(data_storage)

    @classmethod
    def get_by_session(cls, session: AsyncSession) -> "DataStorageRepository":
        model_repository = ModelRepository.get_by_session(session)
        database_object_repository = DatabaseObjectRepository(session)
        return cls(session, model_repository, database_object_repository)
