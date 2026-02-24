"""
Репозиторий характеристик.
"""

import copy
import json
from collections import defaultdict
from collections.abc import Sequence
from typing import Any, Optional, TypedDict

from py_common_lib.logger import EPMPYLogger
from py_common_lib.utils import timeit
from sqlalchemy import Select, select, update
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

from src.config import settings
from src.db import HierarchyBaseDimension, HierarchyMeta
from src.db.data_storage import DataStorage, DataStorageField, DataStorageModelRelation
from src.db.dimension import (
    AIPrompt,
    Dimension,
    DimensionAttribute,
    DimensionAttributeLabel,
    DimensionLabel,
    DimensionModelRelation as DimensionModelRelationOrm,
    PVDctionary,
    TextLink,
)
from src.db.measure import Measure
from src.db.model import Model
from src.integration.pv_dictionaries.client import ClientPVDictionaries
from src.integration.pv_dictionaries.models import PVDictionary, PVDictionaryType
from src.models.any_field import AnyFieldTypeEnum
from src.models.consts import DATEFROM, DATETO, LANGUAGE_FIELD, TEXT_TO_LENGTH
from src.models.data_storage import (
    DataStorage as DataStorageModel,
    DataStorageEnum,
    DataStorageLogsFieldEnum,
    HierarchyDimensionsEnum,
)
from src.models.database_object import DatabaseObject as DatabaseObjectModel
from src.models.dimension import (
    DefaultDimensionEnum,
    Dimension as DimensionModel,
    DimensionAttributeRequest,
    DimensionCreateRequest as DimensionCreateRequestModel,
    DimensionEditRequest as DimensionEditRequestModel,
    DimensionTextFieldEnum,
    DimensionTypeEnum,
    TechDimensionEnum,
    TextEnum,
)
from src.models.enum import InformationCategoryEnum
from src.models.field import BaseFieldType, BaseFieldTypeEnum, SemanticType
from src.models.model import Model as ModelModel, ModelStatusEnum
from src.models.request_params import Pagination
from src.repository.composite import CompositeRepository
from src.repository.data_storage import DataStorageRepository
from src.repository.database_object import DatabaseObjectRepository
from src.repository.generators.clickhouse_generator import GeneratorClickhouseRepository
from src.repository.generators.postgresql_generator import GeneratorPostgreSQLRepository
from src.repository.generators.utils import get_generator
from src.repository.graph import GraphRepository
from src.repository.history.dimension import DimensionHistoryRepository
from src.repository.measure import MeasureRepository
from src.repository.model import ModelRepository
from src.repository.model_relations import ModelRelationsRepository
from src.repository.utils import (
    add_missing_labels,
    check_exists_object_in_models,
    convert_anyfield_dict_to_orm,
    convert_labels_list_to_orm,
    get_and_compare_model_name_by_priority,
    get_list_dimension_orm_by_session,
    get_list_of_measures_orm_by_session,
    get_measure_orm_model_by_session,
    get_select_query_with_offset_limit_order,
    is_ignore_dimension,
)
from src.utils.exceptions import CreatePVDException
from src.utils.validators import snake_to_camel

logger = EPMPYLogger(__name__)


class NotVirtualDimensionDictCreate(TypedDict):
    name: str
    texts: list[TextEnum]
    type: DimensionTypeEnum
    precision: int


class DimensionRepository:

    def __init__(
        self,
        session: AsyncSession,
        model_repository: ModelRepository,
        datastorage_repository: DataStorageRepository,
        database_object_repository: DatabaseObjectRepository,
        model_relations_repository: ModelRelationsRepository,
        measure_repository: MeasureRepository,
    ) -> None:
        self.session = session
        self.model_repository = model_repository
        self.datastorage_repository = datastorage_repository
        self.model_relations_repository = model_relations_repository
        self.database_object_repository = database_object_repository
        self.dimension_history_repository = DimensionHistoryRepository(session)
        self.measure_repository = measure_repository
        self.graph_repository = GraphRepository(session)

    def convert_pv_dictionaries_dict_to_orm_model(self, pv_dictionary: dict) -> PVDctionary:
        return PVDctionary(
            object_id=pv_dictionary["object_id"],
            object_type=PVDictionaryType.DIMENSION,
            object_name=pv_dictionary["object_name"],
            domain_name=pv_dictionary["domain_name"],
            domain_label=pv_dictionary["domain_label"],
        )

    async def get_ref_tables_dimensions_by_dimension_or_dimensions(
        self, target_dimension: Dimension, dimensions: Optional[Sequence[Dimension]] = None
    ) -> dict[str, Optional[str]]:
        """
        Ищет таблицы у dimension на который ссылается target_dimension.

        Args:
            target_dimension (Dimension): Объект Dimension, для которого ищем таблицы.
            dimensions (Optional[Sequence[Dimension]]): Дополнительные объекты Dimension (если ранее уже запрашивали все dimension)

        Returns:
            dict[str, Optional[str]]: Словарь с именами найденных таблиц:
                - "ref_attributes_table_name": имя атрибутной таблицы или None.
                - "ref_text_table_name": имя текстовой таблицы или None.
                - "ref_values_table_name": имя таблицы значений или None.
        """
        dimensions_cache = {}
        if dimensions:
            dimensions_cache = {dimension.id: dimension for dimension in dimensions}

        current = target_dimension
        visited = set()
        while (
            current.text_table is None
            and current.attributes_table is None
            and current.values_table is None
            and current.dimension_id is not None
        ):
            if current.dimension_id in visited:
                # Защита от циклов в ссылках
                break
            visited.add(current.dimension_id)

            if dimensions_cache:
                parent = dimensions_cache.get(current.dimension_id)
            else:
                parent = await self.get_orm_by_id(current.dimension_id)

            if parent is None:
                break
            current = parent

        return {
            "ref_attributes_table_name": current.attributes_table.name if current.attributes_table else None,
            "ref_text_table_name": current.text_table.name if current.text_table else None,
            "ref_values_table_name": current.values_table.name if current.values_table else None,
            "ref_texts": getattr(current, "texts", None),
            "ref_attributes": getattr(current, "attributes", None),
            "ref_pv_dictionary": getattr(current, "pv_dictionary", None),
            "ref_values_table": getattr(current, "values_table", None),
            "ref_attributes_table": getattr(current, "attributes_table", None),
            "ref_text_table": getattr(current, "text_table", None),
            "ref_case_sensitive": getattr(current, "case_sensitive", False),  # type: ignore[dict-item]
            "ref_texts_language_dependency": getattr(current, "texts_language_dependency", False),  # type: ignore[dict-item]
            "ref_texts_time_dependency": getattr(current, "texts_time_dependency", False),  # type: ignore[dict-item]
            "ref_attributes_time_dependency": getattr(current, "attributes_time_dependency", False),  # type: ignore[dict-item]
            "ref_data_access_method": getattr(current, "data_access_method", None),  # type: ignore[dict-item]
            "ref_is_virtual": getattr(current, "is_virtual", False),  # type: ignore[dict-item]
            "ref_business_key_representation": getattr(current, "business_key_representation", None),
            "ref_auth_relevant": getattr(current, "auth_relevant", False),  # type: ignore[dict-item]
            "ref_precision": getattr(current, "precision", None),
        }

    async def get_related_dimensions_by_dimension_id(self, dimension_id: int) -> Sequence[str]:
        """Получить список Dimension, которые ссылаются на dimension_id."""
        query = (
            select(Dimension.name)
            .select_from(DimensionAttribute)
            .join(Dimension, Dimension.id == DimensionAttribute.dimension_id)
            .where(DimensionAttribute.dimension_attribute_id == dimension_id)
        )
        result = (await self.session.execute(query)).unique().scalars().all()
        return result

    async def get_related_dimensions_by_dimensions(self, dimensions_id: list[int]) -> dict[int, list[str]]:
        """Получить словарь взаимосвязи всех Dimenion друг с другом."""
        query = (
            select(Dimension.name, DimensionAttribute.dimension_attribute_id)
            .select_from(DimensionAttribute)
            .join(Dimension, Dimension.id == DimensionAttribute.dimension_id)
            .where(DimensionAttribute.dimension_attribute_id.in_(dimensions_id))
        )
        result = (await self.session.execute(query)).unique().mappings().all()
        rusult_dict = defaultdict(list)
        for dimension_value in result:
            rusult_dict[dimension_value["dimension_attribute_id"]].append(dimension_value["name"])
        return rusult_dict

    async def get_dimension_id_by_name(self, tenant_id: str, name: str, model_name: Optional[str] = None) -> int | None:
        query = select(Dimension.id).where(
            Dimension.name == name,
            Dimension.tenant_id == tenant_id,
        )
        query = query.where(Dimension.models.any(Model.name == model_name)) if model_name else query
        result = (await self.session.execute(query)).unique().scalars().one_or_none()
        return result

    @timeit
    async def get_pv_dictionary_object_names_by_dimension_names(
        self,
        tenant_id: str,
        model_name: str,
        names: list[str],
    ) -> dict[str, str]:
        """
        Возвращает соответствие имени измерения и object_name его PV Dictionary.

        Метод выполняет один batch-запрос только по нужным полям и не загружает тяжелый ORM-граф Dimension.
        """
        if not names:
            return {}

        query = (
            select(Dimension.name, PVDctionary.object_name)
            .select_from(Dimension)
            .join(
                DimensionModelRelationOrm,
                DimensionModelRelationOrm.dimension_id == Dimension.id,
            )
            .join(Model, Model.id == DimensionModelRelationOrm.model_id)
            .join(PVDctionary, PVDctionary.id == Dimension.pv_dictionary_id)
            .where(
                Dimension.tenant_id == tenant_id,
                Model.name == model_name,
                Dimension.name.in_(names),
            )
        )
        rows = (await self.session.execute(query)).all()

        result: dict[str, str] = {}
        for name, object_name in rows:
            if name not in result:
                result[name] = object_name
        return result

    async def get_dimension_orm_model(self, tenant_id: str, name: str, model_name: Optional[str] = None) -> Any:
        """
        Получает объект измерения ORM по имени и (при наличии) имени модели.

        Args:
            tenant_id (str): Идентификатор арендатора (тенанта).
            name (str): Имя измерения.
            model_name (Optional[str]): Имя модели, к которой привязано измерение (если применимо).

        Returns:
            Any: Объект измерения ORM или None, если не найден.
        """
        query = (
            select(Dimension)
            .options(joinedload(Dimension.dimension))
            .options(
                joinedload(Dimension.values_table).options(
                    selectinload(DataStorage.fields).options(
                        joinedload(DataStorageField.dimension),
                        joinedload(DataStorageField.measure),
                        joinedload(DataStorageField.any_field),
                    )
                )
            )
            .options(
                joinedload(Dimension.text_table).options(
                    selectinload(DataStorage.fields).options(
                        joinedload(DataStorageField.dimension),
                        joinedload(DataStorageField.measure),
                        joinedload(DataStorageField.any_field),
                    )
                )
            )
            .options(
                joinedload(Dimension.attributes_table).options(
                    selectinload(DataStorage.fields).options(
                        joinedload(DataStorageField.dimension),
                        joinedload(DataStorageField.measure),
                        joinedload(DataStorageField.any_field),
                    )
                )
            )
            .where(
                Dimension.name == name,
                Dimension.tenant_id == tenant_id,
            )
        )
        query = self._get_joined_hierarchies(query)
        query = query.where(Dimension.models.any(Model.name == model_name)) if model_name else query
        result = (await self.session.execute(query)).unique().scalars().one_or_none()
        return result

    async def get_dimension_orm_model_by_session_with_error(
        self, tenant_id: str, name: str, model_names: Optional[list[str]]
    ) -> Dimension:
        """
        Получает объект измерения ORM по имени и (при наличии) именам моделей.
        Если измерение не найдено, выбрасывает исключение.

        Args:
            tenant_id (str): Идентификатор арендатора (тенанта).
            name (str): Имя измерения.
            model_names (Optional[list[str]]): Список имен моделей, к которым привязано измерение (если применимо).

        Raises:
            NotFoundError: Если измерение не найдено.

        Returns:
            Dimension: Объект измерения ORM.
        """
        result = await self.get_dimension_orm_model(
            tenant_id=tenant_id,
            name=name,
            model_name=None,
        )
        if not result:
            raise NoResultFound(f"""Dimension with tenant_id={tenant_id} and name={name} not found.""")
        if model_names:
            check_exists_object_in_models(result, model_names)
        return result

    async def get_dimension_orm_model_if_not_present_in_models(
        self, tenant_id: str, name: str, model_names: Optional[list[str]]
    ) -> Optional[Dimension]:
        """
        Возвращает объект измерения (`Dimension`), если оно отсутствует во всех указанных моделях.

        Args:
            tenant_id (str): Идентификатор арендатора.
            name (str): Имя измерения (`Dimension`), которое проверяется.
            model_names (Optional[list[str]]): Список названий моделей, в которых проверяется наличие измерения.

        Returns:
            Dimension: Объект измерения, если оно отсутствует во всех указанных моделях, иначе исключение.
        """
        result = await self.get_dimension_orm_model(
            tenant_id=tenant_id,
            name=name,
            model_name=None,
        )
        if model_names:
            try:
                check_exists_object_in_models(result, model_names)
            except NoResultFound:
                ...
            else:
                return None
        if result:
            return result
        raise NoResultFound(f"""Dimension with tenant_id={tenant_id} and name={name} not found.""")

    async def create_pv_dictionary_by_dimension(
        self,
        tenant_id: str,
        name: str,
        pv_dictionary: Optional[PVDictionary] = None,
        with_error: bool = True,
        commit: bool = False,
    ) -> DimensionModel:
        """Добавить PV Dictionary в dimension."""
        dimension = await self.get_dimension_orm_model_by_session_with_error(tenant_id, name, None)
        if dimension.is_virtual:
            if with_error:
                raise ValueError(
                    f"It is not possible to create a Dimension {name} because it is a reference type or a virtual type."
                )
            else:
                logger.info(
                    "It is not possible to create a Dimension %s because it is a reference type or a virtual type. Skip...",
                    name,
                )
                return DimensionModel.model_validate(dimension)
        await self.create_pv_dictionary(
            dimension,
            pv_dictionary.model_dump(mode="json") if pv_dictionary else None,
            commit=commit,
        )
        if not commit:
            await self.session.commit()
        dimension = await self.get_dimension_orm_model_by_session_with_error(tenant_id, name, None)
        return DimensionModel.model_validate(dimension)

    async def update_pv_dictionary_by_dimension(
        self,
        tenant_id: str,
        name: str,
        pv_dictionary: Optional[PVDictionary] = None,
    ) -> DimensionModel:
        """Обновить PV Dictionary у dimension."""
        dimension = await self.get_dimension_orm_model_by_session_with_error(tenant_id, name, None)
        if dimension.is_virtual:
            raise ValueError("It is not possible to update a pvd in a virtual dimension or a reference dimension.")
        await self.update_pv_dictionary(
            dimension,
            pv_dictionary.model_dump(mode="json") if pv_dictionary else None,
        )
        await self.session.commit()
        return DimensionModel.model_validate(dimension)

    async def _update_text_data_storage_info_by_dimension(
        self,
        tenant_id: str,
        text_table_id: Optional[str],
        dimension: dict,
        is_create: bool,
        model: Model,
        original_dimension: Optional[Dimension] = None,
    ) -> tuple[Optional[DataStorage], Any]:
        """Создает или обновляет text_table Datastorage в зависимости от содержания "texts"."""
        if len(dimension["texts"]) == 0:
            text_table_id = None
            if original_dimension:
                original_dimension.text_table = None
            return None, None
        if text_table_id is None:
            return None, None
        ds_fields = [self.datastorage_repository.create_dimension_data_storage_field_dict(dimension["name"])]
        if dimension.get("texts_language_dependency") is None and original_dimension:
            dimension["texts_language_dependency"] = original_dimension.texts_language_dependency
        if dimension.get("texts_language_dependency"):
            ds_fields.append(
                self.datastorage_repository.create_dimension_data_storage_field_dict(
                    dimension_name=DefaultDimensionEnum.LANGUAGE_TAG, ds_field_name=LANGUAGE_FIELD
                )
            )
        if dimension.get("texts_time_dependency") is None and original_dimension:
            dimension["texts_time_dependency"] = original_dimension.texts_time_dependency
        if dimension.get("texts_time_dependency"):
            field_names = (DATEFROM, DATETO)
            for field_name in field_names:
                is_key = field_name == DATETO
                ds_fields.append(
                    self.datastorage_repository.create_dimension_data_storage_field_dict(field_name, is_key)
                )
        for text in dimension["texts"]:
            ds_fields.append(
                self.datastorage_repository.create_dimension_data_storage_field_dict(
                    dimension_name=f"txt{text.lower()}",
                    is_key=False,
                )
            )
        data_storage_schema = self.datastorage_repository.get_data_storage_schema(
            text_table_id,
            ds_fields,
            DataStorageEnum.DIMENSION_TEXTS,
            model,
            is_create,
        )
        text_datastorage = await self.datastorage_repository.create_or_update_data_storage_by_session(
            tenant_id=tenant_id,
            model_name=model.name,
            name=text_table_id,
            data_storage_model=data_storage_schema,
            is_create=is_create,
        )
        if original_dimension:
            text_datastorage.version = original_dimension.version  # type: ignore
            text_datastorage.timestamp = original_dimension.timestamp
        return text_datastorage, data_storage_schema

    async def _update_attribute_data_storage_info_by_dimension(
        self,
        tenant_id: str,
        dimension: dict,
        attributes_table_id: Optional[str],
        is_create: bool,
        model: Model,
        original_dimension: Optional[Dimension] = None,
    ) -> tuple[Optional[DataStorage], Any]:
        """Создает или обновляет attribute_table Datastorage в зависимости от содержания "attributes"."""

        if len(dimension["attributes"]) == 0:
            if original_dimension:
                original_dimension.attributes_table = None
            return None, None
        if attributes_table_id is None:
            return None, None
        dimension["attributes_time_dependency"] = False
        ds_fields = [self.datastorage_repository.create_dimension_data_storage_field_dict(dimension["name"])]
        attributes_fields = []
        for attribute in dimension["attributes"]:
            if attribute.get("time_dependency"):
                dimension["attributes_time_dependency"] = True
            else:
                attribute["time_dependency"] = False
            attributes_fields.append(
                self.datastorage_repository.get_field_dict(
                    attribute["name"],
                    ref_type=attribute["ref_type"],
                    labels=attribute.get("labels"),
                    semantic_type=attribute["semantic_type"],
                )
            )
        if dimension["attributes_time_dependency"]:
            field_names = (DATEFROM, DATETO)
            for field_name in field_names:
                is_key = field_name == DATETO
                ds_fields.append(
                    self.datastorage_repository.create_dimension_data_storage_field_dict(field_name, is_key)
                )
        ds_fields.extend(attributes_fields)
        data_storage_schema = self.datastorage_repository.get_data_storage_schema(
            attributes_table_id,
            ds_fields,
            DataStorageEnum.DIMENSION_ATTRIBUTES,
            model,
            is_create,
        )
        attribute_datastorage = await self.datastorage_repository.create_or_update_data_storage_by_session(
            tenant_id=tenant_id,
            model_name=model.name,
            name=attributes_table_id,
            data_storage_model=data_storage_schema,
            is_create=is_create,
        )
        if original_dimension:
            attribute_datastorage.version = original_dimension.version  # type: ignore
            attribute_datastorage.timestamp = original_dimension.timestamp
        return attribute_datastorage, data_storage_schema

    async def _create_values_data_storage_info(
        self,
        tenant_id: str,
        model: Model,
        values_table_id: Optional[str],
        name: str,
    ) -> DataStorage:
        if values_table_id is None:
            values_table_id = settings.VALUES_DATASTORAGE_PATTERN % name
        ds_fields = [
            self.datastorage_repository.create_dimension_data_storage_field_dict(name),
            self.datastorage_repository.create_dimension_data_storage_field_dict(
                TechDimensionEnum.TIMESTAMP, is_key=False, is_tech_field=True
            ),
            self.datastorage_repository.create_dimension_data_storage_field_dict(
                TechDimensionEnum.DELETED, is_key=False, is_tech_field=True
            ),
        ]
        data_storage_schema = self.datastorage_repository.get_data_storage_schema(
            values_table_id, ds_fields, DataStorageEnum.DIMENSION_VALUES, model, True
        )
        return await self.datastorage_repository.create_or_update_data_storage_by_session(
            tenant_id=tenant_id,
            model_name=model.name,
            name=values_table_id,
            data_storage_model=data_storage_schema,
            is_create=True,
        )

    async def _update_texts_and_attributes_data_storages_info_by_dimension(
        self,
        tenant_id: str,
        attributes_table_id: Optional[str],
        text_table_id: Optional[str],
        dimension: dict,
        model: Model,
        original_dimension: Optional[Dimension] = None,
    ) -> tuple[Optional[DataStorage], Optional[DataStorage], Any, Any]:
        """Обновляет data_storage"""
        attributes_schema = None
        text_schema = None
        pop_texts = False
        pop_attributes = False
        if original_dimension:
            attributes_table_id = (
                original_dimension.attributes_table.name if original_dimension.attributes_table else None
            )
            text_table_id = original_dimension.text_table.name if original_dimension.text_table else None
            original_dimension_dict = DimensionModel.model_validate(original_dimension).model_dump(mode="json")
            if dimension.get("attributes") is None and original_dimension.attributes is not None:
                dimension["attributes"] = original_dimension_dict["attributes"]
                pop_attributes = True
            if dimension.get("texts") is None and original_dimension.texts is not None:
                dimension["texts"] = original_dimension_dict["linked_texts"]
                pop_texts = True
        if dimension.get("attributes") is not None and attributes_table_id is None:
            attributes_table_id = settings.ATTRIBUTE_DATASTORAGE_PATTERN % dimension["name"]
        if dimension.get("texts") is not None and text_table_id is None:
            text_table_id = settings.TEXT_DATASTORAGE_PATTERN % dimension["name"]
        create_attributes_data_storage = True
        create_text_data_storage = True
        ds_attributes = await self.datastorage_repository.get_datastorage_orm_by_session(
            tenant_id=tenant_id,
            model_name=None,
            name=attributes_table_id,
        )
        if ds_attributes:
            create_attributes_data_storage = False
        ds_text = await self.datastorage_repository.get_datastorage_orm_by_session(
            tenant_id=tenant_id,
            model_name=None,
            name=text_table_id,
        )
        if ds_text:
            create_text_data_storage = False
        if dimension.get("attributes") is not None and not pop_attributes:
            ds_attributes, attributes_schema = await self._update_attribute_data_storage_info_by_dimension(
                tenant_id=tenant_id,
                attributes_table_id=attributes_table_id,
                dimension=dimension,
                is_create=create_attributes_data_storage,
                model=model,
                original_dimension=original_dimension,
            )
        if dimension.get("texts") is not None and not pop_texts:
            ds_text, text_schema = await self._update_text_data_storage_info_by_dimension(
                tenant_id=tenant_id,
                text_table_id=text_table_id,
                dimension=dimension,
                is_create=create_text_data_storage,
                model=model,
                original_dimension=original_dimension,
            )
        if pop_texts:
            dimension.pop("texts")
        if pop_attributes:
            dimension.pop("attributes")
        return ds_attributes, ds_text, attributes_schema, text_schema

    async def _convert_attribute_object_list_to_orm(
        self,
        tenant_id: str,
        model_names: Optional[list[str]],
        attributes: list[dict[str, Any]],
    ) -> list[DimensionAttribute]:
        """Создать список объектов SQLAlchemy DimensionAttribute из списка словарей."""
        result_attributes = []
        ref_object: Optional[Dimension | Measure]
        for attribute in attributes:
            measure = None
            dimension = None
            ref_type = attribute.pop("ref_type")
            attribute["labels"] = convert_labels_list_to_orm(attribute.get("labels", []), DimensionAttributeLabel)
            attribute["attribute_type"] = ref_type["ref_object_type"]
            if attribute["attribute_type"] == BaseFieldTypeEnum.MEASURE:
                ref_object = await get_measure_orm_model_by_session(
                    session=self.session, tenant_id=tenant_id, name=ref_type["ref_object"], model_names=model_names
                )
                measure = ref_object
                attribute["measure_attribute_id"] = measure.id
            elif attribute["attribute_type"] == BaseFieldTypeEnum.DIMENSION:
                ref_object = await self.get_dimension_orm_model_by_session_with_error(
                    tenant_id=tenant_id, name=ref_type["ref_object"], model_names=model_names
                )

                dimension = ref_object
                attribute["dimension_attribute_id"] = dimension.id
            elif attribute["attribute_type"] == BaseFieldTypeEnum.ANYFIELD:
                add_missing_labels(ref_type["ref_object"]["labels"], ref_type["ref_object"]["name"], append_long=False)
                attribute["any_field_attribute"] = convert_anyfield_dict_to_orm(ref_type["ref_object"])
            dimension_attribute_model = DimensionAttribute(**attribute)
            dimension_attribute_model.dimension_attribute = dimension
            dimension_attribute_model.measure_attribute = measure
            result_attributes.append(dimension_attribute_model)
        return result_attributes

    def _convert_texts_object_list_to_orm(self, texts: list[str]) -> list[TextLink]:
        """Создать список объектов SQLAlchemy TextLink из списка словарей."""
        result_texts = []
        for text in texts:
            result_texts.append(TextLink(text_type=text))
        return result_texts

    @timeit
    async def get_by_name(self, tenant_id: str, name: str, model_name: Optional[str]) -> DimensionModel:
        """
        Получает объект измерения по имени и (при наличии) имени модели.

        Args:
            tenant_id (str): Идентификатор арендатора (тенанта).
            name (str): Имя измерения.
            model_name (Optional[str]): Имя модели, к которой привязано измерение (если применимо).

        Returns:
            DimensionModel: Объект измерения ORM.
        """
        result = await self.get_dimension_orm_model(tenant_id=tenant_id, name=name, model_name=model_name)
        if not result:
            raise NoResultFound(
                f"Dimension with tenant_id={tenant_id}, model_name={model_name} and name={name} not found."
            )
        related_dimensions = await self.get_related_dimensions_by_dimension_id(result.id)
        ref_tables_by_dimension = await self.get_ref_tables_dimensions_by_dimension_or_dimensions(result)
        return DimensionModel.model_validate(
            result,
            context=self._get_context_from_origin_dimension(ref_tables_by_dimension, list(related_dimensions)),
        )

    @staticmethod
    def _get_context_from_origin_dimension(
        ref_tables_by_dimension: dict[str, Any], related_dimensions: list[str]
    ) -> dict[str, Any]:
        """
        Формирует контекстные данные на основе таблиц и связанных измерений.

        Args:
            ref_tables_by_dimension (dict[str, Any]): Словарь таблиц, привязанных к измерениям.
            related_dimensions (list[str]): Список связанных измерений.

        Returns:
            dict[str, Any]: Контекстные данные, сформированные на основе входных параметров.
        """
        return {
            "related_dimensions": related_dimensions,
            "ref_values_table_name": ref_tables_by_dimension["ref_values_table_name"],
            "ref_attributes_table_name": ref_tables_by_dimension["ref_attributes_table_name"],
            "ref_text_table_name": ref_tables_by_dimension["ref_text_table_name"],
            "ref_texts": (
                [text.text_type for text in ref_tables_by_dimension["ref_texts"]]  # type: ignore[attr-defined]
                if ref_tables_by_dimension["ref_texts"]
                else []
            ),
            "ref_attributes": ref_tables_by_dimension["ref_attributes"],
            "ref_pv_dictionary": ref_tables_by_dimension["ref_pv_dictionary"],
            "ref_case_sensitive": ref_tables_by_dimension["ref_case_sensitive"],
            "ref_texts_language_dependency": ref_tables_by_dimension["ref_texts_language_dependency"],
            "ref_texts_time_dependency": ref_tables_by_dimension["ref_texts_time_dependency"],
            "ref_attributes_time_dependency": ref_tables_by_dimension["ref_attributes_time_dependency"],
            "ref_data_access_method": ref_tables_by_dimension["ref_data_access_method"],
            "ref_is_virtual": ref_tables_by_dimension["ref_is_virtual"],
            "ref_business_key_representation": ref_tables_by_dimension["ref_business_key_representation"],
            "ref_auth_relevant": ref_tables_by_dimension["ref_auth_relevant"],
            "ref_precision": ref_tables_by_dimension["ref_precision"],
        }

    @timeit
    async def get_orm_by_id(self, dimension_id: int) -> Optional[Dimension]:
        """
        Получить объект SQLAlchemy Dimension по его идентификатору.

        Выполняет SQL-запрос к базе данных для получения объекта `Dimension` по указанному `dimension_id`.
        Загружает связанные хранилища `values_table`, `text_table` и `attributes_table` через joinedload.

        Args:
            dimension_id (int): Идентификатор объекта Dimension, который необходимо получить.

        Returns:
            Optional[Dimension]: Объект модели SQLAlchemy Dimension, если он найден;
                                None, если объект не найден.
        """
        query = (
            select(Dimension)
            .where(
                Dimension.id == dimension_id,
            )
            .options(
                joinedload(Dimension.values_table),
                joinedload(Dimension.text_table),
                joinedload(Dimension.attributes_table),
            )
        )
        query = self._get_joined_hierarchies(query)
        result = (await self.session.execute(query)).unique().scalars().one_or_none()
        return result

    async def get_list_of_orm_models(
        self,
        tenant_id: str,
        model_name: Optional[str] = None,
        names: Optional[list[str]] = None,
        ids: Optional[list[int]] = None,
        pagination: Optional[Pagination] = None,
        with_ref_dim: bool = True,
        with_datastorages: bool = False,
    ) -> Sequence[Dimension]:
        """
        Получает список объектов измерений ORM по различным критериям.

        Args:
            tenant_id (str): Идентификатор арендатора (тенанта).
            model_name (Optional[str]): Имя модели, к которой привязаны измерения (если применимо).
            names (Optional[list[str]]): Список имен измерений для фильтрации (если применимо).
            pagination (Optional[Pagination]): Параметры пагинации (если применимы).
            with_ref_dim (bool): Флаг, указывающий, следует ли загружать связанные измерения.
            with_datastorages (bool): Флаг, указывающий, следует ли загружать связанные хранилища данных.

        Returns:
            Sequence[Dimension]: Последовательность объектов измерений ORM.
        """
        query = select(Dimension)
        if with_ref_dim:
            query = query.options(joinedload(Dimension.dimension))
        if with_datastorages:
            query = (
                query.options(
                    joinedload(Dimension.values_table).options(
                        selectinload(DataStorage.fields).options(
                            joinedload(DataStorageField.dimension),
                            joinedload(DataStorageField.measure),
                            joinedload(DataStorageField.any_field),
                        )
                    )
                )
                .options(
                    joinedload(Dimension.text_table).options(
                        selectinload(DataStorage.fields).options(
                            joinedload(DataStorageField.dimension),
                            joinedload(DataStorageField.measure),
                            joinedload(DataStorageField.any_field),
                        )
                    )
                )
                .options(
                    joinedload(Dimension.attributes_table).options(
                        selectinload(DataStorage.fields).options(
                            joinedload(DataStorageField.dimension),
                            joinedload(DataStorageField.measure),
                            joinedload(DataStorageField.any_field),
                        )
                    )
                )
            )
        if names is None and ids is None:
            query = query.where(
                Dimension.tenant_id == tenant_id,
            )
        elif ids is not None:
            query = query.where(
                Dimension.tenant_id == tenant_id,
                Dimension.id.in_(ids),
            )
        elif names is not None:
            query = query.where(
                Dimension.tenant_id == tenant_id,
                Dimension.name.in_(names),
            )
        query = self._get_joined_hierarchies(query)
        query = query.where(Dimension.models.any(Model.name == model_name)) if model_name else query
        query = get_select_query_with_offset_limit_order(query, Dimension.name, pagination)
        result = (await self.session.execute(query)).scalars().unique().all()
        return result

    @timeit
    async def get_list(
        self,
        tenant_id: str,
        model_name: str,
        names: Optional[list[str]] = None,
        pagination: Optional[Pagination] = None,
    ) -> list[DimensionModel]:
        """
        Получает список измерений по заданным критериям.

        Args:
            tenant_id (str): Идентификатор арендатора (тенанта).
            model_name (str): Имя модели, к которой привязаны измерения.
            names (Optional[list[str]]): Список имен измерений для фильтрации (если применимо).
            pagination (Optional[Pagination]): Параметры пагинации (если применимы).

        Returns:
            list[DimensionModel]: Список объектов измерений.
        """
        result = await self.get_list_of_orm_models(tenant_id, model_name, names, pagination=pagination)
        related_dimensions = await self.get_related_dimensions_by_dimensions(
            [dimension_element.id for dimension_element in result]
        )
        ref_tables = {}
        for dimension in result:
            ref_tables_by_dimension = await self.get_ref_tables_dimensions_by_dimension_or_dimensions(
                dimension, None if names else result
            )
            ref_tables[dimension.id] = ref_tables_by_dimension
        res = []
        for dimension in result:
            context = self._get_context_from_origin_dimension(
                ref_tables[dimension.id], related_dimensions.get(dimension.id, [])
            )
            res.append(
                DimensionModel.model_validate(
                    dimension,
                    context=context,
                )
            )
        return res

    @timeit
    async def get_list_by_tenant(self, tenant_id: str) -> list[DimensionModel]:
        """
        Получает список измерений, связанных с конкретным арендатором.

        Args:
            tenant_id (str): Идентификатор арендатора (тенанта).

        Returns:
            list[DimensionModel]: Список объектов измерений.
        """
        result = await self.get_list_of_orm_models(tenant_id)
        return [DimensionModel.model_validate(dimension) for dimension in result]

    async def create_not_virtual_dimension(
        self,
        dimension_name: str,
        tenant_id: str,
        model_names: list[str],
        texts: list[TextEnum],
        type: DimensionTypeEnum,
        precision: int,
        generate_on_db: bool = True,
        check_possible_delete: bool = True,
    ) -> None:
        if len(model_names) == 0:
            raise ValueError("Список моделей пуст")
        dimension = await self.get_dimension_orm_model(tenant_id, dimension_name, None)
        if dimension is None:
            operation_dim = DimensionCreateRequestModel(
                name=dimension_name,
                texts=texts,
                type=type,
                precision=precision,
            )
            dimension = await self.create_by_schema(
                tenant_id=tenant_id,
                model_name=model_names[0],
                dimension=operation_dim,
                if_not_exists=True,
                generate_on_db=generate_on_db,
                check_possible_delete=check_possible_delete,
            )
            exists_models = set(dimension.models_names)
        else:
            if isinstance(dimension, dict):
                exists_models = {model.name for model in dimension["models"]}
            else:
                if hasattr(dimension, "models_names"):
                    exists_models = set(dimension.models_names)
                elif hasattr(dimension, "models"):
                    exists_models = {model.name for model in dimension.models}
        for model in model_names:
            if model not in exists_models:
                await self.copy_model_dimension(
                    tenant_id=tenant_id,
                    name=dimension.name,
                    model_names=[model],
                    copy_attributes=True,
                    generate_on_db=generate_on_db,
                    if_not_exists=True,
                    check_possible_delete=check_possible_delete,
                )

    async def create_not_virtual_dimensions(
        self,
        tenant_id: str,
        model_names: list[str],
        generate_on_db: bool = True,
        check_possible_delete: bool = True,
    ) -> None:
        """
        Создает физические dimension для указанной модели.

        Args:
            models (list[Model]): Модель, для которой нужно создать измерения.
        """
        dimensions: list[NotVirtualDimensionDictCreate] = [
            {
                "name": DataStorageLogsFieldEnum.OPERATION,
                "texts": [TextEnum.LONG],
                "type": DimensionTypeEnum.STRING,
                "precision": 255,
            },
            {
                "name": HierarchyDimensionsEnum.LANGUAGE_TAG,
                "texts": [TextEnum.LONG],
                "type": DimensionTypeEnum.STRING,
                "precision": 5,
            },
            {
                "name": DefaultDimensionEnum.CALENDAR_DAY,
                "texts": [TextEnum.MEDIUM],
                "type": DimensionTypeEnum.DATE,
                "precision": 8,
            },
        ]
        for dimension in dimensions:
            await self.create_not_virtual_dimension(
                dimension["name"],
                tenant_id,
                model_names,
                dimension["texts"],
                dimension["type"],
                dimension["precision"],
                generate_on_db,
                check_possible_delete=check_possible_delete,
            )
        await self.session.commit()

    async def _create_additional_virtual_dimensions(self, model: Model) -> None:

        await self.datastorage_repository.create_fields_dimensions_if_not_exists(
            model,
            {
                DimensionTextFieldEnum.SHORT_TEXT: {
                    "precision": TEXT_TO_LENGTH[TextEnum.SHORT],
                    "type": AnyFieldTypeEnum.STRING,
                },
                DimensionTextFieldEnum.MEDIUM_TEXT: {
                    "precision": TEXT_TO_LENGTH[TextEnum.MEDIUM],
                    "type": AnyFieldTypeEnum.STRING,
                },
                DimensionTextFieldEnum.LONG_TEXT: {
                    "precision": TEXT_TO_LENGTH[TextEnum.LONG],
                    "type": AnyFieldTypeEnum.STRING,
                },
                TechDimensionEnum.IS_ACTIVE_DIMENSION: {
                    "precision": 1,
                    "type": AnyFieldTypeEnum.BOOLEAN,
                },
                TechDimensionEnum.DELETED: {
                    "precision": 1,
                    "type": AnyFieldTypeEnum.BOOLEAN,
                },
                TechDimensionEnum.TIMESTAMP: {
                    "precision": 14,
                    "type": AnyFieldTypeEnum.TIMESTAMP,
                },
                DefaultDimensionEnum.DATEFROM: {
                    "dimension_ref": DefaultDimensionEnum.CALENDAR_DAY,
                    "name_ru_short": "Дата начала",
                    "name_ru_long": "Дата начала",
                    "type": AnyFieldTypeEnum.DATE,
                },
                DefaultDimensionEnum.DATETO: {
                    "dimension_ref": DefaultDimensionEnum.CALENDAR_DAY,
                    "name_ru_short": "Дата окончания",
                    "name_ru_long": "Дата окончания",
                    "type": AnyFieldTypeEnum.DATE,
                },
            },
            is_virtual=True,
        )

    @timeit
    async def delete_by_name(
        self, tenant_id: str, model_name: str, name: str, if_exists: bool = True, check_possible_delete: bool = True
    ) -> None:
        """
        Создает дополнительные виртуальные измерения для указанной модели.

        Args:
            model (Model): Модель, для которой нужно создать виртуальные измерения.
        """
        result: Optional[Dimension] = await self.get_dimension_orm_model(
            tenant_id=tenant_id, name=name, model_name=model_name
        )
        if result is None:
            raise NoResultFound(
                f"Dimension with tenant_id={tenant_id}, model_name={model_name} and name={name} not found."
            )
        value_table = None
        attributes_table = None
        text_table = None
        exclude_dso = []
        model = await self.model_repository.get_model_orm_by_session_with_error(tenant_id, model_name)
        if result.values_table:
            value_table = await self.datastorage_repository.get_datastorage_orm_by_session(
                tenant_id, None, result.values_table.name
            )
            result.values_table = value_table
            if value_table:
                exclude_dso.append(value_table.id)
        if result.attributes_table:
            attributes_table = await self.datastorage_repository.get_datastorage_orm_by_session(
                tenant_id, None, result.attributes_table.name
            )
            result.attributes_table = attributes_table
            if attributes_table:
                exclude_dso.append(attributes_table.id)
        if result.text_table:
            text_table = await self.datastorage_repository.get_datastorage_orm_by_session(
                tenant_id, None, result.text_table.name
            )
            result.text_table = text_table
            if text_table:
                exclude_dso.append(text_table.id)

        possible_delete, block_objects = await self.model_relations_repository.check_if_dimension_can_be_deleted(
            result.id, model.id, exclude_dso
        )
        if not possible_delete:
            raise ValueError(f"It is not possible to delete {name} due to the following dependencies: {block_objects}")
        value_table_database_objects_model: Optional[list[DatabaseObjectModel]] = None
        attributes_table_database_objects_model: Optional[list[DatabaseObjectModel]] = None
        text_table_database_objects_model: Optional[list[DatabaseObjectModel]] = None
        if len(result.models) > 1:
            await self.dimension_history_repository.save_history(result, forced=True)
            result.models = list(filter(lambda model: model.name != model_name, result.models))
            if value_table:
                value_table_database_objects_model, _ = (
                    await self.datastorage_repository.delete_without_commit_by_session(value_table, model_name)
                )
            if attributes_table:
                attributes_table_database_objects_model, _ = (
                    await self.datastorage_repository.delete_without_commit_by_session(attributes_table, model_name)
                )
            if text_table:
                text_table_database_objects_model, _ = (
                    await self.datastorage_repository.delete_without_commit_by_session(text_table, model_name)
                )
            await self.session.flush()
            await self.dimension_history_repository.update_version(result)
        else:
            await self.dimension_history_repository.save_history(result, deleted=True)
            await self.session.delete(result)
        await self.session.flush()
        generator = get_generator(model)
        await generator.delete_dimension(
            result,
            model,
            value_table_database_objects_model,
            text_table_database_objects_model,
            attributes_table_database_objects_model,
            if_exists,
            check_possible_delete=check_possible_delete,
        )
        return None

    async def get_dimension_dim_dependencies(self, dimension_orm: Dimension) -> tuple[list[Dimension], list[Dimension]]:
        """Получение зависимостей измерений."""
        dimension_ids = await self.graph_repository.get_dependency_dimension_ids(dimension_orm.id)
        dimensions = await self.get_list_of_orm_models(
            dimension_orm.tenant_id, ids=dimension_ids, with_datastorages=True
        )
        not_cycles_dim_names, cycles_dim_names = (
            self.graph_repository.get_reversed_topological_order_dimensions_without_cycles(
                list(dimensions), pv_flag=True
            )
        )

        dimensions_dict: dict[str, Dimension] = {}
        for dimension in dimensions:
            dimensions_dict[dimension.name] = dimension

        not_cycles_dim = []
        cycles_dim = []
        logger.debug("Not created in pvd dimensions without cycles: %s", not_cycles_dim_names)
        logger.debug("Not created in pvd dimensions with cycles: %s", list(cycles_dim_names.keys()))
        for not_cycles_dim_name in not_cycles_dim_names:
            if not_cycles_dim_name in dimensions_dict:
                not_cycles_dim.append(dimensions_dict[not_cycles_dim_name])
            else:
                raise ValueError(f"Dimension {not_cycles_dim_name} not found in dimensions")
        for dimension in dimensions:
            if dimension.name in cycles_dim_names:
                cycles_dim.append(dimension)
        return not_cycles_dim, cycles_dim

    async def _create_in_pvd_by_orm_and_ref_mapping(
        self,
        dimension_orm: Dimension,
        pv_dictionary: dict,
        ref_mapping: dict,
        with_attrs: bool = True,
        with_texts: bool = True,
        commit: bool = False,
    ) -> None:
        logger.debug(
            "Start creating %s dimension_orm with_attrs=%s, with_texts=%s", dimension_orm.name, with_attrs, with_texts
        )
        client_pv = ClientPVDictionaries(ref_mapping)
        version = await client_pv.get_dictionary_or_none(pv_dictionary["domain_name"], pv_dictionary["object_name"])
        if version:
            logger.info("Dictionary already exists. Skip...")
        else:
            version = await client_pv.create_dictionary(
                dimension_orm, pv_dictionary, with_attrs=with_attrs, with_texts=with_texts
            )
            if settings.PV_DICTIONARIES_VERSIONED_DICTIONARY:
                saved_version = await client_pv.create_version_dictionary(version)
                await client_pv.activate_version_dictionary(saved_version, version.object_name)
        pv_dictionary["object_id"] = version.object_id
        dimension_orm.pv_dictionary = self.convert_pv_dictionaries_dict_to_orm_model(pv_dictionary)
        if commit:
            await self.session.commit()
        else:
            await self.session.flush()

    async def _update_in_pvd_by_orm_and_ref_mapping(
        self,
        dimension_orm: Dimension,
        pv_dictionary: dict,
        ref_mapping: dict,
        with_attrs: bool = True,
        with_texts: bool = True,
        commit: bool = False,
    ) -> None:
        logger.debug(
            "Start updating %s dimension_orm with_attrs=%s, with_texts=%s", dimension_orm.name, with_attrs, with_texts
        )
        client_pv = ClientPVDictionaries(ref_mapping)
        exist_version = await client_pv.get_dictionary_or_none(
            pv_dictionary["domain_name"], pv_dictionary["object_name"]
        )
        if not exist_version:
            version = await client_pv.create_dictionary(
                dimension_orm, pv_dictionary, with_attrs=with_attrs, with_texts=with_texts
            )
        else:
            version = await client_pv.update_dictionary(
                dimension_orm, pv_dictionary, with_attrs=with_attrs, with_texts=with_texts
            )
        if settings.PV_DICTIONARIES_VERSIONED_DICTIONARY:
            saved_version = await client_pv.create_version_dictionary(version)
            await client_pv.activate_version_dictionary(saved_version, version.object_name)
        pv_dictionary["object_id"] = version.object_id
        dimension_orm.pv_dictionary = self.convert_pv_dictionaries_dict_to_orm_model(pv_dictionary)
        if commit:
            await self.session.commit()
        else:
            await self.session.flush()

    async def create_cycles_pv_dictionary(
        self,
        cycles_dim: list[Dimension],
        root_pv_dictionary: dict | None = None,
        root_dimension_name: str | None = None,
        commit: bool = False,
    ) -> None:
        ref_mapping = {}
        pv_mapping = {}
        for dimension_orm in cycles_dim:
            logger.debug("Check creating dimension %s in pvd (cycles)", dimension_orm.name)
            if dimension_orm.is_virtual or dimension_orm.dimension_id or dimension_orm.pv_dictionary_id:
                logger.debug("Dimension %s already created or virtual. Skip... (cycles)", dimension_orm.name)
                continue
            if (
                root_dimension_name is not None
                and dimension_orm.name == root_dimension_name
                and root_pv_dictionary
                and not root_pv_dictionary.get("object_name")
            ):
                root_pv_dictionary["object_name"] = snake_to_camel(
                    settings.PV_DICTIONARIES_DEFAULT_NAME_PATTERN % dimension_orm.name
                    + settings.PV_DICTIONARIES_DEFAULT_NAME_SUFFIX
                )
                pv_dictionary = root_pv_dictionary
            elif root_dimension_name is not None and dimension_orm.name == root_dimension_name and root_pv_dictionary:
                pv_dictionary = root_pv_dictionary
            else:
                pv_dictionary = PVDictionary(
                    object_name=snake_to_camel(settings.PV_DICTIONARIES_DEFAULT_NAME_PATTERN % dimension_orm.name)
                    + settings.PV_DICTIONARIES_DEFAULT_NAME_SUFFIX,
                ).model_dump(mode="json")
            pv_mapping[dimension_orm.name] = pv_dictionary
            ref_mapping[dimension_orm.name] = await self._get_ref_dimension_field_mapping(dimension_orm)
            await self._create_in_pvd_by_orm_and_ref_mapping(
                dimension_orm, pv_dictionary, ref_mapping[dimension_orm.name], False, False, False
            )
        for dimension_orm in cycles_dim:
            if dimension_orm.is_virtual or dimension_orm.dimension_id:
                logger.debug("Dimension %s virtual. Skip... (cycles)", dimension_orm.name)
                continue
            await self._update_in_pvd_by_orm_and_ref_mapping(
                dimension_orm,
                pv_mapping[dimension_orm.name],
                ref_mapping[dimension_orm.name],
                with_attrs=True,
                with_texts=True,
                commit=False,
            )
        if commit:
            await self.session.commit()

    async def create_not_cycles_pv_dictionary_by_topology_order(
        self,
        not_cicles_dim: list[Dimension],
        root_pv_dictionary: dict | None = None,
        root_dimension_name: str | None = None,
        commit: bool = False,
    ) -> None:
        for dimension_orm in not_cicles_dim:
            logger.debug("Check creating dimension %s in pvd (not cycles)", dimension_orm.name)
            if dimension_orm.is_virtual or dimension_orm.dimension_id or dimension_orm.pv_dictionary_id:
                logger.debug("Dimension %s already created or virtual. Skip... (not cycles)", dimension_orm.name)
                continue
            if (
                root_dimension_name is not None
                and dimension_orm.name == root_dimension_name
                and root_pv_dictionary
                and not root_pv_dictionary.get("object_name")
            ):
                root_pv_dictionary["object_name"] = snake_to_camel(
                    settings.PV_DICTIONARIES_DEFAULT_NAME_PATTERN % dimension_orm.name
                    + settings.PV_DICTIONARIES_DEFAULT_NAME_SUFFIX
                )
                pv_dictionary = root_pv_dictionary
            elif root_dimension_name is not None and dimension_orm.name == root_dimension_name and root_pv_dictionary:
                pv_dictionary = root_pv_dictionary
            else:
                pv_dictionary = PVDictionary(
                    object_name=snake_to_camel(settings.PV_DICTIONARIES_DEFAULT_NAME_PATTERN % dimension_orm.name)
                    + settings.PV_DICTIONARIES_DEFAULT_NAME_SUFFIX,
                ).model_dump(mode="json")
            ref_mapping = await self._get_ref_dimension_field_mapping(dimension_orm)
            await self._create_in_pvd_by_orm_and_ref_mapping(
                dimension_orm, pv_dictionary, ref_mapping, True, True, commit
            )

    async def create_pv_dictionary(
        self,
        dimension_orm: Dimension,
        pv_dictionary: Optional[dict],
        commit: bool = False,
    ) -> None:
        """Создание справочника в PVDictionaries."""
        if dimension_orm.is_virtual or dimension_orm.dimension_id:
            logger.debug("Dimension %s is virtual. Skip...", dimension_orm.name)
            return None
        not_cycles, cycles = await self.get_dimension_dim_dependencies(dimension_orm)
        await self.create_not_cycles_pv_dictionary_by_topology_order(
            not_cycles, pv_dictionary, dimension_orm.name, commit
        )
        await self.create_cycles_pv_dictionary(cycles, pv_dictionary, dimension_orm.name, commit)
        await self.session.flush()

    async def _get_last_dimension_info_by_id(self, dimension_id: int) -> dict[str, str | bool] | None:
        """
        Возвращает последнее измерение в иерархии измерений по идентификатору измерения.
        """

        # Anchor (начальная точка)
        anchor = select(
            Dimension.id.label("id"),
            Dimension.name.label("name"),
            Dimension.is_virtual.label("is_virtual"),
            Dimension.dimension_id.label("next_id"),
        ).where(Dimension.id == dimension_id)

        # Рекурсивная часть
        recursive_cte = anchor.cte("dimension_path", recursive=True)
        recursive_part = (
            select(
                Dimension.id.label("id"),
                Dimension.name.label("name"),
                Dimension.is_virtual.label("is_virtual"),
                Dimension.dimension_id.label("next_id"),
            )
            .select_from(Dimension)
            .join(recursive_cte, Dimension.id == recursive_cte.c.next_id)
        )

        # Полный CTE
        full_cte = recursive_cte.union_all(recursive_part)

        # Берем последнее по цепочке (где нет next_id или максимальный путь)
        query = (
            select(full_cte.c.name, full_cte.c.is_virtual)
            .select_from(full_cte)
            .where(full_cte.c.next_id.is_(None))
            .limit(1)
        )

        result = await self.session.execute(query)
        row = result.fetchone()

        return {"name": row.name, "is_virtual": row.is_virtual} if row else None

    async def _get_ref_dimension_field_mapping(self, dimension: Dimension) -> dict[str, Dimension]:
        """Возвращает словарь маппинга полей измерения на последние измерения в иерархии."""
        fields: list[DataStorageField] = []
        result: dict[str, Dimension] = {}
        if dimension.values_table:
            fields.extend(dimension.values_table.fields)
        if dimension.text_table:
            fields.extend(dimension.text_table.fields)
        if dimension.attributes_table:
            fields.extend(dimension.attributes_table.fields)
        for field in fields:
            if not field.dimension or field.dimension_id == dimension.id or not field.dimension.dimension_id:
                continue
            last_dimension_info = await self._get_last_dimension_info_by_id(field.dimension.id)
            if (
                last_dimension_info
                and last_dimension_info["name"] != field.dimension.name
                and not last_dimension_info["is_virtual"]
            ):
                if isinstance(last_dimension_info["name"], bool):
                    raise ValueError("Bad type last_dimension_info['name']")
                result[field.dimension.name] = await self.get_dimension_orm_model(
                    dimension.tenant_id, last_dimension_info["name"], None
                )
        return result

    async def update_pv_dictionary(
        self,
        dimension_orm: Dimension,
        pv_dictionary: Optional[dict],
        commit: bool = False,
    ) -> None:
        """Обновить справочник в PVDictionaries."""
        update = True
        if not dimension_orm.pv_dictionary:
            logger.debug("Dimension %s has no pv_dictionary. Create", dimension_orm.name)
            update = False
        if not pv_dictionary and not dimension_orm.pv_dictionary:
            pv_dictionary = PVDictionary(
                object_name=snake_to_camel(settings.PV_DICTIONARIES_DEFAULT_NAME_PATTERN % dimension_orm.name)
                + settings.PV_DICTIONARIES_DEFAULT_NAME_SUFFIX,
            ).model_dump(mode="json")
        elif not pv_dictionary and dimension_orm.pv_dictionary:
            pv_dictionary = PVDictionary.model_validate(dimension_orm.pv_dictionary).model_dump(mode="json")
        elif pv_dictionary and not pv_dictionary.get("object_name"):
            pv_dictionary["object_name"] = snake_to_camel(
                settings.PV_DICTIONARIES_DEFAULT_NAME_PATTERN % dimension_orm.name
                + settings.PV_DICTIONARIES_DEFAULT_NAME_SUFFIX
            )
        await self.create_pv_dictionary(
            dimension_orm,
            pv_dictionary,
            commit,
        )
        if update and pv_dictionary and not dimension_orm.is_virtual and not dimension_orm.dimension_id:
            ref_mapping = await self._get_ref_dimension_field_mapping(dimension_orm)
            await self._update_in_pvd_by_orm_and_ref_mapping(dimension_orm, pv_dictionary, ref_mapping, commit=commit)

    @timeit
    async def create_pv_dictionary_after_dimension(
        self,
        dimension_orm: Dimension,
        pv_dictionary: Optional[dict],
        generator: GeneratorClickhouseRepository | GeneratorPostgreSQLRepository,
        model: Model,
        check_possible_delete: bool = True,
    ) -> None:
        """Создание справочника в PVDictionaries после создания его в dimension."""
        if not settings.ENABLE_PV_DICTIONARIES:
            return None
        try:
            await self.create_pv_dictionary(dimension_orm, pv_dictionary)
        except Exception as exc:
            await generator.delete_dimension(
                dimension_orm, model, if_exists=True, check_possible_delete=check_possible_delete
            )
            raise CreatePVDException(exc)
        return None

    @timeit
    async def update_pv_dictionary_after_dimension(
        self,
        dimension_orm: Dimension,
        pv_dictionary: Optional[dict],
    ) -> None:
        """Обновление справочника в PVDictionaries после создания его в dimension."""
        if not settings.ENABLE_PV_DICTIONARIES:
            return None
        try:
            await self.update_pv_dictionary(dimension_orm, pv_dictionary)
        except Exception as exc:
            raise CreatePVDException(exc)
        return None

    def _convert_prompt_to_orm(self, prompt: Optional[dict]) -> Optional[AIPrompt]:
        """Конвертирует AiPrompt в orm объект."""
        if not prompt:
            return None

        return AIPrompt(
            analytic_role=json.dumps(prompt["analytic_role"], ensure_ascii=False),
            purpose=json.dumps(prompt["purpose"], ensure_ascii=False),
            key_features=json.dumps(prompt["key_features"], ensure_ascii=False),
            data_type=json.dumps(prompt["data_type"], ensure_ascii=False),
            subject_area=json.dumps(prompt["subject_area"], ensure_ascii=False),
            example_questions=json.dumps(prompt["example_questions"], ensure_ascii=False),
            synonyms=prompt["analytic_descriptions"].get("synonyms"),
            markers=json.dumps(prompt["markers"], ensure_ascii=False),
            notes=json.dumps(prompt["notes"], ensure_ascii=False),
            ai_usage=prompt.get("ai_usage"),
            domain_id=prompt.get("domain_id"),
            group_id=prompt["group_id"] if prompt.get("group_id") else None,
            vector_search=prompt.get("vector_search"),
            fallback_to_llm_values=prompt.get("fallback_to_llm_values"),
            preferable_columns=prompt.get("preferable_columns"),
            entity_name=prompt["analytic_descriptions"].get("entity_name"),
            description=prompt["analytic_descriptions"].get("description"),
            few_shots=prompt["analytic_descriptions"].get("few_shots"),
        )

    def _enrich_dimension_attrs_with_default_attributes(
        self,
        dimension_attrs: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Дополняет атрибуты измерений значениями по умолчанию.

        Метод проходит по списку атрибутов измерений и добавляет отсутствующие поля,
        используя значения по умолчанию, если они определены.

        Args:
            dimension_attrs (list[dict[str, Any]]): Список словарей с атрибутами измерений.

        Returns:
            list[dict[str, Any]]: Обновлённый список атрибутов с добавленными значениями по умолчанию.
        """
        are_there_time_dependent_attributes = False
        is_active_attr_index = None

        for index, attr in enumerate(dimension_attrs):
            if attr["time_dependency"]:
                are_there_time_dependent_attributes = True
            if attr["name"] == TechDimensionEnum.IS_ACTIVE_DIMENSION:
                is_active_attr_index = index

        if is_active_attr_index and not are_there_time_dependent_attributes:
            return dimension_attrs

        if is_active_attr_index is None:
            dimension_attrs.append(
                self._get_default_is_active_attr(are_there_time_dependent_attributes).model_dump(mode="json")
            )
            is_active_attr_index = len(dimension_attrs) - 1
        dimension_attrs[is_active_attr_index]["time_dependency"] = are_there_time_dependent_attributes
        return dimension_attrs

    @staticmethod
    def _get_default_is_active_attr(
        time_dependency: bool,
    ) -> DimensionAttributeRequest:
        """
        Возвращает атрибут "is_active" по умолчанию в зависимости от наличия временной зависимости.

        Если измерение времязависимое, значение "is_active" устанавливается в True по умолчанию.
        В противном случае может возвращаться иное значение или не устанавливаться.

        Args:
            time_dependency (bool): Флаг, указывающий, является ли измерение времязависимым.

        Returns:
            DimensionAttributeRequest: Объект атрибута "is_active" с значением по умолчанию.
        """
        return DimensionAttributeRequest(
            name=TechDimensionEnum.IS_ACTIVE_DIMENSION,
            ref_type=BaseFieldType(
                ref_object_type=BaseFieldTypeEnum.DIMENSION,
                ref_object="is_active",
            ),
            time_dependency=time_dependency,
            semantic_type=SemanticType.DIMENSION,
        )

    @timeit
    async def create_by_schema(
        self,
        tenant_id: str,
        model_name: str,
        dimension: DimensionCreateRequestModel,
        if_not_exists: bool = False,
        generate_on_db: bool = True,
        check_possible_delete: bool = True,
    ) -> DimensionModel:
        """
        Создаёт новое измерение по переданной схеме.

        Метод создаёт измерение в указанной модели и тенанте. При установке флага `if_not_exists`
        операция будет выполнена только в случае, если измерение с таким именем ещё не существует.
        Опционально можно отключить генерацию объекта в БД, если требуется только валидация.

        Args:
            tenant_id (str): Идентификатор тенанта.
            model_name (str): Название модели, в которой создаётся измерение.
            dimension (DimensionCreateRequestModel): Данные для создания измерения.
            if_not_exists (bool): Если True, измерение будет создано только при отсутствии существующего с таким именем.
            generate_on_db (bool): Если True, изменения применяются к базе данных. Если False — только валидация.

        Returns:
            DimensionModel: Созданный объект измерения.
        """
        dimension_dict = dimension.model_dump(mode="json")
        add_missing_labels(dimension_dict["labels"], dimension.name)
        dimension_dict["labels"] = convert_labels_list_to_orm(dimension_dict["labels"], DimensionLabel)
        dimension_dict["tenant_id"] = tenant_id
        dimension_id = dimension_dict.pop("dimension_id")
        model = await self.model_repository.get_model_orm_by_session_with_error(tenant_id, model_name)
        dimension_dict["attributes"] = self._enrich_dimension_attrs_with_default_attributes(
            dimension_attrs=dimension_dict["attributes"],
        )
        data_storage_data: dict = copy.deepcopy(dimension_dict)
        await self._create_additional_virtual_dimensions(model)
        dimension_dict["attributes"] = await self._convert_attribute_object_list_to_orm(
            tenant_id=tenant_id,
            attributes=dimension_dict["attributes"],
            model_names=[model_name],
        )
        dimension_dict["prompt"] = self._convert_prompt_to_orm(dimension_dict.pop("prompt", None))
        dimension_dict["texts"] = self._convert_texts_object_list_to_orm(dimension_dict["texts"])
        values_table_id = dimension_dict.pop("values_table_id", None)
        attributes_table_id = dimension_dict.pop("attributes_table_id", None)
        text_table_id = dimension_dict.pop("text_table_id", None)
        pv_dictionary = dimension_dict.pop("pv_dictionary", None)
        ref_dimension = None
        if dimension_id:
            dimension_dict["is_virtual"] = True
            ref_dimension = await self.get_dimension_orm_model_by_session_with_error(
                tenant_id=tenant_id, name=dimension_id, model_names=None
            )
            dimension_dict["type"] = (
                ref_dimension.type if dimension_dict.get("type") is None else dimension_dict["type"]
            )
            dimension_dict["precision"] = (
                ref_dimension.precision if dimension_dict.get("precision") is None else dimension_dict["precision"]
            )

        dimension_orm = Dimension(**dimension_dict)
        dimension_orm.models = [model]
        dimension_orm.dimension = ref_dimension
        self.session.add(dimension_orm)
        await self.session.flush()
        if not dimension_id and not dimension_dict["is_virtual"]:
            attributes_ds, texts_ds, _, _ = await self._update_texts_and_attributes_data_storages_info_by_dimension(
                tenant_id,
                attributes_table_id,
                text_table_id,
                data_storage_data,
                model,
            )
            values_ds = await self._create_values_data_storage_info(
                tenant_id,
                model,
                values_table_id,
                dimension_dict["name"],
            )
            dimension_orm.attributes_time_dependency = data_storage_data["attributes_time_dependency"]
            dimension_orm.values_table = values_ds
            dimension_orm.attributes_table = attributes_ds
            dimension_orm.text_table = texts_ds
            dimension_orm.dimension = None
        self._update_dimension_information_category(dimension_orm, dimension_dict.get("information_category"))
        await self.session.flush()
        await self.set_owner_model([dimension_orm], model)
        await self.dimension_history_repository.update_version(dimension_orm, create=True)
        await self.session.flush()
        generator = get_generator(model)
        if generate_on_db:
            await generator.create_dimension(dimension_orm, model, if_not_exists)
        await self.create_pv_dictionary_after_dimension(
            dimension_orm, pv_dictionary, generator, model, check_possible_delete=check_possible_delete
        )
        await self.session.commit()
        returned_dimension = await self.get_dimension_orm_model(
            tenant_id=tenant_id, model_name=model_name, name=dimension_orm.name
        )
        ref_tables_by_dimension = await self.get_ref_tables_dimensions_by_dimension_or_dimensions(returned_dimension)

        return DimensionModel.model_validate(
            returned_dimension, context=self._get_context_from_origin_dimension(ref_tables_by_dimension, [])
        )

    async def _get_dimension_data_storages_with_field_types(
        self,
        original_dimension: Dimension,
    ) -> Any:
        """Возвращает атрибуты, необходимые для генерации таблиц."""
        prev_attributes_model = None
        prev_texts_model = None
        prev_values_model = None
        if original_dimension.attributes_table:
            attributes_table = await self.datastorage_repository.get_datastorage_orm_by_session(
                original_dimension.tenant_id,
                None,
                original_dimension.attributes_table.name,
            )
            prev_attributes_model = DataStorageModel.model_validate(attributes_table)
        if original_dimension.text_table:
            text_table = await self.datastorage_repository.get_datastorage_orm_by_session(
                original_dimension.tenant_id,
                None,
                original_dimension.text_table.name,
            )
            prev_texts_model = DataStorageModel.model_validate(text_table)
        if original_dimension.values_table:
            values_table = await self.datastorage_repository.get_datastorage_orm_by_session(
                original_dimension.tenant_id,
                None,
                original_dimension.values_table.name,
            )
            prev_values_model = DataStorageModel.model_validate(values_table)
        return (
            prev_attributes_model,
            prev_texts_model,
            prev_values_model,
        )

    async def clear_dimension_status(self, dimension: Dimension) -> None:
        datastorage_to_clear = []
        if dimension.values_table and dimension.values_table.id:
            datastorage_to_clear.append(dimension.values_table.id)
        if dimension.attributes_table and dimension.attributes_table.id:
            datastorage_to_clear.append(dimension.attributes_table.id)
        if dimension.text_table and dimension.text_table.id:
            datastorage_to_clear.append(dimension.text_table.id)
        await self.datastorage_repository.clear_datastorage_status(datastorage_to_clear)
        await self.session.execute(
            update(DimensionModelRelationOrm)
            .where(
                DimensionModelRelationOrm.dimension_id == dimension.id,
            )
            .values({"status": ModelStatusEnum.PENDING})
        )

    def _update_dimension_information_category(
        self, dimension: Dimension, information_category: Optional[InformationCategoryEnum]
    ) -> None:
        """
        Обновляет категорию информации для объекта Dimension и связанных с ним таблиц.

        Эта функция устанавливает переданную категорию информации (information_category)
        для самого объекта Dimension, а также для всех существующих связанных таблиц:
        values_table, attributes_table и texts_table.

        Args:
            dimension (Dimension): Объект, для которого обновляется категория информации.
            information_category (Optional[InformationCategoryEnum]): Новая категория информации.
                Если значение равно None, функция завершается без изменений.

        Returns:
            None: Функция не возвращает значение.
        """
        if information_category is None:
            return None
        dimension.information_category = information_category
        if dimension.values_table:
            dimension.values_table.information_category = information_category
        if dimension.attributes_table:
            dimension.attributes_table.information_category = information_category
        if dimension.text_table:
            dimension.text_table.information_category = information_category

    @timeit
    async def update_by_name_and_schema(
        self,
        tenant_id: str,
        model_name: str,
        name: str,
        dimension: DimensionEditRequestModel,
        generate_on_db: bool = True,
        enable_delete_not_empty: bool = False,
    ) -> DimensionModel:
        """
        Обновляет объект Dimension по имени и схеме.

        Этот метод обновляет существующий объект Dimension в базе данных на основе переданных параметров.
        Он управляет различными аспектами измерения, включая виртуальный статус, связанные хранилища данных,
        атрибуты, тексты, метки и подсказки.

        Args:
            tenant_id (str): Уникальный идентификатор тенанта.
            model_name (str): Имя модели, к которой привязано измерение.
            name (str): Имя измерения для обновления.
            dimension (DimensionEditRequestModel): Новая конфигурация измерения.

        Returns:
            DimensionModel: Обновленный объект модели измерения.

        Raises:
            NoResultFound: Если указанное измерение не найдено в базе данных.
        """
        dimension_dict = dimension.model_dump(mode="json", exclude_unset=True)
        dimension_dict["name"] = name
        dimension_dict["tenant_id"] = tenant_id
        attributes_edit_model = None
        texts_edit_model = None
        original_dimension: Optional[Dimension] = await self.get_dimension_orm_model(
            tenant_id=tenant_id, model_name=model_name, name=name
        )
        if original_dimension is None:
            raise NoResultFound(
                f"Dimension with tenant_id={tenant_id}, model_name={model_name} and name={name} not found."
            )
        value_table = (
            await self.datastorage_repository.get_datastorage_orm_by_session(
                tenant_id, None, original_dimension.values_table.name
            )
            if original_dimension.values_table
            else None
        )
        attributes_table = (
            await self.datastorage_repository.get_datastorage_orm_by_session(
                tenant_id, None, original_dimension.attributes_table.name
            )
            if original_dimension.attributes_table
            else None
        )
        text_table = (
            await self.datastorage_repository.get_datastorage_orm_by_session(
                tenant_id, None, original_dimension.text_table.name
            )
            if original_dimension.text_table
            else None
        )
        original_dimension.text_table = text_table
        original_dimension.values_table = value_table
        original_dimension.attributes_table = attributes_table
        await self.dimension_history_repository.save_history(original_dimension, edit_model=dimension_dict)
        model_names = [model.name for model in original_dimension.models]
        models_without_endpoint_model = list(filter(lambda obj: obj.name != model_name, original_dimension.models))
        model = await self.model_repository.get_model_orm_by_session_with_error(tenant_id, model_name)
        await self._create_additional_virtual_dimensions(model)
        (
            prev_attributes_model,
            prev_texts_model,
            prev_values_model,
        ) = await self._get_dimension_data_storages_with_field_types(original_dimension)
        if (
            "is_virtual" in dimension_dict
            and not dimension_dict["is_virtual"]
            and original_dimension.values_table is None
            and original_dimension.dimension is None
        ):
            values_ds = await self._create_values_data_storage_info(
                tenant_id,
                model,
                None,
                name,
            )
            original_dimension.is_virtual = False
            original_dimension.values_table = values_ds
        if (
            "dimension_id" in dimension_dict
            and dimension_dict["dimension_id"] is None
            and original_dimension.values_table_id is None
        ):

            original_dimension.dimension = None
            original_dimension.is_virtual = False
            dimension_dict.pop("dimension_id", None)
            if original_dimension.values_table is None:
                values_ds = await self._create_values_data_storage_info(
                    tenant_id,
                    model,
                    None,
                    name,
                )
                original_dimension.values_table = values_ds

        elif dimension_dict.get("dimension_id") is not None:
            ref_dimension = await self.get_dimension_orm_model_by_session_with_error(
                tenant_id=tenant_id, name=dimension_dict["dimension_id"], model_names=None
            )
            original_dimension.dimension = ref_dimension
            original_dimension.precision = ref_dimension.precision
            original_dimension.type = ref_dimension.type
            dimension_dict.pop("dimension_id", None)
            original_dimension.is_virtual = True
            original_dimension.attributes_table = None
            original_dimension.text_table = None
            original_dimension.values_table = None

        if dimension_dict.get("is_virtual"):
            original_dimension.is_virtual = True
            original_dimension.attributes_table = None
            original_dimension.text_table = None
            original_dimension.values_table = None
        if dimension_dict.get("attributes") is not None and name != "is_active":
            dimension_dict["attributes"] = self._enrich_dimension_attrs_with_default_attributes(
                dimension_dict.pop("attributes"),
            )
        if original_dimension.dimension is None and not original_dimension.is_virtual:
            attributes_ds, texts_ds, attributes_edit_model, texts_edit_model = (
                await self._update_texts_and_attributes_data_storages_info_by_dimension(
                    tenant_id,
                    None,
                    None,
                    dimension_dict,
                    model,
                    original_dimension,
                )
            )
            if dimension_dict.get("attributes_time_dependency") is not None:
                original_dimension.attributes_time_dependency = dimension_dict["attributes_time_dependency"]

        if not original_dimension.is_virtual and not original_dimension.dimension and attributes_ds:
            if original_dimension.attributes_table is None:
                _ = await self.datastorage_repository.copy_model_data_storage_orm_by_session(
                    tenant_id,
                    attributes_ds.name,
                    models_without_endpoint_model,
                    attributes_ds,
                    save_history=False,
                )
            original_dimension.attributes_table = attributes_ds
        if not original_dimension.is_virtual and not original_dimension.dimension and texts_ds:
            if original_dimension.text_table is None:
                _ = await self.datastorage_repository.copy_model_data_storage_orm_by_session(
                    tenant_id,
                    texts_ds.name,
                    models_without_endpoint_model,
                    texts_ds,
                    save_history=False,
                )
            original_dimension.text_table = texts_ds
        if dimension_dict.get("texts") is not None:
            original_dimension.texts = self._convert_texts_object_list_to_orm(dimension_dict.pop("texts"))
        if dimension_dict.get("attributes") is not None:
            original_dimension.attributes = await self._convert_attribute_object_list_to_orm(
                tenant_id=tenant_id,
                attributes=dimension_dict.pop("attributes"),
                model_names=model_names,
            )
        if dimension_dict.get("labels") is not None:
            add_missing_labels(dimension_dict["labels"], name)
            original_dimension.labels = convert_labels_list_to_orm(
                dimension_dict.pop("labels"),
                DimensionLabel,
            )
        if dimension_dict.get("prompt") is not None:
            original_dimension.prompt = self._convert_prompt_to_orm(dimension_dict.pop("prompt", None))
        dimension_dict.pop("prompt", None)
        dimension_dict.pop("name", None)
        self._update_dimension_information_category(original_dimension, dimension_dict.get("information_category"))
        await self.session.flush()
        if dimension_dict:
            await self.session.execute(
                update(Dimension)
                .where(
                    Dimension.name == name,
                    Dimension.models.any(Model.name == model_name),
                    Dimension.tenant_id == tenant_id,
                )
                .values(dimension_dict)
            )
        await self.session.flush()
        result_dimension: Optional[Dimension] = await self.get_dimension_orm_model(
            tenant_id=tenant_id, model_name=model_name, name=name
        )
        if result_dimension:
            await self.clear_dimension_status(result_dimension)
        await self.dimension_history_repository.update_version(original_dimension)
        await self.session.flush()
        related_dimensions: Sequence[str] = []
        updated_db = set()
        if result_dimension is not None:
            for model in result_dimension.models:
                if is_ignore_dimension(
                    model_name=model.name,
                    name=name,
                    is_virtual=result_dimension.is_virtual,
                ):
                    logger.warning(
                        "Dimension %s for model %s is ignored",
                        name,
                        model.name,
                    )
                    continue
                model_model = ModelModel.model_validate(model)
                if model_model.database is None:
                    raise ValueError("Database is None")
                if (model_model.schema_name, model_model.database.name) in updated_db:
                    logger.debug(
                        "Datastorage %s for database %s and schema %s has already been updated",
                        result_dimension.name,
                        model_model.database.name,
                        model_model.schema_name,
                    )
                    continue
                updated_db.add((model_model.schema_name, model_model.database.name))
                if generate_on_db:
                    generator = get_generator(model)
                    await generator.update_dimension(
                        result_dimension,
                        model,
                        prev_attributes_model.model_copy(deep=True) if prev_attributes_model is not None else None,
                        attributes_edit_model.model_copy(deep=True) if attributes_edit_model is not None else None,
                        prev_texts_model.model_copy(deep=True) if prev_texts_model is not None else None,
                        texts_edit_model.model_copy(deep=True) if texts_edit_model is not None else None,
                        prev_values_model.model_copy(deep=True) if prev_values_model is not None else None,
                        enable_delete_not_empty=enable_delete_not_empty,
                    )
            related_dimensions = await self.get_related_dimensions_by_dimension_id(result_dimension.id)
            ref_tables_by_dimension = await self.get_ref_tables_dimensions_by_dimension_or_dimensions(
                result_dimension,
            )
        if result_dimension:
            await self.update_pv_dictionary_after_dimension(result_dimension, None)
        await self.session.commit()
        result_dimension = await self.get_dimension_orm_model(tenant_id=tenant_id, model_name=model_name, name=name)
        context = self._get_context_from_origin_dimension(ref_tables_by_dimension, []) if result_dimension else {}
        context.update({"related_dimensions": related_dimensions})
        return DimensionModel.model_validate(result_dimension, context=context)

    async def set_owner_model(self, dimensions: list[Dimension], model: Model) -> None:
        """Обновляет состояние владельца модели."""
        datastorages = []
        dimensions_ids = []
        for dimension in dimensions:
            datastorages.extend(
                [
                    datastorage
                    for datastorage in (dimension.values_table, dimension.attributes_table, dimension.text_table)
                    if datastorage is not None
                ]
            )
            dimensions_ids.append(dimension.id)
        await self.session.execute(
            update(DimensionModelRelationOrm)
            .where(
                DimensionModelRelationOrm.dimension_id.in_(dimensions_ids),
            )
            .values({"is_owner": False})
        )
        await self.session.execute(
            update(DimensionModelRelationOrm)
            .where(
                DimensionModelRelationOrm.dimension_id.in_(dimensions_ids),
                DimensionModelRelationOrm.model_id == model.id,
            )
            .values({"is_owner": True})
        )
        await self.datastorage_repository.set_owner_model(datastorages, model)

    @staticmethod
    def _get_models_in_which_dimension_presents_and_absents(
        dimension_model: DimensionModel, model_names: list[str]
    ) -> tuple[set[str], set[str]]:
        """
        Определяет, какие из указанных моделей содержат данное измерение, а какие — нет.

        Args:
            dimension_model (DimensionModel): Измерение, присутствие которого проверяется.
            model_names (list[str]): Список названий моделей для анализа.

        Returns:
            tuple[set[str], set[str]]: Кортеж двух множеств строк:
                - Первое множество содержит названия моделей, содержащих указанное измерение.
                - Второе множество содержит названия моделей, НЕ содержащих указанное измерение.
        """
        orig_model_names = {model_status.name for model_status in dimension_model.models_statuses}
        dimension_absent_model_names = set(model_names) - orig_model_names
        dimension_present_model_names = orig_model_names & set(model_names)

        return dimension_present_model_names, dimension_absent_model_names

    async def _validate_dimension_attributes(
        self, dimension_model_list: list[DimensionModel], model_names: list[str], tenant_id: str
    ) -> None:
        """
        Валидирует атрибуты измерения перед выполнением операций копирования.

        Args:
            dimension_model_list (list[DimensionModel]): Модель измерения, которую планируется скопировать.
            model_names (list[str]): Список названий моделей, куда предполагается выполнить копирование.
            tenant_id (str): Идентификатор арендатора, которому принадлежат модели и измерения.

        Returns:
            None
        """
        attributes = []
        for dimension_model in dimension_model_list:
            dimension_dict = dimension_model.model_dump(mode="json")
            attributes.extend(dimension_dict["attributes"])

        await self._validate_attributes(attributes, model_names, tenant_id)

    async def _validate_attributes(
        self, attributes: list[dict[str, Any]], model_names: list[str], tenant_id: str
    ) -> None:
        """
        Валидирует атрибуты перед созданием или обновлением объекта.

        Args:
            attributes (list[dict[str, Any]]): Список атрибутов, представленных в виде словарей.
            model_names (list[str]): Список названий моделей, в рамках которых происходит проверка.
            tenant_id (str): Идентификатор арендатора, которому принадлежат объекты.

        Returns:
            None
        """
        measures = await get_list_of_measures_orm_by_session(self.session, tenant_id)
        dimensions = await get_list_dimension_orm_by_session(self.session, tenant_id)
        measures_by_names = {measure.name: measure for measure in measures}
        dimensions_by_names = {dimension.name: dimension for dimension in dimensions}
        model_names_set = set(model_names)

        error_list = []

        for attribute in attributes:
            if (
                attribute["ref_type"]["ref_object_type"] == BaseFieldTypeEnum.MEASURE
                and attribute["ref_type"]["ref_object"]
                and model_names_set - {model.name for model in measures_by_names[attribute["ref_type"]["ref_object"]].models}  # type: ignore[union-attr]
            ):
                error_list.append(f"Measure attribute {attribute['ref_type']['ref_object']} not found")
            if (
                attribute["ref_type"]["ref_object_type"] == BaseFieldTypeEnum.DIMENSION
                and attribute["ref_type"]["ref_object"]
                and model_names_set - {model.name for model in dimensions_by_names[attribute["ref_type"]["ref_object"]].models}  # type: ignore[union-attr]
            ):
                error_list.append(f"Dimension attribute {attribute['ref_type']['ref_object']} not found")

        if error_list:
            raise ValueError(f"Invalid attributes: {', '.join(error_list)}")

    async def copy_model_dimension_orm_by_session(
        self, tenant_id: str, name: str, model_names: list[str], validate: bool
    ) -> tuple[Dimension, list[Model]]:
        """
        Копирует измерение (`Dimension`) в указанные модели через ORM-сессию базы данных.

        Args:
            tenant_id (str): Идентификатор арендатора.
            name (str): Имя исходного измерения (`dimension`), которое нужно скопировать.
            model_names (list[str]): Список названий моделей, куда будет выполнено копирование.
            validate (bool): Флаг проверки наличия моделей до начала операции.

        Returns:
            tuple[Dimension, list[Model]]: Кортеж, содержащий копию измерения и список целевых моделей.
        """
        dimension: Dimension = await self.get_dimension_orm_model_by_session_with_error(
            tenant_id=tenant_id,
            model_names=None,
            name=name,
        )

        dimension_model = DimensionModel.model_validate(dimension)
        if validate:
            await self._validate_dimension_attributes([dimension_model], model_names, tenant_id)
        _, dimension_absent_model_names = self._get_models_in_which_dimension_presents_and_absents(
            dimension_model, model_names
        )
        model_names = list(dimension_absent_model_names)
        models = await self.model_repository.get_list_orm_by_names_and_session(tenant_id, model_names)

        value_table = (
            await self.datastorage_repository.get_datastorage_orm_by_session(
                tenant_id, None, dimension.values_table.name
            )
            if dimension.values_table
            else None
        )
        attributes_table = (
            await self.datastorage_repository.get_datastorage_orm_by_session(
                tenant_id, None, dimension.attributes_table.name
            )
            if dimension.attributes_table
            else None
        )
        text_table = (
            await self.datastorage_repository.get_datastorage_orm_by_session(tenant_id, None, dimension.text_table.name)
            if dimension.text_table
            else None
        )
        dimension.text_table = text_table
        dimension.values_table = value_table
        dimension.attributes_table = attributes_table

        await self.dimension_history_repository.save_history(dimension, forced=True)
        not_ignored_models_names = set()
        not_ignored_models = []

        if value_table:
            value_table, not_ignored_value_table_models = (
                await self.datastorage_repository.copy_model_data_storage_orm_by_session(
                    tenant_id,
                    value_table.name,
                    models,
                    value_table,
                    dimension.name,
                    save_history=False,
                    validate=validate,
                )
            )
            for value_model in not_ignored_value_table_models:
                if value_model.name not in not_ignored_models_names:
                    not_ignored_models_names.add(value_model.name)
                    not_ignored_models.append(value_model)
        if attributes_table:
            attributes_table, not_ignored_attributes_table_models = (
                await self.datastorage_repository.copy_model_data_storage_orm_by_session(
                    tenant_id,
                    attributes_table.name,
                    models,
                    attributes_table,
                    dimension.name,
                    save_history=False,
                    validate=validate,
                )
            )
            for attributes_model in not_ignored_attributes_table_models:
                if attributes_model.name not in not_ignored_models_names:
                    not_ignored_models_names.add(attributes_model.name)
                    not_ignored_models.append(attributes_model)
        if text_table:
            text_table, not_ignored_text_table_models = (
                await self.datastorage_repository.copy_model_data_storage_orm_by_session(
                    tenant_id,
                    text_table.name,
                    models,
                    text_table,
                    dimension.name,
                    save_history=False,
                    validate=validate,
                )
            )
            for text_model in not_ignored_text_table_models:
                if text_model.name not in not_ignored_models_names:
                    not_ignored_models_names.add(text_model.name)
                    not_ignored_models.append(text_model)

        dimension.models.extend(models)
        return dimension, not_ignored_models

    async def _validate_dimension_models(self, tenant_id: str, model_names: list[str]) -> None:
        """
        Валидация существования моделей для указанного арендатора перед операциями с измерениями.

        Args:
            tenant_id (str): Идентификатор арендатора.
            model_names (list[str]): Список названий моделей, которые проверяются на наличие.

        Returns:
            None
        """
        models: list[Model] = await self.model_repository.get_list_orm_by_names_and_session(tenant_id, model_names)
        exceptions = []
        exist_models = [model.name for model in models]
        for model_name in model_names:
            if model_name not in exist_models:
                exceptions.append(model_name)
        if exceptions:
            raise NoResultFound(f"Models not found: {', '.join(exceptions)}")

    @staticmethod
    async def _create_tables_for_new_dimension_models(
        dimension: Dimension, models: list[Model], if_not_exists: bool = False, check_possible_delete: bool = True
    ) -> None:
        """
        Создает необходимые таблицы для нового измерения в указанных моделях.

        Args:
            dimension (Dimension): Измерение, для которого создаются таблицы.
            models (list[Model]): Список моделей, где будут созданы таблицы для данного измерения.

        Returns:
            None
        """
        for index, model in enumerate(models):
            generator = get_generator(model)
            try:
                await generator.create_dimension(
                    dimension,
                    model,
                    if_not_exists=if_not_exists,
                )
            except Exception as ext:
                for model in models[:index]:
                    generator = get_generator(model)
                    await generator.delete_dimension(
                        dimension, model, if_exists=True, check_possible_delete=check_possible_delete
                    )
                raise Exception(str(ext))

    @staticmethod
    async def _delete_tables_for_new_dimension_models(
        dimension: Dimension, models: list[Model], check_possible_delete: bool = True
    ) -> None:
        """
        Создает необходимые таблицы для нового измерения в указанных моделях.

        Args:
            dimension (Dimension): Измерение, для которого создаются таблицы.
            models (list[Model]): Список моделей, где будут созданы таблицы для данного измерения.

        Returns:
            None
        """
        for _, model in enumerate(models):
            generator = get_generator(model)
            try:
                await generator.delete_dimension(
                    dimension,
                    model,
                    if_exists=True,
                    check_possible_delete=check_possible_delete,
                )
            except Exception:
                logger.exception("Error during dimension delete: %s", dimension.name)

    async def _get_related_dimensions_and_measures_from_attributes(
        self,
        tenant_id: str,
        model_names: list[str],
        init_dimension: Dimension,
        dimensions_by_name: dict[str, Dimension],
        measures_by_name: dict[str, Measure],
    ) -> tuple[dict[str, Dimension], dict[str, Measure]]:
        """
        Возвращает связанные измерения и метрики на основе атрибутов текущего измерения.

        Args:
            tenant_id (str): Идентификатор арендатора.
            model_names (Optional[list[str]]): Необязательный список названий моделей, ограничивающий область поиска.
            dimension (Dimension): Текущее измерение, от которого зависят остальные измерения и метрики.
            dimensions_by_name (dict[str, Dimension]): Словарь всех измерений, индексированных по имени.
            measures_by_name (dict[str, Measure]): Словарь всех метрик, индексированных по имени.

        Returns:
            tuple[dict[str, Dimension], dict[str, Measure]]: Кортеж, содержащий словарь зависимых измерений и словарь зависимых метрик.
        """
        attribute_dimensions = [
            attr.dimension_attribute for attr in init_dimension.attributes if attr.dimension_attribute
        ]
        attribute_measures = [attr.measure_attribute for attr in init_dimension.attributes if attr.measure_attribute]

        added_dimensions = []
        for dimension in attribute_dimensions:
            if dimension.name not in dimensions_by_name:
                dimension_in_model = await self.get_dimension_orm_model_if_not_present_in_models(
                    tenant_id=tenant_id, name=dimension.name, model_names=model_names
                )
                if dimension_in_model is None:
                    logger.debug("Dimension %s already in models %s", dimension.name, model_names)
                    continue
                dimensions_by_name[dimension_in_model.name] = dimension_in_model
                added_dimensions.append(dimension_in_model)

        for measure in attribute_measures:
            if set(measure.models) == set(model_names):
                logger.debug("Measure %s already in models %s", measure.name, model_names)
                continue
            if measure.name not in measures_by_name:
                measures_by_name[measure.name] = measure

            if measure.dimension and measure.dimension.name not in dimensions_by_name:
                if set(measure.dimension.models) == set(model_names):
                    logger.debug("Dimension %s already in models %s", measure.dimension, model_names)
                    continue
                dimensions_by_name[measure.dimension.name] = measure.dimension
                added_dimensions.append(measure.dimension)

            for filter in measure.filter:
                if set(filter.dimension.models) == set(model_names):
                    logger.debug("Dimension %s already in models %s", filter.dimension, model_names)
                    continue
                dimensions_by_name
                if filter.dimension.name not in dimensions_by_name:
                    dimensions_by_name[filter.dimension.name] = filter.dimension
                    added_dimensions.append(filter.dimension)

        for dimension in added_dimensions:
            await self._get_related_dimensions_and_measures_from_attributes(
                tenant_id, model_names, dimension, dimensions_by_name, measures_by_name
            )
        return dimensions_by_name, measures_by_name

    async def _copy_related_dimensions(
        self,
        tenant_id: str,
        name: str,
        model_names: list[str],
    ) -> tuple[list[Dimension], dict[str, list[Model]], dict[str, str], dict[str, str]]:
        """
        Получает существующие измерения и соответствующие модели для копирования.

        Args:
            tenant_id (str): Идентификатор арендатора.
            name (str): Имя исходного измерения (`dimension`), которое нужно скопировать.
            model_names (list[str]): Список названий моделей, куда будет выполнено копирование.

        Returns:
            tuple[list[Dimension], set[Model]]: Кортеж, содержащий список измерений и набор моделей.
        """
        initial_dimension = await self.get_dimension_orm_model_if_not_present_in_models(
            tenant_id=tenant_id, name=name, model_names=model_names
        )
        if initial_dimension is None:
            logger.debug("Dimension %s already in models %s", name, model_names)
            return [], {}, {}, {}
        related_dimensions, related_measures = await self._get_related_dimensions_and_measures_from_attributes(
            tenant_id, model_names, initial_dimension, {}, {}
        )
        related_dimensions[name] = initial_dimension
        dimensions_to_copy = list(related_dimensions.values())
        copied_dimensions: list[Dimension] = []
        not_copied_dimensions: dict[str, str] = {}

        not_ignored_models_by_dimension_name: dict[str, list[Model]] = defaultdict(list)
        for dimension in dimensions_to_copy:
            try:
                logger.debug("Copy dimension %s", dimension.name)
                dimension, _not_ignored_models = await self.copy_model_dimension_orm_by_session(
                    tenant_id, dimension.name, model_names, validate=False
                )
                await self.dimension_history_repository.update_version(dimension)
                logger.debug("Dimension %s copied", dimension.name)
                copied_dimensions.append(dimension)
            except Exception as exc:
                logger.exception("Dimension %s copy error", dimension.name)
                not_copied_dimensions[dimension.name] = str(exc)
                continue
            not_ignored_models_by_dimension_name[dimension.name].extend(_not_ignored_models)
        models = await self.model_repository.get_list_orm_by_names_and_session(tenant_id, model_names)

        _, not_copied_measures = await self.measure_repository.copy_list_of_measures(
            tenant_id=tenant_id, measures=list(related_measures.values()), model_names=[model.name for model in models]  # type: ignore[arg-type]
        )
        for model in models:
            await self._create_additional_virtual_dimensions(model)
        await self.session.flush(copied_dimensions)
        if not not_copied_dimensions and not not_copied_measures:
            await self._validate_dimension_attributes(
                [DimensionModel.model_validate(dimension) for dimension in copied_dimensions], model_names, tenant_id
            )
        return copied_dimensions, not_ignored_models_by_dimension_name, not_copied_dimensions, not_copied_measures

    async def _update_dimension_by_model_mappings(
        self,
        dimension_model: dict[str, str],
        dimension_datastorages: dict[str, list[int]],
        dimension_id_mapping: dict[str, int],
        model_id_mapping: dict[str, int],
    ) -> None:
        """
        Обновляет отношение владения между измерениями (`Dimension`) и моделями (`Model`).

        Args:
            dimension_model (dict): Словарь, где ключ — имя измерения, значение — название модели-владельца.
            dimension_datastorages (dict): Словарь хранения данных измерений, ключи — имена измерений, значения — списки идентификаторов хранилищ данных.
            dimension_id_mapping (dict): Словарь соответствия имен измерений и их идентификаторов.
            model_id_mapping (dict): Словарь соответствия названий моделей и их идентификаторов.

        Returns:
            None
        """
        # Для каждого отношения измерения-имени модели обновляем флаги владельцев
        for dimension_name, model_name in dimension_model.items():
            # Устанавливаем флаг владельца всех модлей равным False
            update_dimension_is_owner_false_query = (
                update(DimensionModelRelationOrm)
                .where(DimensionModelRelationOrm.dimension_id == dimension_id_mapping[dimension_name])
                .values({"is_owner": False})
            )
            await self.session.execute(update_dimension_is_owner_false_query)

            # Устанавливаем флаг владельца конкретного отношения измерения-модели равным True
            update_dimension_is_owner_true_query = (
                update(DimensionModelRelationOrm)
                .where(
                    DimensionModelRelationOrm.dimension_id == dimension_id_mapping[dimension_name],
                    DimensionModelRelationOrm.model_id == model_id_mapping[model_name],
                )
                .values({"is_owner": True})
            )
            await self.session.execute(update_dimension_is_owner_true_query)

            # Если есть привязанные хранилища данных к данному измерению
            datastorages = dimension_datastorages.get(dimension_name)
            if datastorages:

                # Сбрасываем флаг владельца во всех отношениях хранения данных
                update_ds_is_owner_false_query = (
                    update(DataStorageModelRelation)
                    .where(DataStorageModelRelation.data_storage_id.in_(datastorages))
                    .values({"is_owner": False})
                )
                await self.session.execute(update_ds_is_owner_false_query)

                # Устанавливаем владельцем данные хранилища именно данной модели
                update_ds_is_owner_true_query = (
                    update(DataStorageModelRelation)
                    .where(
                        DataStorageModelRelation.data_storage_id.in_(datastorages),
                        DataStorageModelRelation.model_id == model_id_mapping[model_name],
                    )
                    .values({"is_owner": True})
                )
                await self.session.execute(update_ds_is_owner_true_query)

    async def update_dimension_owners_by_names(self, tenant_id: str, dimension_names: list[str]) -> None:
        """
        Обновляет связи владений между измерениями и моделями на основе указанных имен измерений.

        Args:
            tenant_id (str): Идентификатор арендатора.
            dimension_names (list): Список имен измерений, для которых нужно обновить владельцев.

        Returns:
            None
        """
        if not dimension_names:
            return None
        # Запрашиваем существующие отношения измерений и моделей
        query = (
            select(
                Dimension.id,
                Model.id,
                Dimension.name,
                Model.name,
                DimensionModelRelationOrm.is_owner,
                Dimension.values_table_id,
                Dimension.attributes_table_id,
                Dimension.text_table_id,
            )
            .select_from(DimensionModelRelationOrm)
            .join(Dimension, Dimension.id == DimensionModelRelationOrm.dimension_id)
            .join(Model, Model.id == DimensionModelRelationOrm.model_id)
            .where(Dimension.name.in_(dimension_names), Dimension.tenant_id == tenant_id)
        )

        # Инициализация вспомогательных структур данных
        dimensions_relations = (await self.session.execute(query)).unique().tuples()
        dimension_relations_info = defaultdict(list)
        dimension_datastorages = defaultdict(list)
        dimension_id_mapping = {}
        model_id_mapping = {}
        dimension_owner_model = {}

        # Собираем необходимые данные из запроса
        for (
            dimension_id,
            model_id,
            dimension_name,
            model_name,
            is_current_owner,
            values_table_id,
            attributes_table_id,
            text_table_id,
        ) in dimensions_relations:

            # Сохраняем сопоставления id измерений и моделей
            dimension_id_mapping[dimension_name] = dimension_id
            model_id_mapping[model_name] = model_id

            # Определяем текущую модель владелец измерения
            if is_current_owner:
                dimension_owner_model[dimension_name] = model_name

            # Добавляем таблицы-хранилища к данным измерений
            dimension_datastorages[dimension_name].extend(
                [table_id for table_id in (values_table_id, attributes_table_id, text_table_id) if table_id]
            )

            # Формируем список связанных моделей для каждого измерения
            dimension_relations_info[dimension_name].append(model_name)

        # Составляем словарь на обновление владельца измерения
        dimensions_to_update = {}
        for dimension_name, owner_model in dimension_owner_model.items():
            for model in dimension_relations_info.get(dimension_name, []):
                new_model = get_and_compare_model_name_by_priority(owner_model, model)
                if new_model != owner_model:
                    dimensions_to_update[dimension_name] = new_model

        # Применяем изменения через внутренний метод обновления
        await self._update_dimension_by_model_mappings(
            dimensions_to_update, dimension_datastorages, dimension_id_mapping, model_id_mapping
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
    ) -> tuple[list[DimensionModel], dict[str, str], dict[str, str]]:
        """
        Копирует измерение (`Dimension`) в указанные модели с возможностью выбора копирования атрибутов.

        Args:
            tenant_id (str): Идентификатор арендатора.
            name (str): Имя исходного измерения (`Dimension`), которое нужно скопировать.
            model_names (list[str]): Список названий моделей, куда будет выполнено копирование.
            copy_attributes (bool): Флаг, указывающий, нужно ли копировать атрибуты измерения.

        Returns:
            DimensionModel: Скопированный экземпляр измерения после завершения операции.
        """
        await self._validate_dimension_models(tenant_id, model_names)
        not_copied_dimensions: dict[str, str] = {}
        not_copied_measures: dict[str, str] = {}
        if copy_attributes:
            dimensions, not_ignored_models_by_dimension_name, not_copied_dimensions, not_copied_measures = (
                await self._copy_related_dimensions(
                    tenant_id,
                    name,
                    model_names,
                )
            )
        else:
            dimension, not_ignored_models = await self.copy_model_dimension_orm_by_session(  # type: ignore[assignment]
                tenant_id, name, model_names, validate=True
            )
            await self.dimension_history_repository.update_version(dimension)
            dimensions = [dimension]
            not_ignored_models_by_dimension_name = {dimension.name: not_ignored_models}
        await self.session.flush()
        copied_dimensions = []
        dimension_names = []
        if not not_copied_dimensions and not not_copied_measures and generate_on_db:
            commit = True
            for dimension in dimensions:
                dimension_names.append(dimension.name)
                try:
                    await self._create_tables_for_new_dimension_models(
                        dimension,
                        not_ignored_models_by_dimension_name[dimension.name],
                        if_not_exists,
                        check_possible_delete=check_possible_delete,
                    )
                    copied_dimensions.append(DimensionModel.model_validate(dimension))
                except Exception as exc:
                    logger.exception("Error create tables for dimension %s", dimension.name)
                    commit = False
                    not_copied_dimensions[dimension.name] = str(exc)
            if commit:
                try:
                    await self.update_dimension_owners_by_names(tenant_id, dimension_names)
                    await self.session.commit()
                    for dimension in dimensions:
                        await self.session.refresh(dimension)
                except Exception:
                    logger.exception("Error commit changes")
                    for dimension in dimensions:
                        await self._delete_tables_for_new_dimension_models(
                            dimension,
                            not_ignored_models_by_dimension_name[dimension.name],
                            check_possible_delete=check_possible_delete,
                        )
                    await self.session.rollback()

            else:
                for dimension in dimensions:
                    await self._delete_tables_for_new_dimension_models(
                        dimension,
                        not_ignored_models_by_dimension_name[dimension.name],
                        check_possible_delete=check_possible_delete,
                    )
                await self.session.rollback()
        elif not not_copied_dimensions and not not_copied_measures:
            for dimension in dimensions:
                copied_dimensions.append(DimensionModel.model_validate(dimension))
                dimension_names.append(dimension.name)
            await self.update_dimension_owners_by_names(tenant_id, dimension_names)
            await self.session.commit()
            for dimension in dimensions:
                await self.session.refresh(dimension)

        return (
            copied_dimensions,
            not_copied_dimensions,
            not_copied_measures,
        )

    @staticmethod
    def _get_joined_hierarchies(query: Select[tuple[Dimension]]) -> Select[tuple[Dimension]]:
        """
        Объединяет запрос измерений с информацией о соответствующих иерархиях.

        Args:
            query (Select[tuple[Dimension]]): Начальный запрос измерений.

        Returns:
            Select[tuple[Dimension]]: Модифицированный запрос с присоединёнными иерархиями.
        """
        return query.join(HierarchyBaseDimension, isouter=True).join(HierarchyMeta, isouter=True)

    @classmethod
    def get_by_session(cls, session: AsyncSession) -> "DimensionRepository":
        """
        Получить экземпляр DimensionRepository с использованием сессии.
        Args:
            session (AsyncSession): Сессия для работы с базой данных.
        Returns:
            DimensionRepository: Экземпляр DimensionRepository.
        """
        model_repository = ModelRepository.get_by_session(session)
        datastorage_repository = DataStorageRepository.get_by_session(session)
        database_object_repository = DatabaseObjectRepository(session)
        measure_repository = MeasureRepository.get_by_session(session)
        composite_repository = CompositeRepository(
            session, model_repository, datastorage_repository, database_object_repository
        )
        model_relations_repository = ModelRelationsRepository(
            session=session,
            measure_repository=measure_repository,
            composite_repository=composite_repository,
            model_repository=model_repository,
            datastorage_repository=datastorage_repository,
        )

        return cls(
            session,
            model_repository,
            datastorage_repository,
            database_object_repository,
            model_relations_repository,
            measure_repository,
        )
