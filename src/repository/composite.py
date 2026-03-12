"""
Репозиторий для композита
"""

import copy
from typing import Any, Optional, Sequence

from py_common_lib.logger import EPMPYLogger
from py_common_lib.utils import timeit
from sqlalchemy import select, update
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload, with_loader_criteria

from src.db.any_field import AnyField
from src.db.composite import (
    Composite,
    CompositeDatasource,
    CompositeField,
    CompositeFieldLabel,
    CompositeLabel,
    CompositeLinkFields,
    CompositeModelRelation,
    DatasourceLink,
)
from src.db.data_storage import DataStorageField
from src.db.database_object import DatabaseObject
from src.db.dimension import Dimension
from src.db.measure import Measure
from src.db.model import Model
from src.models.any_field import AnyFieldTypeEnum
from src.models.composite import (
    Composite as CompositeModel,
    CompositeCreateRequest as CompositeCreateRequestModel,
    CompositeEditRequest as CompositeEditRequestModel,
    CompositeFieldRefObjectEnum,
    CompositeLinkTypeEnum,
)
from src.models.consts import DATASOURCE_FIELD
from src.models.database import Database as DatabaseModel
from src.models.database_object import DatabaseObject as DatabaseObjectModel, DbObjectTypeEnum
from src.models.field import BaseFieldTypeEnum, SemanticType
from src.models.model import Model as ModelModel, ModelStatusEnum
from src.models.request_params import Pagination
from src.repository.data_storage import DataStorageRepository
from src.repository.database_object import DatabaseObjectRepository
from src.repository.generators.composite_sql_generator import CompositeSqlGenerator
from src.repository.generators.utils import get_generator
from src.repository.history.composite import CompositeHistoryRepository
from src.repository.model import ModelRepository
from src.repository.utils import (
    add_missing_labels,
    check_exists_object_in_models,
    convert_field_to_orm,
    convert_labels_list_to_orm,
    convert_ref_type_to_orm,
    get_database_schema_database_object_mapping,
    get_object_filtred_by_model_name,
    get_select_query_with_offset_limit_order,
)

logger = EPMPYLogger(__name__)


class CompositeRepository:
    """
    Репозиторий для работы с композитом на уровне базы данных.
    """

    def __init__(
        self,
        session: AsyncSession,
        model_repository: ModelRepository,
        datastorage_repository: DataStorageRepository,
        database_object_repository: DatabaseObjectRepository,
    ) -> None:
        self.session = session
        self.model_repository = model_repository
        self.datastorage_repository = datastorage_repository
        self.database_object_repository = database_object_repository
        self.composite_history_repository = CompositeHistoryRepository(session)

    def get_database_objects_by_model(self, composite_name: str, model: Model) -> list[DatabaseObjectModel]:
        """Получить список databaseObject по имени Composite и модели."""
        model_model = ModelModel.model_validate(model)
        database_objects = [
            DatabaseObjectModel.model_validate(
                {
                    "name": composite_name,
                    "schema_name": model_model.schema_name,
                    "type": DbObjectTypeEnum.VIEW,
                    "specific_attributes": [],
                }
            )
        ]
        return database_objects

    async def create_additional_virtual_dimensions(self, model: Model) -> None:
        """
        Создает виртуальные дополнительные измерения для указанной модели.

        Args:
            model (Model): Модель, для которой создаются виртуальные измерения.
        """
        await self.datastorage_repository.create_fields_dimensions_if_not_exists(
            model,
            {
                DATASOURCE_FIELD: {
                    "precision": 255,
                    "type": AnyFieldTypeEnum.STRING,
                    "name_ru_short": "Источник данных",
                    "name_ru_long": "Источник данных",
                },
            },
            is_virtual=True,
        )

    async def _get_datasources_field_dict(
        self, tenant_id: str, model_name: Optional[str], datasources: list[dict]
    ) -> dict[str, dict[str, Any]]:
        """
        Возвращает мапу, которая содержит отображение полей DataSource в orm объекты.
        Структура нужна, чтобы один раз запросить все объекты из бд.
        Пример:
        {
            "datasource1": {
                "type": "COMPOSITE",
                "source": Composite(),
                "fields" : {
                    "field1": CompositeField()
                }
            }
        }
        """
        result_datasources: dict[str, dict[str, Any]] = {}
        for datasource in datasources:
            if datasource["type"] == CompositeFieldRefObjectEnum.COMPOSITE:
                composite = await self.get_composite_orm_by_session(tenant_id, model_name, datasource["name"])
                if composite is None:
                    raise NoResultFound(
                        f"Composite with tenant_id={tenant_id}, model_name={model_name} and name={datasource['name']} not found."
                    )
                result_datasources[datasource["name"]] = {
                    "fields": {},
                    "source": composite,
                    "type": CompositeFieldRefObjectEnum.COMPOSITE,
                }
                for composite_field in composite.fields:
                    result_datasources[datasource["name"]]["fields"][composite_field.name] = composite_field
            elif datasource["type"] == CompositeFieldRefObjectEnum.DATASTORAGE:
                datastorage = await self.datastorage_repository.get_datastorage_orm_by_session(
                    tenant_id, model_name, datasource["name"]
                )
                if datastorage is None:
                    raise NoResultFound(
                        f"DataStorage with tenant_id={tenant_id}, model_name={model_name} and name={datasource['name']} not found."
                    )
                result_datasources[datasource["name"]] = {
                    "fields": {},
                    "type": CompositeFieldRefObjectEnum.DATASTORAGE,
                    "source": datastorage,
                }
                for dso_field in datastorage.fields:
                    result_datasources[datasource["name"]]["fields"][dso_field.name] = dso_field
            elif datasource["type"] in (CompositeFieldRefObjectEnum.VIEW, CompositeFieldRefObjectEnum.CE_SCENARIO):
                if datasource.get("schema_name") is None and datasource["type"] == CompositeFieldRefObjectEnum.VIEW:
                    raise ValueError("Schema name is required for VIEW datasource")
                result_datasources[datasource["name"]] = {
                    "fields": {},
                    "type": datasource["type"],
                    "name": datasource["name"],
                    "schema_name": datasource.get("schema_name"),
                }
        return result_datasources

    def _convert_datasource_links_to_orm(
        self,
        datasource_links: list[dict],
        datasources_info_dict: dict,
    ) -> list[DatasourceLink]:
        """
        Конвертирует список datasource_links из формата list[dict] в list[DatasourceLink]
        """
        result = []
        for datasource_link in datasource_links:
            if datasources_info_dict[datasource_link["datasource"]]["type"] == CompositeFieldRefObjectEnum.DATASTORAGE:
                data_storage_field = datasources_info_dict[datasource_link["datasource"]]["fields"][
                    datasource_link["datasource_field"]
                ]
                dso_datasource_dict = {
                    "data_storage_field_ref_id": data_storage_field.id,
                    "datasource_type": CompositeFieldRefObjectEnum.DATASTORAGE,
                }
                datasource_link_model = DatasourceLink(**dso_datasource_dict)
                datasource_link_model.data_storage_field_ref = data_storage_field
                result.append(datasource_link_model)
            elif datasources_info_dict[datasource_link["datasource"]]["type"] == CompositeFieldRefObjectEnum.COMPOSITE:
                composite_field = datasources_info_dict[datasource_link["datasource"]]["fields"][
                    datasource_link["datasource_field"]
                ]
                composite_datasource_dict = {
                    "composite_field_ref_id": composite_field.id,
                    "datasource_type": CompositeFieldRefObjectEnum.COMPOSITE,
                }
                datasource_link_model = DatasourceLink(**composite_datasource_dict)
                datasource_link_model.composite_field_ref = composite_field
                result.append(datasource_link_model)
            elif datasources_info_dict[datasource_link["datasource"]]["type"] in (
                CompositeFieldRefObjectEnum.VIEW,
                CompositeFieldRefObjectEnum.CE_SCENARIO,
            ):
                view_datasource_dict = {
                    "undescribed_ref_object_field_name": datasource_link["datasource_field"],
                    "undescribed_ref_object_name": datasource_link["datasource"],
                    "datasource_type": datasources_info_dict[datasource_link["datasource"]]["type"],
                }
                datasource_link_model = DatasourceLink(**view_datasource_dict)
                result.append(datasource_link_model)
            else:
                raise ValueError("Unknown datasource type")
        return result

    async def _update_composite_field_attrs(
        self,
        field: CompositeField,
        field_dict: dict[str, Any],
        tenant_id: str,
        model_names: list[str],
        datasources_info_dict: dict[str, dict[str, Any]],
    ) -> None:
        """
        Обновляет атрибуты существующего ORM-объекта CompositeField из словаря,
        сохраняя id строки в БД (без DELETE + INSERT).

        Args:
            field (CompositeField): Существующий ORM-объект поля.
            field_dict (dict[str, Any]): Словарь с новыми значениями атрибутов поля.
            tenant_id (str): Идентификатор тенанта.
            model_names (list[str]): Список имён моделей для резолва ссылок.
            datasources_info_dict (dict[str, dict[str, Any]]): Словарь информации о datasource'ах.
        """
        old_any_field = field.any_field

        datasource_links = None
        datasource_links_payload = field_dict.pop("datasource_links", None)
        if datasource_links_payload is not None:
            datasource_links = self._convert_datasource_links_to_orm(
                datasource_links_payload,
                datasources_info_dict,
            )

        ref_type = field_dict.pop("ref_type")
        field_dict.pop("sql_column_type", None)
        object_field = await convert_ref_type_to_orm(self.session, tenant_id, model_names, ref_type)

        labels = field_dict.pop("labels", None)
        if labels is not None:
            converted_labels = convert_labels_list_to_orm(labels, CompositeFieldLabel)
            current_labels_values = {(label.language, label.type, label.text) for label in field.labels}
            new_labels_values = {(label.language, label.type, label.text) for label in converted_labels}
            if current_labels_values != new_labels_values:
                field.labels = converted_labels
        field.semantic_type = field_dict.get("semantic_type", field.semantic_type)
        field.sql_name = field_dict.get("sql_name", field.sql_name)
        field.field_type = ref_type["ref_object_type"]
        if datasource_links is not None:
            field.datasource_links = datasource_links

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

    async def _update_composite_fields_in_place(
        self,
        composite_orm: Composite,
        new_fields_dicts: list[dict[str, Any]],
        tenant_id: str,
        model_names: list[str],
        datasources_info_dict: dict[str, dict[str, Any]],
    ) -> None:
        """
        Обновляет поля Composite на месте в InstrumentedList, без замены коллекции.
        Сопоставляет существующие поля по имени и обновляет их атрибуты,
        новые поля добавляет через append, отсутствующие — удаляет через remove.
        Сохраняет id существующих строк, что предотвращает нарушение FK из composite_link_fields.

        Args:
            composite_orm (Composite): ORM-объект композита, чья коллекция fields мутируется.
            new_fields_dicts (list[dict[str, Any]]): Список словарей с новыми значениями полей.
            tenant_id (str): Идентификатор тенанта.
            model_names (list[str]): Список имён моделей для резолва ссылок.
            datasources_info_dict (dict[str, dict[str, Any]]): Словарь информации о datasource'ах.
        """
        existing_by_name: dict[str, CompositeField] = {f.name: f for f in composite_orm.fields}
        new_field_names: set[str] = set()

        for field_dict in new_fields_dicts:
            field_name = field_dict["name"]
            new_field_names.add(field_name)

            if field_name in existing_by_name:
                await self._update_composite_field_attrs(
                    existing_by_name[field_name], field_dict, tenant_id, model_names, datasources_info_dict
                )
            else:
                field_dict["datasource_links"] = self._convert_datasource_links_to_orm(
                    field_dict.pop("datasource_links", []), datasources_info_dict
                )
                model_field = await convert_field_to_orm(
                    self.session, field_dict, tenant_id, model_names, CompositeField
                )
                if not isinstance(model_field, CompositeField):
                    raise ValueError("Failed to cast model_field to CompositeField")
                composite_orm.fields.append(model_field)

        for field_name, field in existing_by_name.items():
            if field_name not in new_field_names:
                composite_orm.fields.remove(field)

    async def _convert_field_object_list_to_orm(
        self,
        tenant_id: str,
        model_names: list[str],
        fields: list[dict[str, Any]],
        datasources_info_dict: dict[str, dict[str, Any]],
    ) -> list[CompositeField]:
        """
        Конвертирует field для composite из формата list[dict] в list[CompositeField]
        """
        result_fields = []
        for field in fields:
            field["datasource_links"] = self._convert_datasource_links_to_orm(
                field.pop("datasource_links", []), datasources_info_dict
            )
            model_field = await convert_field_to_orm(self.session, field, tenant_id, model_names, CompositeField)
            if isinstance(model_field, CompositeField):
                result_fields.append(model_field)
            else:
                raise ValueError("Failed to cast model_field to CompositeField")
        return result_fields

    def get_data_source_field(
        self,
    ) -> dict:
        """
        Возвращает структурированный словарь с данными о поле источника данных.

        Returns:
            dict: Словарь:
                {
                    "name": "data_source",
                    "ref_type": {
                        "ref_object_type":"DIMENSION",
                        "ref_object": "data_source"
                    },
                    "sql_name": "data_source",
                    "semantic_type": "DIMENSION",
                    "datasource_links": []
                }
        """
        return {
            "name": DATASOURCE_FIELD,
            "ref_type": {
                "ref_object_type": BaseFieldTypeEnum.DIMENSION,
                "ref_object": DATASOURCE_FIELD,
            },
            "sql_name": DATASOURCE_FIELD,
            "semantic_type": SemanticType.DIMENSION,
            "datasource_links": [],
        }

    def _convert_datasource_object_dict_to_orm(
        self,
        datasources_info_dict: dict[str, dict[str, Any]],
    ) -> list[CompositeDatasource]:
        """
        Конвертирует datasource'ы из словаря в list[CompositeDatasource].
        Словарь на вход:
        {
            "datasource1": {
                "type": "COMPOSITE",
                "source": Composite(),
                "fields" : {
                    "field1": CompositeField()
                }
            }
        }
        """
        result = []
        for _, datasource in datasources_info_dict.items():
            composite_datasource = (
                datasource["source"] if datasource["type"] == CompositeFieldRefObjectEnum.COMPOSITE else None
            )
            datastorage_datasource = (
                datasource["source"] if datasource["type"] == CompositeFieldRefObjectEnum.DATASTORAGE else None
            )
            name = (
                datasource.get("name")
                if datasource["type"] in (CompositeFieldRefObjectEnum.VIEW, CompositeFieldRefObjectEnum.CE_SCENARIO)
                else None
            )
            schema_name = (
                datasource.get("schema_name")
                if datasource["type"] in (CompositeFieldRefObjectEnum.VIEW, CompositeFieldRefObjectEnum.CE_SCENARIO)
                else None
            )
            composite_datasource_dict = {
                "type": datasource["type"],
                "undescribed_ref_object_name": name,
                "undescribed_ref_object_schema_name": schema_name,
                "composite_datasource_id": composite_datasource.id if composite_datasource is not None else None,
                "datastorage_datasource_id": datastorage_datasource.id if datastorage_datasource is not None else None,
            }
            model_datasource = CompositeDatasource(**composite_datasource_dict)
            model_datasource.composite_datasource = composite_datasource
            model_datasource.datastorage_datasource = datastorage_datasource
            result.append(model_datasource)
        return result

    def _convert_link_fields_object_list_to_orm(
        self,
        link_fields_list: list[dict],
        datasources_info_dict: dict[str, dict[str, Any]],
    ) -> list[CompositeLinkFields]:
        """
        Конвертирует link_fields из списка словарей list[dict] в list[CompositeLinkFields].
        """
        result = []
        for link_field in link_fields_list:
            left_data_storage_field: Optional[DataStorageField] = None
            left_composite_field: Optional[CompositeField] = None
            right_data_storage_field: Optional[DataStorageField] = None
            right_composite_field: Optional[CompositeField] = None
            link_fields: dict = {}
            left_link_field = link_field["left"]
            right_link_field = link_field["right"]
            left_datasource, _ = self._get_field_and_datasource_from_datasources_info(
                datasources_info_dict, left_link_field
            )
            right_datasource, _ = self._get_field_and_datasource_from_datasources_info(
                datasources_info_dict, right_link_field
            )
            if left_datasource["type"] == CompositeFieldRefObjectEnum.DATASTORAGE:
                left_data_storage_field = left_datasource["fields"][left_link_field["datasource_field"]]
                link_fields["left_type"] = CompositeFieldRefObjectEnum.DATASTORAGE
            elif left_datasource["type"] == CompositeFieldRefObjectEnum.COMPOSITE:
                left_composite_field = left_datasource["fields"][left_link_field["datasource_field"]]
                link_fields["left_type"] = CompositeFieldRefObjectEnum.COMPOSITE
            elif left_datasource["type"] in (CompositeFieldRefObjectEnum.CE_SCENARIO, CompositeFieldRefObjectEnum.VIEW):
                link_fields["left_type"] = left_datasource["type"]
                link_fields["left_undescribed_ref_object_name"] = left_link_field["datasource"]
                link_fields["left_undescribed_ref_object_field_name"] = left_link_field["datasource_field"]
            if right_datasource["type"] == CompositeFieldRefObjectEnum.DATASTORAGE:
                right_data_storage_field = right_datasource["fields"][right_link_field["datasource_field"]]
                link_fields["right_type"] = CompositeFieldRefObjectEnum.DATASTORAGE
            elif right_datasource["type"] == CompositeFieldRefObjectEnum.COMPOSITE:
                right_composite_field = right_datasource["fields"][right_link_field["datasource_field"]]
                link_fields["right_type"] = CompositeFieldRefObjectEnum.COMPOSITE
            elif left_datasource["type"] in (CompositeFieldRefObjectEnum.CE_SCENARIO, CompositeFieldRefObjectEnum.VIEW):
                link_fields["right_type"] = left_datasource["type"]
                link_fields["right_undescribed_ref_object_name"] = right_link_field["datasource"]
                link_fields["right_undescribed_ref_object_field_name"] = right_link_field["datasource_field"]
            model_link_fields = CompositeLinkFields(**link_fields)
            model_link_fields.left_data_storage_field = left_data_storage_field
            model_link_fields.left_composite_field = left_composite_field
            model_link_fields.right_data_storage_field = right_data_storage_field
            model_link_fields.right_composite_field = right_composite_field
            result.append(model_link_fields)
        return result

    async def get_composite_orm_by_session(
        self, tenant_id: str, model_name: Optional[str], name: str
    ) -> Optional[Composite]:
        """Получить composite"""
        query = select(Composite).where(
            Composite.name == name,
            Composite.tenant_id == tenant_id,
        )
        query = (
            query.where(Composite.models.any(Model.name == model_name)).options(
                selectinload(Composite.database_objects),
                with_loader_criteria(DatabaseObject, DatabaseObject.models.any(Model.name == model_name)),
            )
            if model_name
            else query.options(selectinload(Composite.database_objects))
        )
        result = (await self.session.execute(query)).unique().scalars().one_or_none()
        return result

    async def get_composite_orm_list_by_session(
        self,
        tenant_id: str,
        model_name: str,
        pagination: Optional[Pagination] = None,
    ) -> Sequence[Composite]:
        """Получить объект композиты (SqlAlchemy модель) по сессии, имени модели и пагинации."""
        query = (
            select(Composite)
            .where(Composite.models.any(Model.name == model_name), Composite.tenant_id == tenant_id)
            .options(
                selectinload(Composite.database_objects),
                with_loader_criteria(DatabaseObject, DatabaseObject.models.any(Model.name == model_name)),
            )
        )
        query = get_select_query_with_offset_limit_order(query, Composite.name, pagination)
        result = (await self.session.execute(query)).unique().scalars().all()
        return result

    @timeit
    async def get_list(
        self, tenant_id: str, model_name: str, pagination: Optional[Pagination] = None
    ) -> list[CompositeModel]:
        """Получить список всех композитов."""
        result = await self.get_composite_orm_list_by_session(tenant_id, model_name, pagination)
        model = await self.model_repository.get_model_orm_by_session_with_error(tenant_id=tenant_id, name=model_name)
        database = DatabaseModel.model_validate(model.database)
        return [
            CompositeModel.model_validate(composite, context={"database_type": database.type}) for composite in result
        ]

    @timeit
    async def get_by_name(self, tenant_id: str, model_name: Optional[str], name: str) -> CompositeModel:
        """
            Получить композит по имени.
        Args:
            tenant_id (str): Идентификатор тенанта.
            model_name (str): Имя модели.
            name (str): Имя композита.
        Returns:
            CompositeModel: Объект композита.
        """
        result = await self.get_composite_orm_by_session(tenant_id=tenant_id, model_name=model_name, name=name)
        context = None
        if model_name:
            model = await self.model_repository.get_model_orm_by_session_with_error(
                tenant_id=tenant_id, name=model_name
            )
            database = DatabaseModel.model_validate(model.database)
            context = {"database_type": database.type}
        if result is None:
            raise NoResultFound(
                f"Composite with tenant_id={tenant_id}, model_name={model_name} and name={name} not found."
            )
        return CompositeModel.model_validate(result, context=context)

    async def get_id_by_name(self, tenant_id: str, model_name: str, name: str) -> int:
        """
            Получить id композита по имени.
        Args:
            tenant_id (str): Идентификатор тенанта.
            model_name (str): Имя модели.
            name (str): Имя композита.
        Returns:
            int: Идентификатор композита.
        """
        result = await self.get_composite_orm_by_session(tenant_id=tenant_id, model_name=model_name, name=name)
        if result:
            return result.id
        else:
            raise NoResultFound(
                f"Composite with tenant_id={tenant_id}, model_name={model_name} and name={name} not found."
            )

    async def delete_without_commit_by_session(
        self, composite: Composite, model_name: str, delete: bool = True
    ) -> Optional[list[DatabaseObjectModel]]:
        """Удалить DSO по сессии и модели."""
        database_object_for_delete = []
        if len(composite.models) > 1:
            await self.composite_history_repository.save_history(composite, forced=True)
            models = list(filter(lambda model: model.name != model_name, composite.models))
            if models == composite.models:
                raise NoResultFound(
                    f"Composite with tenant_id={composite.tenant_id}, model_name={model_name} and name={composite.name} not found."
                )
            composite.models = models
            new_database_objects = []
            database_objects_with_model = get_object_filtred_by_model_name(composite.database_objects, model_name, True)
            database_objects_without_model = get_object_filtred_by_model_name(composite.database_objects, model_name)
            for database_object in database_objects_with_model:
                if len(database_object.models) > 1:
                    database_object_models = list(
                        filter(lambda model: model.name != model_name, database_object.models)
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
            composite.database_objects = new_database_objects
            await self.session.flush()
            await self.composite_history_repository.update_version(composite)
        elif delete:
            await self.composite_history_repository.save_history(composite, deleted=True)
            await self.session.delete(composite)
        await self.session.flush()
        if database_object_for_delete:
            return [
                DatabaseObjectModel.model_validate(database_object) for database_object in database_object_for_delete
            ]
        return None

    @timeit
    async def delete_by_name(self, tenant_id: str, model_name: str, name: str) -> None:
        """Удалить композит по имени."""
        result = await self.get_composite_orm_by_session(tenant_id=tenant_id, model_name=None, name=name)
        if result is None:
            raise NoResultFound(
                f"Composite with tenant_id={tenant_id}, model_name={model_name} and name={name} not found."
            )
        database_object_models = await self.delete_without_commit_by_session(result, model_name)
        model = await self.model_repository.get_model_orm_by_session_with_error(tenant_id, model_name)
        generator = get_generator(model)
        _ = await generator.delete_composite(result, model, database_object_models)
        await self.session.commit()

    def _get_field_and_datasource_from_datasources_info(
        self, datasources_info_dict: dict, field: dict
    ) -> tuple[dict, Any]:
        """Возвращает источник данных и привязанное поле из словаря datasources_info_dict."""
        datasource = datasources_info_dict[field["datasource"]]
        if datasource["type"] in (CompositeFieldRefObjectEnum.VIEW, CompositeFieldRefObjectEnum.CE_SCENARIO):
            return datasource, None
        field = datasource["fields"][field["datasource_field"]]
        return datasource, field

    def validate_link_field(self, datasources_info_dict: dict, link_field: dict) -> None:
        """Проверка поля linkFields на валидность."""
        if datasources_info_dict.get(link_field["datasource"]) is None:
            raise ValueError(
                f"""The datasource {link_field["datasource"]} for field {link_field["datasource_field"]} is not described"""
            )
        if datasources_info_dict[link_field["datasource"]]["fields"].get(
            link_field["datasource_field"]
        ) is None and datasources_info_dict[link_field["datasource"]]["type"] not in (
            CompositeFieldRefObjectEnum.VIEW,
            CompositeFieldRefObjectEnum.CE_SCENARIO,
        ):
            raise ValueError(
                f"""The datasource {link_field["datasource"]} does not contain field {link_field["datasource_field"]}"""
            )

    async def set_owner_model(self, composites: list[Composite], model: Model) -> None:
        """Обновляет состояние владельца модели."""
        composites_ids = [composite.id for composite in composites]
        await self.session.execute(
            update(CompositeModelRelation)
            .where(
                CompositeModelRelation.composite_id.in_(composites_ids),
            )
            .values({"is_owner": False})
        )
        await self.session.execute(
            update(CompositeModelRelation)
            .where(
                CompositeModelRelation.composite_id.in_(composites_ids),
                CompositeModelRelation.model_id == model.id,
            )
            .values({"is_owner": True})
        )

    def validate_fields_and_link_fields(
        self, link_type: str, datasources_info_dict: dict, fields: list[dict], links_fields: list[dict]
    ) -> None:
        """Проверка полей linkFields и fields на валидность."""
        count_scenario_datasource = 0
        for _, datasource_info in datasources_info_dict.items():
            if datasource_info["type"] == CompositeFieldRefObjectEnum.CE_SCENARIO:
                count_scenario_datasource += 1
            if datasource_info["type"] == CompositeFieldRefObjectEnum.VIEW and (
                len(datasources_info_dict) > 1 or link_type != CompositeLinkTypeEnum.SELECT
            ):
                raise ValueError(
                    "For the datasource type 'VIEW', it is possible to use only the 'SELECT' operation with a single Datasource."
                )
        if count_scenario_datasource > 0 and count_scenario_datasource != len(datasources_info_dict):
            raise ValueError("Either all datasources must be of type CE_SCENARIO, or none of them")
        for field in fields:
            for datasource_link in field["datasource_links"]:
                self.validate_link_field(datasources_info_dict, datasource_link)

        for link_field in links_fields:
            self.validate_link_field(datasources_info_dict, link_field["left"])
            self.validate_link_field(datasources_info_dict, link_field["right"])
        return None

    async def generate_composite_sql_expression_by_composite(self, composite: Composite, model: Model) -> str:
        """Генерация sql композита."""
        composite_model = CompositeModel.model_validate(composite)
        fields = [field.model_dump(mode="json") for field in composite_model.fields]
        datasources = [datasource.model_dump(mode="json") for datasource in composite_model.datasources]
        link_fields = [link_field.model_dump(mode="json") for link_field in composite_model.link_fields]
        link_type = composite_model.link_type
        datasources_info_dict = await self._get_datasources_field_dict(composite.tenant_id, model.name, datasources)
        self.validate_fields_and_link_fields(link_type, datasources_info_dict, fields, link_fields)
        model_model = ModelModel.model_validate(model)
        return CompositeSqlGenerator.generate_composite_sql_expression_by_create_parameters(
            link_type,
            datasources_info_dict,
            fields,
            datasources,
            link_fields,
            model_model,
        )

    @timeit
    async def create_by_schema(
        self,
        tenant_id: str,
        model_name: str,
        composite: CompositeCreateRequestModel,
        generate_on_db: bool = True,
        replace: bool = False,
    ) -> CompositeModel:
        """
        Создает композит на основе переданной схемы и параметров.

        Формирует структуру данных для композита, валидирует поля и связи, генерирует SQL-выражение,
        создает соответствующие объекты в базе данных и обновляет историю версий.

        Args:
            tenant_id (str): Идентификатор тенанта.
            model_name (str): Название модели, к которой привязан композит.
            composite (CompositeCreateRequestModel): Объект с данными для создания композита.
        Returns:
            CompositeModel: Модель созданного композита с дополнительными метаданными.

        """
        # Формируем словарь для создания композита
        composite_dict = composite.model_dump(mode="json")
        composite_dict["tenant_id"] = tenant_id
        add_missing_labels(composite_dict["labels"], composite.name)

        # конвертируем dict labels в ORM
        composite_dict["labels"] = convert_labels_list_to_orm(composite_dict["labels"], CompositeLabel)
        link_fields_list = composite_dict.pop("link_fields")

        # формируем словарь со всеми datasources в композите
        datasources_info_dict = await self._get_datasources_field_dict(
            tenant_id, model_name, composite_dict["datasources"]
        )
        # Получаем модель, привязанную к композиту.
        model = await self.model_repository.get_model_orm_by_session_with_error(tenant_id=tenant_id, name=model_name)

        # Создаем технические dimension, если их нет
        await self.create_additional_virtual_dimensions(model)

        model_model = ModelModel.model_validate(model)
        database = DatabaseModel.model_validate(model.database)
        if composite_dict["link_type"] in (CompositeLinkTypeEnum.SELECT, CompositeLinkTypeEnum.UNION):
            link_fields_list = []

        # Валидируем fields и linkFields
        self.validate_fields_and_link_fields(
            composite_dict["link_type"], datasources_info_dict, composite_dict["fields"], link_fields_list
        )
        # Добавляем техническое поле "data_source"
        if composite_dict["link_type"] == CompositeLinkTypeEnum.UNION:
            new_composite_fields = []
            for field in composite_dict["fields"]:
                if field["name"] != DATASOURCE_FIELD:
                    new_composite_fields.append(field)
            new_composite_fields.append(self.get_data_source_field())
            composite_dict["fields"] = new_composite_fields

        # Генерируем sql выражение для композита
        sql_expression = CompositeSqlGenerator.generate_composite_sql_expression_by_create_parameters(
            composite_dict["link_type"],
            datasources_info_dict,
            composite_dict["fields"],
            composite_dict["datasources"],
            link_fields_list,
            model_model,
        )
        composite_dict.pop("datasources")

        # Конвертируем fields в ORM
        composite_dict["fields"] = await self._convert_field_object_list_to_orm(
            tenant_id=tenant_id,
            model_names=[model_name],
            fields=composite_dict["fields"],
            datasources_info_dict=datasources_info_dict,
        )

        # Создаем database_objects для композита
        composite_dict.pop("database_objects", [])
        if datasources_info_dict[next(iter(datasources_info_dict))]["type"] != CompositeFieldRefObjectEnum.CE_SCENARIO:
            composite.database_objects = self.get_database_objects_by_model(composite.name, model)
        else:
            composite.database_objects = []

        # Создаем композит
        composite_orm = Composite(**composite_dict)
        self.session.add(composite_orm)
        composite_orm.models = [model]
        composite_orm.datasources = self._convert_datasource_object_dict_to_orm(datasources_info_dict)
        composite_orm.link_fields = self._convert_link_fields_object_list_to_orm(
            link_fields_list, datasources_info_dict
        )
        database_objects = await self.database_object_repository.create_orm_db_objects(
            tenant_id,
            composite.database_objects,
            [model],
        )
        composite_orm.database_objects = database_objects
        await self.session.flush()
        await self.set_owner_model([composite_orm], model)
        # Обновляем версию композита
        await self.composite_history_repository.update_version(composite_orm, create=True)
        if generate_on_db:
            await self.session.flush()

            # Создаем композит в бд, привязанной к модели.
            generator = get_generator(model)
            _ = await generator.create_composite(composite_orm, model, sql_expression, replace=replace)
        await self.session.commit()
        returned_composite = await self.get_composite_orm_by_session(
            tenant_id=tenant_id, model_name=model_name, name=composite_orm.name
        )
        return CompositeModel.model_validate(returned_composite, context={"database_type": database.type})

    def _get_datasources_list_from_orm_composite(self, composite: Composite) -> list[dict]:
        """Получить список словарей datasources из Composite модели SQLAlchemy."""
        datasources = []
        for datasource in composite.datasources:
            datasource_dict: dict = {"type": datasource.type}
            if datasource.type == CompositeFieldRefObjectEnum.DATASTORAGE and datasource.datastorage_datasource:
                datasource_dict["name"] = datasource.datastorage_datasource.name
            elif datasource.type == CompositeFieldRefObjectEnum.COMPOSITE and datasource.composite_datasource:
                datasource_dict["name"] = datasource.composite_datasource.name
            elif datasource.type in (CompositeFieldRefObjectEnum.VIEW, CompositeFieldRefObjectEnum.CE_SCENARIO):
                datasource_dict["name"] = datasource.undescribed_ref_object_name
                datasource_dict["schema_name"] = datasource.undescribed_ref_object_schema_name
            else:
                raise ValueError("Unknown type datasource")
            datasources.append(datasource_dict)
        return datasources

    def _get_link_fields_list_from_orm_composite(self, composite: Composite) -> list[dict]:
        """Получить список словарей linkFields из Composite модели SQLAlchemy."""
        link_fields = []
        for link_field in composite.link_fields:
            link_field_dict: dict = {
                "right": {},
                "left": {},
            }
            if link_field.left_type == CompositeFieldRefObjectEnum.DATASTORAGE and link_field.left_data_storage_field:
                link_field_dict["left"]["datasource_field"] = link_field.left_data_storage_field.name
                link_field_dict["left"]["datasource"] = link_field.left_data_storage_field.data_storage.name
            elif link_field.left_type == CompositeFieldRefObjectEnum.COMPOSITE and link_field.left_composite_field:
                link_field_dict["left"]["datasource_field"] = link_field.left_composite_field.name
                link_field_dict["left"]["datasource"] = link_field.left_composite_field.composite.name
            elif (
                link_field.left_type == CompositeFieldRefObjectEnum.CE_SCENARIO
                and link_field.left_undescribed_ref_object_name
                and link_field.left_undescribed_ref_object_field_name
            ):
                link_field_dict["left"]["datasource_field"] = link_field.left_undescribed_ref_object_field_name
                link_field_dict["left"]["datasource"] = link_field.left_undescribed_ref_object_name
            else:
                raise ValueError("Unknown type datasource")
            if link_field.right_type == CompositeFieldRefObjectEnum.DATASTORAGE and link_field.right_data_storage_field:
                link_field_dict["right"]["datasource_field"] = link_field.right_data_storage_field.name
                link_field_dict["right"]["datasource"] = link_field.right_data_storage_field.data_storage.name
            elif link_field.right_type == CompositeFieldRefObjectEnum.COMPOSITE and link_field.right_composite_field:
                link_field_dict["right"]["datasource_field"] = link_field.right_composite_field.name
                link_field_dict["right"]["datasource"] = link_field.right_composite_field.composite.name
            elif (
                link_field.right_type == CompositeFieldRefObjectEnum.CE_SCENARIO
                and link_field.right_undescribed_ref_object_name
                and link_field.right_undescribed_ref_object_field_name
            ):
                link_field_dict["left"]["datasource_field"] = link_field.right_undescribed_ref_object_field_name
                link_field_dict["left"]["datasource"] = link_field.right_undescribed_ref_object_name
            else:
                raise ValueError("Unknown type datasource")
            link_fields.append(link_field_dict)
        return link_fields

    def _get_fields_list_from_orm_composite(self, composite: Composite) -> list[dict]:
        """Получить список словарей fields из Composite модели SQLAlchemy."""
        fields = []
        for field in composite.fields:
            field_dict: dict = {}
            datasource_link_list: list[dict] = []
            for datasource_link in field.datasource_links:
                if (
                    datasource_link.datasource_type == CompositeFieldRefObjectEnum.COMPOSITE
                    and datasource_link.composite_field_ref
                ):
                    datasource_link_list.append(
                        {
                            "datasource_field": datasource_link.composite_field_ref.name,
                            "datasource": datasource_link.composite_field_ref.composite.name,
                        }
                    )

                elif (
                    datasource_link.datasource_type == CompositeFieldRefObjectEnum.DATASTORAGE
                    and datasource_link.data_storage_field_ref
                ):
                    datasource_link_list.append(
                        {
                            "datasource_field": datasource_link.data_storage_field_ref.name,
                            "datasource": datasource_link.data_storage_field_ref.data_storage.name,
                        }
                    )
                elif (
                    datasource_link.datasource_type
                    in (CompositeFieldRefObjectEnum.VIEW, CompositeFieldRefObjectEnum.CE_SCENARIO)
                    and datasource_link.undescribed_ref_object_name
                    and datasource_link.undescribed_ref_object_field_name
                ):
                    datasource_link_list.append(
                        {
                            "datasource_field": datasource_link.undescribed_ref_object_field_name,
                            "datasource": datasource_link.undescribed_ref_object_name,
                        }
                    )
                else:
                    raise ValueError("Unknown datasource_link.datasource_type or not found ref object.")
            field_dict["datasource_links"] = datasource_link_list
            field_dict["sql_name"] = field.sql_name
            field_dict["name"] = field.name
            fields.append(field_dict)
        return fields

    async def update_composite_attributes(
        self,
        original_composite: Composite,
        model: Model,
        composite_dict: dict,
        datasources_info_dict: dict,
    ) -> bool:
        """
        Обновить атрибуты композита.

        Args:
            session (AsyncSession): сессия в бд.
            composite (CompositeEditRequestModel): Запрос на изменение композита.
            original_composite (Composite): композит, который хотим изменить.
            model (Model): модель, где лежит композит.
            composite_dict (dict): словарь с полями на изменение композита.
            datasources_info_dict (dict): словарь, сформированный _get_datasources_field_dict.

        Returns:
            bool: флаг необходимости обновить sql_expression для композита.
        """
        regenerate_sql_expression = False
        model_names = [composite_model.name for composite_model in original_composite.models]
        if composite_dict.get("fields") is not None:
            await self._update_composite_fields_in_place(
                composite_orm=original_composite,
                new_fields_dicts=copy.deepcopy(composite_dict.pop("fields")),
                tenant_id=model.tenant_id,
                model_names=model_names,
                datasources_info_dict=datasources_info_dict,
            )
            regenerate_sql_expression = True
        if composite_dict.get("link_fields") is not None:
            original_composite.link_fields = self._convert_link_fields_object_list_to_orm(
                composite_dict.pop("link_fields"), datasources_info_dict
            )
            regenerate_sql_expression = True
        if composite_dict.get("datasources") is not None:
            composite_dict.pop("datasources")
            original_composite.datasources = self._convert_datasource_object_dict_to_orm(datasources_info_dict)
            regenerate_sql_expression = True
        if composite_dict.get("labels") is not None:
            add_missing_labels(composite_dict["labels"], original_composite.name)
            original_composite.labels = convert_labels_list_to_orm(
                composite_dict.pop("labels"),
                CompositeLabel,
            )
        if composite_dict.get("link_type"):
            regenerate_sql_expression = True
        return regenerate_sql_expression

    @timeit
    async def update_by_name_and_schema(
        self,
        tenant_id: str,
        model_name: str,
        name: str,
        composite: CompositeEditRequestModel,
        generate_on_db: bool = True,
    ) -> CompositeModel:
        """
        Обновляет композит по указанному имени и схеме.

        Args:
            tenant_id (str): Идентификатор тенанта.
            model_name (str): Имя модели, связанной с композитом.
            name (str): Имя обновляемого композита.
            composite (CompositeEditRequestModel): Объект с данными для обновления композита.

        Returns:
            CompositeModel: Обновленная модель композита.

        Raises:
            NoResultFound: Если композит с указанными параметрами не найден.
        """
        composite_dict = composite.model_dump(mode="json", exclude_none=True, exclude_unset=True)
        original_composite = await self.get_composite_orm_by_session(tenant_id=tenant_id, model_name=None, name=name)
        if original_composite is None:
            raise NoResultFound(
                f"Composite with tenant_id={tenant_id}, model_name={model_name} and name={name} not found."
            )
        await self.composite_history_repository.save_history(original_composite, edit_model=composite_dict)
        return_model = None
        for model in original_composite.models:
            if model.name == model_name:
                return_model = model
        composite_dict.pop("database_objects", [])
        model = await self.model_repository.get_model_orm_by_session_with_error(tenant_id=tenant_id, name=model_name)
        expected_schema = ModelModel.model_validate(model).schema_name
        database_objects = get_object_filtred_by_model_name(
            original_composite.database_objects,
            model_name,
            True,
        )
        for db_object in database_objects:
            db_object.schema_name = expected_schema
        await self.create_additional_virtual_dimensions(model)
        database = DatabaseModel.model_validate(model.database)
        datasources = composite_dict.get(
            "datasources",
            self._get_datasources_list_from_orm_composite(original_composite),
        )
        link_fields_list = composite_dict.get(
            "link_fields", self._get_link_fields_list_from_orm_composite(original_composite)
        )
        link_type = composite_dict.get("link_type", original_composite.link_type)
        fields = composite_dict.get("fields", self._get_fields_list_from_orm_composite(original_composite))
        if composite_dict.get("fields") and link_type == CompositeLinkTypeEnum.UNION:
            new_composite_fields = []
            for field in fields:
                if field["name"] != DATASOURCE_FIELD:
                    new_composite_fields.append(field)
            new_composite_fields.append(self.get_data_source_field())
            fields = new_composite_fields
            composite_dict["fields"] = new_composite_fields
        datasources_info_dict = await self._get_datasources_field_dict(tenant_id, None, datasources)
        if link_type in (CompositeLinkTypeEnum.SELECT, CompositeLinkTypeEnum.UNION):
            link_fields_list = []
        self.validate_fields_and_link_fields(link_type, datasources_info_dict, fields, link_fields_list)
        regenerate_sql_expression = await self.update_composite_attributes(
            original_composite,
            model,
            composite_dict,
            datasources_info_dict,
        )
        await self.session.execute(
            update(CompositeModelRelation)
            .where(
                CompositeModelRelation.composite_id == original_composite.id,
            )
            .values({"status": ModelStatusEnum.PENDING})
        )
        if composite_dict:
            await self.session.execute(
                update(Composite)
                .where(
                    Composite.name == name,
                    Composite.models.any(Model.name == model_name),
                    Composite.tenant_id == tenant_id,
                )
                .values(composite_dict)
            )
        await self.session.flush()
        result = await self.get_composite_orm_by_session(tenant_id=tenant_id, model_name=None, name=name)
        if result:
            await self.composite_history_repository.update_version(result)
        await self.session.flush()
        if regenerate_sql_expression and result and generate_on_db:
            updated_db = set()
            for model in result.models:
                model_model = ModelModel.model_validate(model)
                if model_model.database is None:
                    raise ValueError("Database is None")
                if (model_model.schema_name, model_model.database.name) in updated_db:
                    logger.debug(
                        "Composite %s for database %s and schema %s has already been updated",
                        result.name,
                        model_model.database.name,
                        model_model.schema_name,
                    )
                    continue
                updated_db.add((model_model.schema_name, model_model.database.name))
                generator = get_generator(model)
                model_model = ModelModel.model_validate(model)
                sql_expression = CompositeSqlGenerator.generate_composite_sql_expression_by_create_parameters(
                    link_type,
                    datasources_info_dict,
                    fields,
                    datasources,
                    link_fields_list,
                    model_model,
                )
                await generator.update_composite(result, model, sql_expression)
        await self.session.commit()
        context = None
        if return_model:
            database = DatabaseModel.model_validate(return_model.database)
            context = {"database_type": database.type}
        composite_response = CompositeModel.model_validate(result, context=context)
        composite_response.database_objects = get_object_filtred_by_model_name(
            composite_response.database_objects, model_name, True
        )
        return composite_response

    def check_exists_datasource_in_models(self, composite: Composite, model_names: list[str]) -> bool:
        """Проверка наличия Datasource композита в моделях"""
        for datasource in composite.datasources:
            if datasource.composite_datasource:
                _ = check_exists_object_in_models(datasource.composite_datasource, model_names)
            elif datasource.datastorage_datasource:
                _ = check_exists_object_in_models(datasource.datastorage_datasource, model_names)
        return True

    async def update_models_composite(
        self,
        composite: Composite,
        models: list[Model],
    ) -> tuple[list, list]:
        original_databases_database_objects_dict, appended_database_objects = (
            get_database_schema_database_object_mapping(composite)
        )
        composite.models.extend(models)
        not_ignored = []
        ignored = []
        tenant_id = composite.tenant_id
        for model in models:
            model_model = ModelModel.model_validate(model)
            if model_model.database is None:
                raise ValueError("Database is None")
            if (model_model.database.name, model_model.schema_name) not in original_databases_database_objects_dict:
                not_ignored.append(model)
                database_objects = self.get_database_objects_by_model(composite.name, model)
                new_database_objects = []
                if composite.datasources[0].type != CompositeFieldRefObjectEnum.CE_SCENARIO:
                    new_database_objects = await self.database_object_repository.create_orm_db_objects(
                        tenant_id,
                        database_objects,
                        [model],
                        composite_id=composite.id,
                    )
                composite.database_objects.extend(new_database_objects)
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

    async def copy_model_composite_orm_by_session(
        self,
        tenant_id: str,
        name: str,
        models: list[Model],
        composite: Optional[Composite] = None,
    ) -> tuple[Optional[Composite], list[Model]]:
        """Копировать Composite в модели по сессии."""
        if not composite:
            composite = await self.get_composite_orm_by_session(
                tenant_id=tenant_id,
                model_name=None,
                name=name,
            )
        if not composite:
            return None, []

        composite_model = CompositeModel.model_validate(composite)
        datasources = [datasource.model_dump(mode="json") for datasource in composite_model.datasources]
        composite_dict = composite_model.model_dump(mode="json")
        model_names = [model.name for model in models]
        _ = self.check_exists_datasource_in_models(composite, model_names)
        datasource_info_dict = await self._get_datasources_field_dict(tenant_id, None, datasources)
        _ = await self._convert_field_object_list_to_orm(
            tenant_id, model_names, composite_dict["fields"], datasource_info_dict
        )
        orig_model_names = [model_status.name for model_status in composite_model.models_statuses]
        for model_name in model_names:
            if model_name in orig_model_names:
                raise ValueError(f"Composite already exists in Model with name={model_name}.")
        await self.composite_history_repository.save_history(composite, forced=True)
        models = await self.model_repository.get_list_orm_by_names_and_session(tenant_id, model_names)
        not_ignored, ignored = await self.update_models_composite(composite, models)
        logger.debug(
            "Composite is copied with the creation of physical objects: %s. Copied without creation: %s",
            not_ignored,
            ignored,
        )
        return composite, not_ignored

    async def create_composite_in_db_by_models(
        self, composite: Composite, models: list[Model], replace: bool = True
    ) -> None:
        """Создать композит в физических базах данных привязанных к моделям"""
        created_db = set()
        for index, model in enumerate(models):
            model_model = ModelModel.model_validate(model)
            if model_model.database is None:
                raise ValueError("Database is None")
            if (model_model.schema_name, model_model.database.name) in created_db:
                logger.debug(
                    "Composite %s for database %s and schema %s has already been created",
                    composite.name,
                    model_model.database.name,
                    model_model.schema_name,
                )
                continue
            created_db.add((model_model.schema_name, model_model.database.name))
            sql_expression = await self.generate_composite_sql_expression_by_composite(composite, model)
            generator = get_generator(model)
            try:
                _ = await generator.create_composite(composite, model, sql_expression, replace=replace)
            except Exception as ext:
                for model in models[:index]:
                    generator = get_generator(model)
                    _ = await generator.delete_composite(composite, model)
                raise Exception(str(ext))

    async def copy_model_composite(
        self,
        tenant_id: str,
        name: str,
        model_names: list[str],
        generate_on_db: bool = True,
        replace: bool = True,
    ) -> CompositeModel:
        """
        Копирует составную композит (Composite) в другие модели.

        Args:
            tenant_id (str): Идентификатор тенанта (tenant).
            name (str): Имя композита.
            model_names (list[str]): Список имен моделей в которые необходимо проивзести копирование.

        Returns:
            CompositeModel: Объект составной модели после успешного копирования.

        Raises:
            NoResultFound: Если хотя бы одна из указанных моделей или композит не найдены.
        """
        models: list[Model] = await self.model_repository.get_list_orm_by_names_and_session(tenant_id, model_names)
        exceptions = []
        exist_models = [model.name for model in models]
        for model_name in model_names:
            if model_name not in exist_models:
                exceptions.append(model_name)
        if exceptions:
            raise NoResultFound(f"Models not found: {', '.join(exceptions)}")
        models = sorted(models, key=lambda model: model_names.index(model.name))
        for model in models:
            await self.create_additional_virtual_dimensions(model)
        composite, not_ignored_models = await self.copy_model_composite_orm_by_session(tenant_id, name, models)
        if not composite:
            raise NoResultFound(f"Composite with tenant_id={tenant_id} and name={name} not found.")
        await self.session.flush()
        await self.composite_history_repository.update_version(composite)
        if generate_on_db:
            await self.session.flush()
            await self.create_composite_in_db_by_models(composite, not_ignored_models, replace)
        await self.session.commit()
        await self.session.refresh(composite)
        return CompositeModel.model_validate(composite)

    @classmethod
    def get_by_session(cls, session: AsyncSession) -> "CompositeRepository":
        model_repository = ModelRepository.get_by_session(session)
        database_object_repository = DatabaseObjectRepository(session)
        datastorage_repository = DataStorageRepository(session, model_repository, database_object_repository)
        return cls(session, model_repository, datastorage_repository, database_object_repository)

