"""Схемы Pydantic для описания Composite."""

from enum import StrEnum
from typing import Annotated, Any, Optional

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, model_validator

from src.config import models_limitations
from src.integration.aor.model import AorType
from src.models.database_object import DatabaseObject
from src.models.label import Label
from src.models.model import ModelStatus
from src.models.object_field import ObjectField, ObjectFieldRequest
from src.models.object_name import ObjectName
from src.models.version import Versioned


class CompositeFieldRefObjectEnum(StrEnum):
    """
    Перечисление, представляющее типы объектов, связанных с составными полями.

    Каждое значение перечисления является строковым идентификатором, используемым для
    обозначения категории объекта в системе. Используется в качестве ограничения или
    фильтрации при работе с составными структурами данных.

    Attibutes:
        DATASTORAGE: Объект, представляющий хранилище данных.
        COMPOSITE: Составной объект, состоящий из нескольких компонентов.
        VIEW: Представление данных (например, визуализация или отчет).
        CE_SCENARIO: Сценарий обработки данных в контексте бизнес-процесса (Нужен для Calculation Engine).
    """

    DATASTORAGE = "DATASTORAGE"
    COMPOSITE = "COMPOSITE"
    VIEW = "VIEW"
    CE_SCENARIO = "CE_SCENARIO"


class CompositeLinkTypeEnum(StrEnum):
    LEFT_JOIN = "LEFT_JOIN"
    INNER_JOIN = "INNER_JOIN"
    UNION = "UNION"
    SELECT = "SELECT"


JOIN_OPERATIONS: dict = {
    CompositeLinkTypeEnum.INNER_JOIN: "INNER JOIN",
    CompositeLinkTypeEnum.LEFT_JOIN: "LEFT JOIN",
}


class CompositeDatasource(BaseModel):
    """Источники данных композита."""

    name: str = Field(
        description=models_limitations["composite_datasource"]["name"]["description"],
        min_length=models_limitations["composite_datasource"]["name"]["min_length"],
        max_length=models_limitations["composite_datasource"]["name"]["max_length"],
        pattern=models_limitations["composite_datasource"]["name"]["pattern"],
    )
    schema_name: Optional[str] = Field(
        description=models_limitations["composite_datasource"]["schema_name"]["description"],
        min_length=models_limitations["composite_datasource"]["schema_name"]["min_length"],
        max_length=models_limitations["composite_datasource"]["schema_name"]["max_length"],
        pattern=models_limitations["composite_datasource"]["schema_name"]["pattern"],
        serialization_alias=models_limitations["composite_datasource"]["schema_name"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["composite_datasource"]["schema_name"]["validation_alias"][0],
            models_limitations["composite_datasource"]["schema_name"]["validation_alias"][1],
        ),
        default=None,
    )
    type: CompositeFieldRefObjectEnum = Field(
        description=models_limitations["composite_datasource"]["type"]["description"],
    )
    model_config = ConfigDict(from_attributes=True)

    @model_validator(mode="before")
    @classmethod
    def add_additional_fields_to_composite_datasource(cls, datasource: Any) -> Any:
        """
        Добавляет перед валидацией дополнительные поля в валидируемый объект, если это возможно.
        Поля: name.
        """
        if (not hasattr(datasource, "type") or not hasattr(datasource, "schema_name")) and (
            not hasattr(datasource, "datastorage_datasource")
            and not hasattr(datasource, "composite_datasource")
            and not hasattr(datasource, "undescribed_ref_object_name")
        ):
            return datasource
        datasource.schema_name = None
        if datasource.type == CompositeFieldRefObjectEnum.DATASTORAGE:
            datasource.name = datasource.datastorage_datasource.name
        elif datasource.type == CompositeFieldRefObjectEnum.COMPOSITE:
            datasource.name = datasource.composite_datasource.name
        elif datasource.type in (CompositeFieldRefObjectEnum.VIEW, CompositeFieldRefObjectEnum.CE_SCENARIO):
            datasource.name = datasource.undescribed_ref_object_name
            datasource.schema_name = datasource.undescribed_ref_object_schema_name
        else:
            raise ValueError(f"Unknown type for datasource: {datasource.type}")
        return datasource


class CompositeFieldName(BaseModel):
    """Модель, используемая для получения имени композита из ссылки на поле."""

    name: str = Field(
        description=models_limitations["composite"]["name"]["description"],
        min_length=models_limitations["composite"]["name"]["min_length"],
        max_length=models_limitations["composite"]["name"]["max_length"],
        pattern=models_limitations["composite"]["name"]["pattern"],
    )
    composite: ObjectName
    model_config = ConfigDict(from_attributes=True)


class DataStorageFieldName(BaseModel):
    """Модель, используемая для получения имени композита из ссылки на поле."""

    name: str = Field(
        description=models_limitations["data_storage"]["name"]["description"],
        min_length=models_limitations["data_storage"]["name"]["min_length"],
        max_length=models_limitations["data_storage"]["name"]["max_length"],
        pattern=models_limitations["data_storage"]["name"]["pattern"],
    )
    data_storage: ObjectName
    model_config = ConfigDict(from_attributes=True)


class DatasourceLinkRequest(BaseModel):
    """Схема для изменения/создания источников данных, привязанных к полям композита."""

    datasource: str = Field(
        description=models_limitations["datasource"]["datasource"]["description"],
        pattern=models_limitations["datasource"]["datasource"]["pattern"],
        max_length=models_limitations["datasource"]["datasource"]["max_length"],
        min_length=models_limitations["datasource"]["datasource"]["min_length"],
    )
    datasource_field: str = Field(
        description=models_limitations["datasource"]["datasource_field"]["description"],
        serialization_alias=models_limitations["datasource"]["datasource_field"]["serialization_alias"],
        pattern=models_limitations["datasource"]["datasource_field"]["pattern"],
        max_length=models_limitations["datasource"]["datasource_field"]["max_length"],
        min_length=models_limitations["datasource"]["datasource_field"]["min_length"],
        validation_alias=AliasChoices(
            models_limitations["datasource"]["datasource_field"]["validation_alias"][0],
            models_limitations["datasource"]["datasource_field"]["validation_alias"][1],
        ),
    )


class CompositeLinkFields(BaseModel):
    """Схема для порядка полей join Composite для запроса."""

    left: DatasourceLinkRequest = Field(
        description=models_limitations["composite_link_fields_request"]["left"]["description"]
    )
    right: DatasourceLinkRequest = Field(
        description=models_limitations["composite_link_fields_request"]["right"]["description"]
    )
    model_config = ConfigDict(from_attributes=True)

    @model_validator(mode="before")
    @classmethod
    def add_additional_fields_to_composite_link_field(cls, linkfield: Any) -> Any:
        """
        Добавляет перед валидацией дополнительные поля в валидируемый объект, если это возможно.
        Поля: name, schema_name.
        """
        if hasattr(linkfield, "left_type") and linkfield.left_type == CompositeFieldRefObjectEnum.COMPOSITE:
            linkfield.left = DatasourceLinkRequest(
                datasource=linkfield.left_composite_field.composite.name,
                datasource_field=linkfield.left_composite_field.name,
            )
        elif hasattr(linkfield, "left_type") and linkfield.left_type == CompositeFieldRefObjectEnum.DATASTORAGE:
            linkfield.left = DatasourceLinkRequest(
                datasource=linkfield.left_data_storage_field.data_storage.name,
                datasource_field=linkfield.left_data_storage_field.name,
            )
        elif hasattr(linkfield, "left_type") and linkfield.left_type in (
            CompositeFieldRefObjectEnum.VIEW,
            CompositeFieldRefObjectEnum.CE_SCENARIO,
        ):
            linkfield.left = DatasourceLinkRequest(
                datasource=linkfield.left_undescribed_ref_object_name,
                datasource_field=linkfield.left_undescribed_ref_object_field_name,
            )
        elif hasattr(linkfield, "left_type"):
            raise ValueError(f"Unknown left_type for linkfield: {linkfield.left_type}")

        if hasattr(linkfield, "right_type") and linkfield.right_type == CompositeFieldRefObjectEnum.COMPOSITE:
            linkfield.right = DatasourceLinkRequest(
                datasource=linkfield.right_composite_field.composite.name,
                datasource_field=linkfield.right_composite_field.name,
            )
        elif hasattr(linkfield, "right_type") and linkfield.right_type == CompositeFieldRefObjectEnum.DATASTORAGE:
            linkfield.right = DatasourceLinkRequest(
                datasource=linkfield.right_data_storage_field.data_storage.name,
                datasource_field=linkfield.right_data_storage_field.name,
            )
        elif hasattr(linkfield, "right_type") and linkfield.right_type in (
            CompositeFieldRefObjectEnum.VIEW,
            CompositeFieldRefObjectEnum.CE_SCENARIO,
        ):
            linkfield.right = DatasourceLinkRequest(
                datasource=linkfield.right_undescribed_ref_object_name,
                datasource_field=linkfield.right_undescribed_ref_object_field_name,
            )
        elif hasattr(linkfield, "right_type"):
            raise ValueError(f"Unknown right_type for linkfield: {linkfield.right_type}")

        return linkfield


class DatasourceLink(BaseModel):
    """Схема для чтения источников данных, привязанных к полям композита."""

    composite_field_ref: Optional[CompositeFieldName] = Field(exclude=True, default=None)
    data_storage_field_ref: Optional[DataStorageFieldName] = Field(exclude=True, default=None)
    undescribed_ref_object_field_name: Optional[str] = Field(exclude=True, default=None)
    undescribed_ref_object_name: Optional[str] = Field(exclude=True, default=None)
    datasource_type: Optional[CompositeFieldRefObjectEnum] = Field(exclude=True, default=None)
    datasource: str = Field(
        description=models_limitations["datasource"]["datasource"]["description"],
        pattern=models_limitations["datasource"]["datasource"]["pattern"],
        max_length=models_limitations["datasource"]["datasource"]["max_length"],
        min_length=models_limitations["datasource"]["datasource"]["min_length"],
        default=None,
        validate_default=True,
    )
    datasource_field: str = Field(
        description=models_limitations["datasource"]["datasource_field"]["description"],
        serialization_alias=models_limitations["datasource"]["datasource_field"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["datasource"]["datasource_field"]["validation_alias"][0],
            models_limitations["datasource"]["datasource_field"]["validation_alias"][1],
        ),
        validate_default=True,
        default=None,
        pattern=models_limitations["datasource"]["datasource_field"]["pattern"],
        max_length=models_limitations["datasource"]["datasource_field"]["max_length"],
        min_length=models_limitations["datasource"]["datasource_field"]["min_length"],
    )
    model_config = ConfigDict(from_attributes=True)

    @model_validator(mode="before")
    @classmethod
    def add_additional_fields_to_datasource_link(cls, datasource_link_obj: Any) -> Any:
        """
        Добавляет перед валидацией дополнительные поля в валидируемый объект, если это возможно.
        Поля: datasource, datasource_field.
        """
        if hasattr(datasource_link_obj, "datasource_type"):
            if (
                datasource_link_obj.datasource_type == CompositeFieldRefObjectEnum.COMPOSITE
                and datasource_link_obj.composite_field_ref
            ):
                datasource_link_obj.datasource = datasource_link_obj.composite_field_ref.composite.name
                datasource_link_obj.datasource_field = datasource_link_obj.composite_field_ref.name
            if (
                datasource_link_obj.datasource_type == CompositeFieldRefObjectEnum.DATASTORAGE
                and datasource_link_obj.data_storage_field_ref
            ):
                datasource_link_obj.datasource = datasource_link_obj.data_storage_field_ref.data_storage.name
                datasource_link_obj.datasource_field = datasource_link_obj.data_storage_field_ref.name
            if (
                datasource_link_obj.datasource_type
                in (CompositeFieldRefObjectEnum.VIEW, CompositeFieldRefObjectEnum.CE_SCENARIO)
                and datasource_link_obj.undescribed_ref_object_field_name
            ):
                datasource_link_obj.datasource = datasource_link_obj.undescribed_ref_object_name
                datasource_link_obj.datasource_field = datasource_link_obj.undescribed_ref_object_field_name

        return datasource_link_obj


class CompositeField(ObjectField):
    """Схема для чтения поля композита."""

    datasource_links: list[DatasourceLink] = Field(
        description=models_limitations["composite_field"]["datasource_links"]["description"],
        serialization_alias=models_limitations["composite_field"]["datasource_links"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["composite_field"]["datasource_links"]["validation_alias"][0],
            models_limitations["composite_field"]["datasource_links"]["validation_alias"][1],
        ),
        min_length=models_limitations["composite_field"]["datasource_links"]["min_length"],
        max_length=models_limitations["composite_field"]["datasource_links"]["max_length"],
        default=[],
    )
    model_config = ConfigDict(from_attributes=True)


class CompositeFieldRequest(ObjectFieldRequest):
    """Схема для изменения/создание поля композита."""

    datasource_links: list[DatasourceLinkRequest] = Field(
        description=models_limitations["composite_field"]["datasource_links"]["description"],
        serialization_alias=models_limitations["composite_field"]["datasource_links"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["composite_field"]["datasource_links"]["validation_alias"][0],
            models_limitations["composite_field"]["datasource_links"]["validation_alias"][1],
        ),
        min_length=models_limitations["composite_field"]["datasource_links"]["min_length"],
        max_length=models_limitations["composite_field"]["datasource_links"]["max_length"],
        default=[],
    )


class Composite(Versioned, BaseModel):
    """Композит."""

    name: str = Field(
        description=models_limitations["composite"]["name"]["description"],
        min_length=models_limitations["composite"]["name"]["min_length"],
        max_length=models_limitations["composite"]["name"]["max_length"],
        pattern=models_limitations["composite"]["name"]["pattern"],
    )
    aor_type: AorType = Field(
        default=AorType.COMPOSITE,
        serialization_alias=models_limitations["aor_type"]["serialization_alias"],
        validation_alias=AliasChoices(*models_limitations["aor_type"]["validation_alias"]),
    )
    models_names: list[
        Annotated[
            str,
            Field(
                description=models_limitations["object_name_32"]["description"],
                min_length=models_limitations["object_name_32"]["min_length"],
                max_length=models_limitations["object_name_32"]["max_length"],
                pattern=models_limitations["object_name_32"]["pattern"],
            ),
        ]
    ] = Field(
        description=models_limitations["data_storage"]["models_names"]["description"],
        min_length=models_limitations["data_storage"]["models_names"]["min_length"],
        max_length=models_limitations["data_storage"]["models_names"]["max_length"],
        serialization_alias=models_limitations["data_storage"]["models_names"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["data_storage"]["models_names"]["validation_alias"][0],
            models_limitations["data_storage"]["models_names"]["validation_alias"][1],
        ),
    )
    models_statuses: list[ModelStatus] = Field(
        description=models_limitations["data_storage"]["models_statuses"]["description"],
        min_length=models_limitations["data_storage"]["models_statuses"]["min_length"],
        max_length=models_limitations["data_storage"]["models_statuses"]["max_length"],
        serialization_alias=models_limitations["data_storage"]["models_statuses"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["data_storage"]["models_statuses"]["validation_alias"][0],
            models_limitations["data_storage"]["models_statuses"]["validation_alias"][1],
        ),
    )
    datasources: list[CompositeDatasource] = Field(
        description=models_limitations["composite"]["datasources"]["description"],
        max_length=models_limitations["composite"]["datasources"]["max_length"],
        min_length=models_limitations["composite"]["datasources"]["min_length"],
        default=[],
    )
    link_type: CompositeLinkTypeEnum = Field(
        description=models_limitations["composite"]["link_type"]["description"],
        serialization_alias=models_limitations["composite"]["link_type"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["composite"]["link_type"]["validation_alias"][0],
            models_limitations["composite"]["link_type"]["validation_alias"][1],
        ),
        default=CompositeLinkTypeEnum.SELECT,
    )
    link_fields: list[CompositeLinkFields] = Field(
        description=models_limitations["composite"]["link_fields"]["description"],
        max_length=models_limitations["composite"]["link_fields"]["max_length"],
        validation_alias=AliasChoices(
            models_limitations["composite"]["link_fields"]["validation_alias"][0],
            models_limitations["composite"]["link_fields"]["validation_alias"][1],
        ),
        serialization_alias=models_limitations["composite"]["link_fields"]["serialization_alias"],
        default=[],
    )
    database_objects: list[DatabaseObject] = Field(
        description=models_limitations["composite"]["db_objects"]["description"],
        min_length=models_limitations["composite"]["db_objects"]["min_length"],
        max_length=models_limitations["composite"]["db_objects"]["max_length"],
        serialization_alias=models_limitations["composite"]["db_objects"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["composite"]["db_objects"]["validation_alias"][0],
            models_limitations["composite"]["db_objects"]["validation_alias"][1],
            models_limitations["composite"]["db_objects"]["validation_alias"][2],
        ),
    )
    labels: list[Label] = Field(
        description=models_limitations["composite"]["labels"]["description"],
        default=[],
        max_length=models_limitations["composite"]["labels"]["max_length"],
    )
    fields: list[CompositeField] = Field(
        description=models_limitations["composite"]["fields"]["description"],
        max_length=models_limitations["composite"]["fields"]["max_length"],
        min_length=models_limitations["composite"]["fields"]["min_length"],
    )
    model_config = ConfigDict(from_attributes=True)

    @model_validator(mode="before")
    @classmethod
    def add_additional_fields_to_measure(cls, comp_obj: Any) -> Any:
        """
        Добавляет перед валидацией дополнительные поля в валидируемый объект, если это возможно.
        Поля: models_names.
        """
        if hasattr(comp_obj, "models"):
            comp_obj.models_names = [model.name for model in comp_obj.models]
            models_id_mapping = {model.id: model.name for model in comp_obj.models}
            comp_obj.models_statuses = [
                ModelStatus(
                    name=models_id_mapping[model_relation.model_id],
                    status=model_relation.status,
                    msg=model_relation.msg,
                )
                for model_relation in comp_obj.model_relations
                if models_id_mapping.get(model_relation.model_id)
            ]
        return comp_obj


class CompositeV0(Composite):
    models_statuses: list[ModelStatus] = Field(default=[], exclude=True)


class CompositeV1(Composite):
    models_names: list[str] = Field(default=[], exclude=True)


class CompositeDatasourceRequest(BaseModel):
    """Источники данных композита для запроса."""

    name: str = Field(
        description=models_limitations["composite_datasource"]["name"]["description"],
        min_length=models_limitations["composite_datasource"]["name"]["min_length"],
        max_length=models_limitations["composite_datasource"]["name"]["max_length"],
        pattern=models_limitations["composite_datasource"]["name"]["pattern"],
    )
    schema_name: Optional[str] = Field(
        description=models_limitations["composite_datasource"]["schema_name"]["description"],
        min_length=models_limitations["composite_datasource"]["schema_name"]["min_length"],
        max_length=models_limitations["composite_datasource"]["schema_name"]["max_length"],
        pattern=models_limitations["composite_datasource"]["schema_name"]["pattern"],
        serialization_alias=models_limitations["composite_datasource"]["schema_name"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["composite_datasource"]["schema_name"]["validation_alias"][0],
            models_limitations["composite_datasource"]["schema_name"]["validation_alias"][1],
        ),
        default=None,
    )
    type: CompositeFieldRefObjectEnum = Field(
        description=models_limitations["composite_datasource"]["type"]["description"],
    )


class CompositeLinkFieldsRequest(BaseModel):
    """Схема для изменения/создание порядка полей для join Composite для запроса."""

    left: DatasourceLinkRequest = Field(
        description=models_limitations["composite_link_fields_request"]["left"]["description"]
    )
    right: DatasourceLinkRequest = Field(
        description=models_limitations["composite_link_fields_request"]["right"]["description"]
    )


class CompositeEditRequest(BaseModel):
    """
    Схема для редактирования Composite
    """

    labels: Optional[list[Label]] = Field(
        description=models_limitations["composite"]["labels"]["description"],
        max_length=models_limitations["composite"]["labels"]["max_length"],
        default=None,
    )
    database_objects: Optional[list[DatabaseObject]] = Field(
        default=None,
        description=models_limitations["composite"]["db_objects"]["description"],
        min_length=models_limitations["composite"]["db_objects"]["min_length"],
        max_length=models_limitations["composite"]["db_objects"]["max_length"],
        serialization_alias=models_limitations["composite"]["db_objects"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["composite"]["db_objects"]["validation_alias"][0],
            models_limitations["composite"]["db_objects"]["validation_alias"][1],
            models_limitations["composite"]["db_objects"]["validation_alias"][2],
        ),
    )
    fields: Optional[list[CompositeFieldRequest]] = Field(
        description=models_limitations["composite"]["fields"]["description"],
        default=None,
        max_length=models_limitations["composite"]["fields"]["max_length"],
        min_length=models_limitations["composite"]["fields"]["min_length"],
    )
    datasources: Optional[list[CompositeDatasourceRequest]] = Field(
        description=models_limitations["composite"]["datasources"]["description"],
        max_length=models_limitations["composite"]["datasources"]["max_length"],
        min_length=models_limitations["composite"]["datasources"]["min_length"],
        default=None,
    )
    link_type: Optional[CompositeLinkTypeEnum] = Field(
        description=models_limitations["composite"]["link_type"]["description"],
        serialization_alias=models_limitations["composite"]["link_type"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["composite"]["link_type"]["validation_alias"][0],
            models_limitations["composite"]["link_type"]["validation_alias"][1],
        ),
        default=None,
    )
    link_fields: Optional[list[CompositeLinkFieldsRequest]] = Field(
        description=models_limitations["composite"]["link_fields"]["description"],
        max_length=models_limitations["composite"]["link_fields"]["max_length"],
        validation_alias=AliasChoices(
            models_limitations["composite"]["link_fields"]["validation_alias"][0],
            models_limitations["composite"]["link_fields"]["validation_alias"][1],
        ),
        serialization_alias=models_limitations["composite"]["link_fields"]["serialization_alias"],
        default=None,
    )


class CompositeCreateRequest(BaseModel):
    """Схема для создания Composite."""

    name: str = Field(
        description=models_limitations["composite"]["name"]["description"],
        min_length=models_limitations["composite"]["name"]["min_length"],
        max_length=models_limitations["composite"]["name"]["max_length"],
        pattern=models_limitations["composite"]["name"]["pattern"],
    )
    labels: list[Label] = Field(
        description=models_limitations["composite"]["labels"]["description"],
        default=[],
        max_length=models_limitations["composite"]["labels"]["max_length"],
    )
    database_objects: Optional[list[DatabaseObject]] = Field(
        default=None,
        description=models_limitations["composite"]["db_objects"]["description"],
        min_length=models_limitations["composite"]["db_objects"]["min_length"],
        max_length=models_limitations["composite"]["db_objects"]["max_length"],
        serialization_alias=models_limitations["composite"]["db_objects"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["composite"]["db_objects"]["validation_alias"][0],
            models_limitations["composite"]["db_objects"]["validation_alias"][1],
            models_limitations["composite"]["db_objects"]["validation_alias"][2],
        ),
    )
    fields: list[CompositeFieldRequest] = Field(
        description=models_limitations["composite"]["fields"]["description"],
        max_length=models_limitations["composite"]["fields"]["max_length"],
        min_length=models_limitations["composite"]["fields"]["min_length"],
    )
    datasources: list[CompositeDatasourceRequest] = Field(
        description=models_limitations["composite"]["datasources"]["description"],
        max_length=models_limitations["composite"]["datasources"]["max_length"],
        min_length=models_limitations["composite"]["datasources"]["min_length"],
    )
    link_type: CompositeLinkTypeEnum = Field(
        description=models_limitations["composite"]["link_type"]["description"],
        serialization_alias=models_limitations["composite"]["link_type"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["composite"]["link_type"]["validation_alias"][0],
            models_limitations["composite"]["link_type"]["validation_alias"][1],
        ),
    )
    link_fields: list[CompositeLinkFieldsRequest] = Field(
        description=models_limitations["composite"]["link_fields"]["description"],
        max_length=models_limitations["composite"]["link_fields"]["max_length"],
        validation_alias=AliasChoices(
            models_limitations["composite"]["link_fields"]["validation_alias"][0],
            models_limitations["composite"]["link_fields"]["validation_alias"][1],
        ),
        serialization_alias=models_limitations["composite"]["link_fields"]["serialization_alias"],
        default=[],
    )
