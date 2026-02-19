from enum import StrEnum
from typing import Annotated, Any, Optional

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator
from pydantic_core import core_schema

from src.config import models_limitations
from src.integration.aor.model import AorType
from src.integration.pv_dictionaries.models import PVDictionary
from src.models.ai_prompt import AIPrompt
from src.models.enum import InformationCategoryEnum
from src.models.field import BaseFieldType, BaseFieldTypeEnum, RefTypeMixin
from src.models.label import Label
from src.models.meta import ContainedEnum
from src.models.model import ModelStatus
from src.models.utils import insert_short_label_to_labels_from_ref_labels
from src.models.version import Versioned


class DimensionAttribute(RefTypeMixin):
    """Атрибуты измерения"""

    name: str = Field(
        description=models_limitations["dimension_attribute"]["name"]["description"],
        pattern=models_limitations["dimension_attribute"]["name"]["pattern"],
        min_length=models_limitations["dimension_attribute"]["name"]["min_length"],
        max_length=models_limitations["dimension_attribute"]["name"]["max_length"],
    )
    time_dependency: bool = Field(
        description=models_limitations["dimension_attribute"]["time_dependency"]["description"],
        default=False,
        serialization_alias=models_limitations["dimension_attribute"]["time_dependency"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension_attribute"]["time_dependency"]["validation_alias"][0],
            models_limitations["dimension_attribute"]["time_dependency"]["validation_alias"][1],
        ),
    )
    replaced_labels: list[Label] = Field(
        description=models_limitations["dimension_attribute"]["labels"]["description"],
        default=[],
        max_length=models_limitations["dimension_attribute"]["labels"]["max_length"],
        serialization_alias=models_limitations["dimension_attribute"]["labels"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension_attribute"]["labels"]["validation_alias"][0],
            models_limitations["dimension_attribute"]["labels"]["validation_alias"][1],
        ),
    )

    model_config = ConfigDict(from_attributes=True)

    @model_validator(mode="before")
    @classmethod
    def add_additional_fields_to_dimension_attribute(cls, dimension_attribute_obj: Any) -> Any:
        """
        Добавляет перед валидацией дополнительное поле ref_type в валидируемый объект, если это возможно, а также заполняет отсутсвующие labels.
        """
        if (
            hasattr(dimension_attribute_obj, "dimension_attribute")
            or hasattr(dimension_attribute_obj, "any_field_attribute")
            or hasattr(dimension_attribute_obj, "measure_attribute")
        ):
            if (
                dimension_attribute_obj.attribute_type == BaseFieldTypeEnum.DIMENSION
                and dimension_attribute_obj.dimension_attribute
            ):
                dimension_attribute_obj.ref_type = BaseFieldType(
                    ref_object_type=dimension_attribute_obj.attribute_type,
                    ref_object=dimension_attribute_obj.dimension_attribute.name,
                )
                replaced_labels = [Label.model_validate(label) for label in dimension_attribute_obj.labels]
                ref_object_lables = [
                    Label.model_validate(label) for label in dimension_attribute_obj.dimension_attribute.labels
                ]
                insert_short_label_to_labels_from_ref_labels(replaced_labels, ref_object_lables)
                dimension_attribute_obj.replaced_labels = replaced_labels
            elif (
                dimension_attribute_obj.attribute_type == BaseFieldTypeEnum.MEASURE
                and dimension_attribute_obj.measure_attribute
            ):
                dimension_attribute_obj.ref_type = BaseFieldType(
                    ref_object_type=dimension_attribute_obj.attribute_type,
                    ref_object=dimension_attribute_obj.measure_attribute.name,
                )
                replaced_labels = [Label.model_validate(label) for label in dimension_attribute_obj.labels]
                ref_object_lables = [
                    Label.model_validate(label) for label in dimension_attribute_obj.measure_attribute.labels
                ]
                insert_short_label_to_labels_from_ref_labels(replaced_labels, ref_object_lables)
                dimension_attribute_obj.replaced_labels = replaced_labels
            elif (
                dimension_attribute_obj.attribute_type == BaseFieldTypeEnum.ANYFIELD
                and dimension_attribute_obj.any_field_attribute
            ):
                dimension_attribute_obj.ref_type = BaseFieldType(
                    ref_object_type=dimension_attribute_obj.attribute_type,
                    ref_object=dimension_attribute_obj.any_field_attribute,
                )
                replaced_labels = [Label.model_validate(label) for label in dimension_attribute_obj.labels]
                ref_object_lables = [
                    Label.model_validate(label) for label in dimension_attribute_obj.any_field_attribute.labels
                ]
                insert_short_label_to_labels_from_ref_labels(replaced_labels, ref_object_lables)
                dimension_attribute_obj.replaced_labels = replaced_labels
            elif dimension_attribute_obj.attribute_type is not None:
                raise ValueError("There must be at least one reference to the object.")
        return dimension_attribute_obj


class DimensionAttributeRequest(RefTypeMixin):
    name: str = Field(
        description=models_limitations["dimension_attribute"]["name"]["description"],
        pattern=models_limitations["dimension_attribute"]["name"]["pattern"],
        min_length=models_limitations["dimension_attribute"]["name"]["min_length"],
        max_length=models_limitations["dimension_attribute"]["name"]["max_length"],
    )
    time_dependency: bool = Field(
        description=models_limitations["dimension_attribute"]["time_dependency"]["description"],
        default=False,
        serialization_alias=models_limitations["dimension_attribute"]["time_dependency"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension_attribute"]["time_dependency"]["validation_alias"][0],
            models_limitations["dimension_attribute"]["time_dependency"]["validation_alias"][1],
        ),
    )
    labels: list[Label] = Field(
        description=models_limitations["dimension_attribute"]["labels"]["description"],
        default=[],
        max_length=models_limitations["dimension_attribute"]["labels"]["max_length"],
    )


class TextEnum(StrEnum):
    """Виды текста для значений измерения."""

    SHORT = "SHORT"
    MEDIUM = "MEDIUM"
    LONG = "LONG"


class TechDimensionEnum(StrEnum):
    """Виды технических измерений."""

    TIMESTAMP = "timestamp"
    DELETED = "deleted"
    IS_ACTIVE_DIMENSION = "is_active"


class TextLink(BaseModel):
    """Вид текста для значений измерения"""

    text_type: TextEnum = Field(description=models_limitations["text_link"]["text_type"]["description"])
    model_config = ConfigDict(from_attributes=True)


class DimensionTypeEnum(StrEnum, metaclass=ContainedEnum):
    INTEGER = "INTEGER"
    STRING = "STRING"
    DATE = "DATE"
    TIME = "TIME"
    DATETIME = "DATETIME"
    TIMESTAMP = "TIMESTAMP"
    BOOLEAN = "BOOLEAN"
    UUID = "UUID"
    ARRAY_STRING = "ARRAY[STRING]"
    ARRAY_INTEGER = "ARRAY[INTEGER]"


DIMENSION_WITH_REQUIRED_PRECISION = {
    DimensionTypeEnum.STRING,
    DimensionTypeEnum.INTEGER,
    DimensionTypeEnum.ARRAY_STRING,
}


def _raise_if_needed_precision_absent(dimension_type: DimensionTypeEnum | str | None, precision: int | None) -> None:
    if dimension_type in DIMENSION_WITH_REQUIRED_PRECISION and not precision:
        raise ValueError(f"Precision is required for dimension with these types: {DIMENSION_WITH_REQUIRED_PRECISION}")


class Dimension(Versioned, BaseModel):
    """
    Модель Pydantic для описания измерения (также известного как аналитика, характеристика, справочник).
    Она предназначена для валидации измерения, полученного из базы данных или из redis.
    """

    name: str = Field(
        description=models_limitations["object_name_32"]["description"],
        min_length=models_limitations["object_name_32"]["min_length"],
        max_length=models_limitations["object_name_32"]["max_length"],
        pattern=models_limitations["object_name_32"]["pattern"],
    )
    information_category: InformationCategoryEnum = Field(
        description=models_limitations["information_category"]["description"],
        default=InformationCategoryEnum.K3,
        serialization_alias=models_limitations["information_category"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["information_category"]["validation_alias"][0],
            models_limitations["information_category"]["validation_alias"][1],
        ),
    )
    aor_type: AorType = Field(
        default=AorType.DIMENSION,
        serialization_alias=models_limitations["aor_type"]["serialization_alias"],
        validation_alias=AliasChoices(*models_limitations["aor_type"]["validation_alias"]),
    )
    precision: Optional[int] = Field(
        description=models_limitations["base_field"]["precision"]["description"],
        ge=models_limitations["base_field"]["precision"]["ge"],
        le=models_limitations["base_field"]["precision"]["le"],
        default=None,
        validate_default=False,
    )
    labels: list[Label] = Field(
        description=models_limitations["base_field"]["labels"]["description"],
        default=[],
        max_length=models_limitations["base_field"]["labels"]["max_length"],
    )
    type: DimensionTypeEnum = Field(
        description=models_limitations["dimension"]["type"]["description"],
        max_length=models_limitations["dimension"]["type"]["max_length"],
    )
    auth_relevant: bool = Field(
        description=models_limitations["dimension"]["auth_relevant"]["description"],
        default=False,
        serialization_alias=models_limitations["dimension"]["auth_relevant"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["auth_relevant"]["validation_alias"][0],
            models_limitations["dimension"]["auth_relevant"]["validation_alias"][1],
        ),
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
        description=models_limitations["dimension"]["models_names"]["description"],
        min_length=models_limitations["dimension"]["models_names"]["min_length"],
        max_length=models_limitations["dimension"]["models_names"]["max_length"],
        serialization_alias=models_limitations["dimension"]["models_names"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["models_names"]["validation_alias"][0],
            models_limitations["dimension"]["models_names"]["validation_alias"][1],
        ),
    )
    models_statuses: list[ModelStatus] = Field(
        description=models_limitations["dimension"]["models_statuses"]["description"],
        min_length=models_limitations["dimension"]["models_statuses"]["min_length"],
        max_length=models_limitations["dimension"]["models_statuses"]["max_length"],
        serialization_alias=models_limitations["dimension"]["models_statuses"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["models_statuses"]["validation_alias"][0],
            models_limitations["dimension"]["models_statuses"]["validation_alias"][1],
        ),
    )
    business_key_representation: Optional[str] = Field(
        description=models_limitations["dimension"]["business_key_representation"]["description"],
        min_length=models_limitations["dimension"]["business_key_representation"]["min_length"],
        max_length=models_limitations["dimension"]["business_key_representation"]["max_length"],
        pattern=models_limitations["dimension"]["business_key_representation"]["pattern"],
        default=None,
        serialization_alias=models_limitations["dimension"]["business_key_representation"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["business_key_representation"]["validation_alias"][0],
            models_limitations["dimension"]["business_key_representation"]["validation_alias"][1],
        ),
    )
    is_virtual: bool = Field(
        default=False,
        description=models_limitations["dimension"]["is_virtual"]["description"],
        serialization_alias=models_limitations["dimension"]["is_virtual"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["is_virtual"]["validation_alias"][0],
            models_limitations["dimension"]["is_virtual"]["validation_alias"][1],
        ),
    )
    data_access_method: Optional[str] = Field(
        description=models_limitations["dimension"]["data_access_method"]["description"],
        pattern=models_limitations["dimension"]["data_access_method"]["pattern"],
        default=None,
        min_length=models_limitations["dimension"]["data_access_method"]["min_length"],
        max_length=models_limitations["dimension"]["data_access_method"]["max_length"],
        serialization_alias=models_limitations["dimension"]["data_access_method"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["data_access_method"]["validation_alias"][0],
            models_limitations["dimension"]["data_access_method"]["validation_alias"][1],
        ),
    )
    texts_time_dependency: bool = Field(
        description=models_limitations["dimension"]["texts_time_dependency"]["description"],
        default=False,
        serialization_alias=models_limitations["dimension"]["texts_time_dependency"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["texts_time_dependency"]["validation_alias"][0],
            models_limitations["dimension"]["texts_time_dependency"]["validation_alias"][1],
        ),
    )
    attributes_time_dependency: bool = Field(
        description=models_limitations["dimension"]["attributes_time_dependency"]["description"],
        default=False,
        serialization_alias=models_limitations["dimension"]["attributes_time_dependency"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["attributes_time_dependency"]["validation_alias"][0],
            models_limitations["dimension"]["attributes_time_dependency"]["validation_alias"][1],
        ),
    )
    texts_language_dependency: bool = Field(
        description=models_limitations["dimension"]["texts_language_dependency"]["description"],
        default=False,
        serialization_alias=models_limitations["dimension"]["texts_language_dependency"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["texts_language_dependency"]["validation_alias"][0],
            models_limitations["dimension"]["texts_language_dependency"]["validation_alias"][1],
        ),
    )
    dimension_name: Optional[str] = Field(
        description=models_limitations["dimension"]["dimension_id"]["description"],
        min_length=models_limitations["object_name_32"]["min_length"],
        max_length=models_limitations["object_name_32"]["max_length"],
        pattern=models_limitations["object_name_32"]["pattern"],
        default=None,
        serialization_alias=models_limitations["dimension"]["dimension_id"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["dimension_id"]["validation_alias"][0],
            models_limitations["dimension"]["dimension_id"]["validation_alias"][1],
        ),
    )
    case_sensitive: bool = Field(
        description=models_limitations["dimension"]["case_sensitive"]["description"],
        default=False,
        serialization_alias=models_limitations["dimension"]["case_sensitive"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["case_sensitive"]["validation_alias"][0],
            models_limitations["dimension"]["case_sensitive"]["validation_alias"][1],
        ),
    )
    text_table_name: Optional[str] = Field(
        description=models_limitations["object_name_64"]["description"],
        min_length=models_limitations["object_name_64"]["min_length"],
        max_length=models_limitations["object_name_64"]["max_length"],
        pattern=models_limitations["object_name_64"]["pattern"],
        default=None,
        serialization_alias=models_limitations["dimension"]["text_table_id"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["text_table_id"]["validation_alias"][0],
            models_limitations["dimension"]["text_table_id"]["validation_alias"][1],
        ),
    )
    attributes_table_name: Optional[str] = Field(
        description=models_limitations["object_name_64"]["description"],
        min_length=models_limitations["object_name_64"]["min_length"],
        max_length=models_limitations["object_name_64"]["max_length"],
        pattern=models_limitations["object_name_64"]["pattern"],
        default=None,
        serialization_alias=models_limitations["dimension"]["attributes_table_id"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["attributes_table_id"]["validation_alias"][0],
            models_limitations["dimension"]["attributes_table_id"]["validation_alias"][1],
        ),
    )
    values_table_name: Optional[str] = Field(
        description=models_limitations["object_name_64"]["description"],
        min_length=models_limitations["object_name_64"]["min_length"],
        max_length=models_limitations["object_name_64"]["max_length"],
        pattern=models_limitations["object_name_64"]["pattern"],
        serialization_alias=models_limitations["dimension"]["values_table_id"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["values_table_id"]["validation_alias"][0],
            models_limitations["dimension"]["values_table_id"]["validation_alias"][1],
        ),
        default=None,
    )
    linked_texts: list[TextEnum] = Field(
        description=models_limitations["dimension"]["texts"]["description"],
        default=[],
        serialization_alias=models_limitations["dimension"]["texts"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["texts"]["validation_alias"][0],
            models_limitations["dimension"]["texts"]["validation_alias"][1],
        ),
        max_length=models_limitations["dimension"]["texts"]["max_length"],
    )
    pv_dictionary: Optional[PVDictionary] = Field(
        description=models_limitations["dimension"]["pv_dictionary"]["description"],
        default=None,
        serialization_alias=models_limitations["dimension"]["pv_dictionary"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["pv_dictionary"]["validation_alias"][0],
            models_limitations["dimension"]["pv_dictionary"]["validation_alias"][1],
        ),
    )
    prompt: Optional[AIPrompt] = Field(
        default=None,
        description=models_limitations["dimension"]["prompt"]["description"],
        serialization_alias=models_limitations["dimension"]["prompt"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["prompt"]["validation_alias"][0],
            models_limitations["dimension"]["prompt"]["validation_alias"][1],
            models_limitations["dimension"]["prompt"]["validation_alias"][2],
        ),
    )
    have_hierarchy: bool = Field(
        default=False,
        description=models_limitations["dimension"]["have_hierarchy"]["description"],
        serialization_alias=models_limitations["dimension"]["have_hierarchy"]["serialization_alias"],
        validation_alias=AliasChoices(
            *models_limitations["dimension"]["have_hierarchy"]["validation_alias"],
        ),
    )
    hierarchy_versions_table: Optional[str] = Field(
        default=None,
        description=models_limitations["dimension"]["hierarchy_versions_table"]["description"],
        serialization_alias=models_limitations["dimension"]["hierarchy_versions_table"]["serialization_alias"],
        validation_alias=AliasChoices(
            *models_limitations["dimension"]["hierarchy_versions_table"]["validation_alias"],
        ),
        min_length=models_limitations["object_name_64"]["min_length"],
        max_length=models_limitations["object_name_64"]["max_length"],
        pattern=models_limitations["object_name_64"]["pattern"],
    )
    hierarchy_text_versions_table: Optional[str] = Field(
        default=None,
        description=models_limitations["dimension"]["hierarchy_text_versions_table"]["description"],
        serialization_alias=models_limitations["dimension"]["hierarchy_text_versions_table"]["serialization_alias"],
        validation_alias=AliasChoices(
            *models_limitations["dimension"]["hierarchy_text_versions_table"]["validation_alias"],
        ),
        min_length=models_limitations["object_name_64"]["min_length"],
        max_length=models_limitations["object_name_64"]["max_length"],
        pattern=models_limitations["object_name_64"]["pattern"],
    )
    hierarchy_nodes_table: Optional[str] = Field(
        default=None,
        description=models_limitations["dimension"]["hierarchy_nodes_table"]["description"],
        serialization_alias=models_limitations["dimension"]["hierarchy_nodes_table"]["serialization_alias"],
        validation_alias=AliasChoices(
            *models_limitations["dimension"]["hierarchy_nodes_table"]["validation_alias"],
        ),
        min_length=models_limitations["object_name_64"]["min_length"],
        max_length=models_limitations["object_name_64"]["max_length"],
        pattern=models_limitations["object_name_64"]["pattern"],
    )
    hierarchy_text_nodes_table: Optional[str] = Field(
        default=None,
        description=models_limitations["dimension"]["hierarchy_text_nodes_table"]["description"],
        serialization_alias=models_limitations["dimension"]["hierarchy_text_nodes_table"]["serialization_alias"],
        validation_alias=AliasChoices(
            *models_limitations["dimension"]["hierarchy_text_nodes_table"]["validation_alias"],
        ),
        min_length=models_limitations["object_name_64"]["min_length"],
        max_length=models_limitations["object_name_64"]["max_length"],
        pattern=models_limitations["object_name_64"]["pattern"],
    )

    model_config = ConfigDict(from_attributes=True)
    attributes: list[DimensionAttribute] = Field(
        description=models_limitations["dimension"]["attributes"]["description"],
        default=[],
        max_length=models_limitations["dimension"]["attributes"]["max_length"],
        serialization_alias=models_limitations["dimension"]["attributes"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["attributes"]["validation_alias"][0],
            models_limitations["dimension"]["attributes"]["validation_alias"][1],
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def add_additional_fields_to_dimension(cls, dimension_obj: Any) -> Any:
        """
        Добавляет перед валидацией дополнительные поля в валидируемый объект, если это возможно.
        Поля: linked_texts, dimension_name, text_table_name, attributes_table_name, values_table_name.
        """
        if hasattr(dimension_obj, "models"):
            dimension_obj.models_names = [model.name for model in dimension_obj.models]
            models_id_mapping = {model.id: model.name for model in dimension_obj.models}
            dimension_obj.models_statuses = [
                ModelStatus(
                    name=models_id_mapping[model_relation.model_id],
                    status=model_relation.status,
                    msg=model_relation.msg,
                )
                for model_relation in dimension_obj.model_relations
                if models_id_mapping.get(model_relation.model_id)
            ]
            models_id_mapping = {model.id: model.name for model in dimension_obj.models}
            dimension_obj.models_statuses = [
                ModelStatus(
                    name=models_id_mapping[model_relation.model_id],
                    status=model_relation.status,
                    msg=model_relation.msg,
                )
                for model_relation in dimension_obj.model_relations
                if models_id_mapping.get(model_relation.model_id)
            ]
        if hasattr(dimension_obj, "texts"):
            dimension_obj.linked_texts = [txt.text_type for txt in dimension_obj.texts]
        if hasattr(dimension_obj, "dimension"):
            dimension_obj.dimension_name = dimension_obj.dimension.name if dimension_obj.dimension is not None else None
        if hasattr(dimension_obj, "text_table"):
            dimension_obj.text_table_name = (
                dimension_obj.text_table.name if dimension_obj.text_table is not None else None
            )
        if hasattr(dimension_obj, "attributes_table"):
            dimension_obj.attributes_table_name = (
                dimension_obj.attributes_table.name if dimension_obj.attributes_table is not None else None
            )
        if hasattr(dimension_obj, "values_table"):
            dimension_obj.values_table_name = (
                dimension_obj.values_table.name if dimension_obj.values_table is not None else None
            )
        if hasattr(dimension_obj, "hierarchies") and dimension_obj.hierarchies:
            dimension_obj.have_hierarchy = True
            dimension_obj.hierarchy_versions_table = dimension_obj.hierarchies[0].data_storage_versions
            dimension_obj.hierarchy_text_versions_table = dimension_obj.hierarchies[0].data_storage_text_versions
            dimension_obj.hierarchy_nodes_table = dimension_obj.hierarchies[0].data_storage_nodes
            dimension_obj.hierarchy_text_nodes_table = dimension_obj.hierarchies[0].data_storage_text_nodes
        return dimension_obj

    @model_validator(mode="after")
    def auto_update_from_context(self, info: ValidationInfo) -> Any:
        """
        Автоматически заполняет поля объекта контекстными данными из ValidationInfo.
        Автоматически заполняет поля объекта контекстными данными из ValidationInfo.

        Args:
            info (ValidationInfo): Контекстная информация, получаемая при валидации модели.
            info (ValidationInfo): Контекстная информация, получаемая при валидации модели.

        Returns:
            Any: Сам объект после автоматического обновления полей.
            Any: Сам объект после автоматического обновления полей.
        """
        if not info.context or not self.dimension_name:
            return self
        self.linked_texts = [TextEnum(text) for text in info.context.get("ref_texts")]
        self.attributes = [
            DimensionAttribute.model_validate(attribute) for attribute in info.context.get("ref_attributes", [])
        ]
        self.pv_dictionary = (
            PVDictionary.model_validate(info.context.get("ref_pv_dictionary"))
            if info.context.get("ref_pv_dictionary")
            else None
        )
        self.values_table_name = info.context.get("ref_values_table_name")
        self.attributes_table_name = info.context.get("ref_attributes_table_name")
        self.text_table_name = info.context.get("ref_text_table_name")
        self.case_sensitive = info.context.get("ref_case_sensitive")

        self.texts_language_dependency = info.context.get("ref_texts_language_dependency")
        self.texts_time_dependency = info.context.get("ref_texts_time_dependency")
        self.attributes_time_dependency = info.context.get("ref_attributes_time_dependency")
        self.data_access_method = info.context.get("ref_data_access_method")
        self.is_virtual = info.context.get("ref_is_virtual")
        self.business_key_representation = info.context.get("ref_business_key_representation")
        self.auth_relevant = info.context.get("ref_auth_relevant")
        self.precision = info.context.get("ref_precision")

        return self


class DimensionV0(Dimension):
    models_statuses: list[ModelStatus] = Field(default=[], exclude=True)


class DimensionV1(Dimension):
    models_names: list[str] = Field(default=[], exclude=True)


class DimensionCreateRequest(BaseModel):
    """Измерение для запроса на создание."""

    name: str = Field(
        description=models_limitations["object_name_32"]["description"],
        min_length=models_limitations["object_name_32"]["min_length"],
        max_length=models_limitations["object_name_32"]["max_length"],
        pattern=models_limitations["object_name_32"]["pattern"],
    )
    information_category: InformationCategoryEnum = Field(
        description=models_limitations["information_category"]["description"],
        default=InformationCategoryEnum.K3,
        serialization_alias=models_limitations["information_category"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["information_category"]["validation_alias"][0],
            models_limitations["information_category"]["validation_alias"][1],
        ),
    )
    labels: list[Label] = Field(
        description=models_limitations["base_field"]["labels"]["description"],
        default=[],
        max_length=models_limitations["base_field"]["labels"]["max_length"],
    )
    dimension_id: Optional[str] = Field(
        description=models_limitations["dimension"]["dimension_id"]["description"],
        min_length=models_limitations["object_name_32"]["min_length"],
        max_length=models_limitations["object_name_32"]["max_length"],
        pattern=models_limitations["object_name_32"]["pattern"],
        default=None,
        serialization_alias=models_limitations["dimension"]["dimension_id"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["dimension_id"]["validation_alias"][0],
            models_limitations["dimension"]["dimension_id"]["validation_alias"][1],
            models_limitations["dimension"]["dimension_id"]["validation_alias"][2],
        ),
    )
    precision: Optional[int] = Field(
        description=models_limitations["base_field"]["precision"]["description"],
        ge=models_limitations["base_field"]["precision"]["ge"],
        le=models_limitations["base_field"]["precision"]["le"],
        validate_default=False,
        default=None,
    )
    type: Optional[DimensionTypeEnum] = Field(
        description=models_limitations["dimension"]["type"]["description"],
        max_length=models_limitations["dimension"]["type"]["max_length"],
        default=None,
        validate_default=True,
    )
    auth_relevant: bool = Field(
        description=models_limitations["dimension"]["auth_relevant"]["description"],
        default=False,
        serialization_alias=models_limitations["dimension"]["auth_relevant"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["auth_relevant"]["validation_alias"][0],
            models_limitations["dimension"]["auth_relevant"]["validation_alias"][1],
        ),
    )
    is_virtual: bool = Field(
        default=False,
        description=models_limitations["dimension"]["is_virtual"]["description"],
        serialization_alias=models_limitations["dimension"]["is_virtual"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["is_virtual"]["validation_alias"][0],
            models_limitations["dimension"]["is_virtual"]["validation_alias"][1],
        ),
    )
    business_key_representation: Optional[str] = Field(
        description=models_limitations["dimension"]["business_key_representation"]["description"],
        min_length=models_limitations["dimension"]["business_key_representation"]["min_length"],
        max_length=models_limitations["dimension"]["business_key_representation"]["max_length"],
        pattern=models_limitations["dimension"]["business_key_representation"]["pattern"],
        default=None,
        serialization_alias=models_limitations["dimension"]["business_key_representation"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["business_key_representation"]["validation_alias"][0],
            models_limitations["dimension"]["business_key_representation"]["validation_alias"][1],
        ),
    )
    data_access_method: Optional[str] = Field(
        description=models_limitations["dimension"]["data_access_method"]["description"],
        pattern=models_limitations["dimension"]["data_access_method"]["pattern"],
        min_length=models_limitations["dimension"]["data_access_method"]["min_length"],
        max_length=models_limitations["dimension"]["data_access_method"]["max_length"],
        default=None,
        serialization_alias=models_limitations["dimension"]["data_access_method"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["data_access_method"]["validation_alias"][0],
            models_limitations["dimension"]["data_access_method"]["validation_alias"][1],
        ),
    )
    texts_time_dependency: bool = Field(
        description=models_limitations["dimension"]["texts_time_dependency"]["description"],
        default=False,
        serialization_alias=models_limitations["dimension"]["texts_time_dependency"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["texts_time_dependency"]["validation_alias"][0],
            models_limitations["dimension"]["texts_time_dependency"]["validation_alias"][1],
        ),
    )
    attributes_time_dependency: bool = Field(
        description=models_limitations["dimension"]["attributes_time_dependency"]["description"],
        default=False,
        serialization_alias=models_limitations["dimension"]["attributes_time_dependency"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["attributes_time_dependency"]["validation_alias"][0],
            models_limitations["dimension"]["attributes_time_dependency"]["validation_alias"][1],
        ),
    )
    texts_language_dependency: bool = Field(
        description=models_limitations["dimension"]["texts_language_dependency"]["description"],
        default=False,
        serialization_alias=models_limitations["dimension"]["texts_language_dependency"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["texts_language_dependency"]["validation_alias"][0],
            models_limitations["dimension"]["texts_language_dependency"]["validation_alias"][1],
        ),
    )
    case_sensitive: bool = Field(
        description=models_limitations["dimension"]["case_sensitive"]["description"],
        default=False,
        serialization_alias=models_limitations["dimension"]["case_sensitive"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["case_sensitive"]["validation_alias"][0],
            models_limitations["dimension"]["case_sensitive"]["validation_alias"][1],
        ),
    )
    text_table_id: Optional[str] = Field(
        description=models_limitations["object_name_64"]["description"],
        min_length=models_limitations["object_name_64"]["min_length"],
        max_length=models_limitations["object_name_64"]["max_length"],
        pattern=models_limitations["object_name_64"]["pattern"],
        default=None,
        serialization_alias=models_limitations["dimension"]["text_table_id"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["text_table_id"]["validation_alias"][0],
            models_limitations["dimension"]["text_table_id"]["validation_alias"][1],
            models_limitations["dimension"]["text_table_id"]["validation_alias"][2],
        ),
    )
    values_table_id: Optional[str] = Field(
        description=models_limitations["object_name_64"]["description"],
        min_length=models_limitations["object_name_64"]["min_length"],
        max_length=models_limitations["object_name_64"]["max_length"],
        pattern=models_limitations["object_name_64"]["pattern"],
        serialization_alias=models_limitations["dimension"]["values_table_id"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["values_table_id"]["validation_alias"][0],
            models_limitations["dimension"]["values_table_id"]["validation_alias"][1],
            models_limitations["dimension"]["values_table_id"]["validation_alias"][2],
        ),
        default=None,
    )
    attributes_table_id: Optional[str] = Field(
        description=models_limitations["object_name_64"]["description"],
        min_length=models_limitations["object_name_64"]["min_length"],
        max_length=models_limitations["object_name_64"]["max_length"],
        pattern=models_limitations["object_name_64"]["pattern"],
        default=None,
        serialization_alias=models_limitations["dimension"]["attributes_table_id"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["attributes_table_id"]["validation_alias"][0],
            models_limitations["dimension"]["attributes_table_id"]["validation_alias"][1],
            models_limitations["dimension"]["attributes_table_id"]["validation_alias"][2],
        ),
    )
    texts: list[TextEnum] = Field(
        description=models_limitations["dimension"]["texts"]["description"],
        default=[],
        max_length=models_limitations["dimension"]["texts"]["max_length"],
        serialization_alias=models_limitations["dimension"]["texts"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["texts"]["validation_alias"][0],
            models_limitations["dimension"]["texts"]["validation_alias"][1],
            models_limitations["dimension"]["texts"]["validation_alias"][2],
        ),
    )
    attributes: list[DimensionAttributeRequest] = Field(
        description=models_limitations["dimension"]["attributes"]["description"],
        default=[],
        max_length=models_limitations["dimension"]["attributes"]["max_length"],
        serialization_alias=models_limitations["dimension"]["attributes"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["attributes"]["validation_alias"][0],
            models_limitations["dimension"]["attributes"]["validation_alias"][1],
        ),
    )
    pv_dictionary: Optional[PVDictionary] = Field(
        description=models_limitations["dimension"]["pv_dictionary"]["description"],
        default=None,
        serialization_alias=models_limitations["dimension"]["pv_dictionary"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["pv_dictionary"]["validation_alias"][0],
            models_limitations["dimension"]["pv_dictionary"]["validation_alias"][1],
        ),
    )
    prompt: Optional[AIPrompt] = Field(
        default=None,
        description=models_limitations["dimension"]["prompt"]["description"],
        serialization_alias=models_limitations["dimension"]["prompt"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["prompt"]["validation_alias"][0],
            models_limitations["dimension"]["prompt"]["validation_alias"][1],
            models_limitations["dimension"]["prompt"]["validation_alias"][2],
        ),
    )

    @field_validator("type")
    @classmethod
    def type_validator(
        cls, type_field: Optional[DimensionTypeEnum], other_fields: core_schema.ValidationInfo, **kwargs: dict
    ) -> Optional[DimensionTypeEnum]:
        if type_field is None and other_fields.data.get("dimension_id") is None:
            raise ValueError("The type cannot be null for non-reference dimensions")
        return type_field

    @model_validator(mode="after")
    def validate_precision_obligation_validation_for_type(self) -> Any:
        _raise_if_needed_precision_absent(self.type, self.precision)
        return self


class DimensionEditRequest(BaseModel):
    """
    Модель Pydantic для описания измерения (также известного как аналитика, характеристика, справочник).
    Она предназначена для валидации и преобразования входных данных для создания или редактирования измерений.
    """

    information_category: Optional[InformationCategoryEnum] = Field(
        description=models_limitations["information_category"]["description"],
        default=None,
        serialization_alias=models_limitations["information_category"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["information_category"]["validation_alias"][0],
            models_limitations["information_category"]["validation_alias"][1],
        ),
    )
    labels: Optional[list[Label]] = Field(
        description=models_limitations["base_field"]["labels"]["description"],
        default=None,
        max_length=100,
    )
    precision: Optional[int] = Field(
        description=models_limitations["base_field"]["precision"]["description"],
        ge=models_limitations["base_field"]["precision"]["ge"],
        le=models_limitations["base_field"]["precision"]["le"],
        default=None,
        validate_default=False,
    )
    type: Optional[DimensionTypeEnum] = Field(
        description=models_limitations["dimension"]["type"]["description"],
        default=None,
        max_length=models_limitations["dimension"]["type"]["max_length"],
    )
    is_virtual: Optional[bool] = Field(
        default=None,
        description=models_limitations["dimension"]["is_virtual"]["description"],
        serialization_alias=models_limitations["dimension"]["is_virtual"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["is_virtual"]["validation_alias"][0],
            models_limitations["dimension"]["is_virtual"]["validation_alias"][1],
        ),
    )
    auth_relevant: Optional[bool] = Field(
        description=models_limitations["dimension"]["auth_relevant"]["description"],
        default=None,
        serialization_alias=models_limitations["dimension"]["auth_relevant"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["auth_relevant"]["validation_alias"][0],
            models_limitations["dimension"]["auth_relevant"]["validation_alias"][1],
        ),
    )
    business_key_representation: Optional[str] = Field(
        description=models_limitations["dimension"]["business_key_representation"]["description"],
        min_length=models_limitations["dimension"]["business_key_representation"]["min_length"],
        max_length=models_limitations["dimension"]["business_key_representation"]["max_length"],
        pattern=models_limitations["dimension"]["business_key_representation"]["pattern"],
        default=None,
        serialization_alias=models_limitations["dimension"]["business_key_representation"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["business_key_representation"]["validation_alias"][0],
            models_limitations["dimension"]["business_key_representation"]["validation_alias"][1],
        ),
    )
    data_access_method: Optional[str] = Field(
        description=models_limitations["dimension"]["data_access_method"]["description"],
        pattern=models_limitations["dimension"]["data_access_method"]["pattern"],
        default=None,
        min_length=models_limitations["dimension"]["data_access_method"]["min_length"],
        max_length=models_limitations["dimension"]["data_access_method"]["max_length"],
        serialization_alias=models_limitations["dimension"]["data_access_method"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["data_access_method"]["validation_alias"][0],
            models_limitations["dimension"]["data_access_method"]["validation_alias"][1],
        ),
    )
    texts_time_dependency: Optional[bool] = Field(
        description=models_limitations["dimension"]["texts_time_dependency"]["description"],
        default=None,
        serialization_alias=models_limitations["dimension"]["texts_time_dependency"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["texts_time_dependency"]["validation_alias"][0],
            models_limitations["dimension"]["texts_time_dependency"]["validation_alias"][1],
        ),
    )
    texts_language_dependency: Optional[bool] = Field(
        description=models_limitations["dimension"]["texts_language_dependency"]["description"],
        default=None,
        serialization_alias=models_limitations["dimension"]["texts_language_dependency"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["texts_language_dependency"]["validation_alias"][0],
            models_limitations["dimension"]["texts_language_dependency"]["validation_alias"][1],
        ),
    )
    dimension_id: Optional[str] = Field(
        description=models_limitations["dimension"]["dimension_id"]["description"],
        min_length=models_limitations["object_name_32"]["min_length"],
        max_length=models_limitations["object_name_32"]["max_length"],
        pattern=models_limitations["object_name_32"]["pattern"],
        default=None,
        serialization_alias=models_limitations["dimension"]["dimension_id"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["dimension_id"]["validation_alias"][0],
            models_limitations["dimension"]["dimension_id"]["validation_alias"][1],
            models_limitations["dimension"]["dimension_id"]["validation_alias"][2],
        ),
    )
    case_sensitive: Optional[bool] = Field(
        description=models_limitations["dimension"]["case_sensitive"]["description"],
        default=None,
        serialization_alias=models_limitations["dimension"]["case_sensitive"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["case_sensitive"]["validation_alias"][0],
            models_limitations["dimension"]["case_sensitive"]["validation_alias"][1],
        ),
    )
    texts: Optional[list[TextEnum]] = Field(
        description=models_limitations["dimension"]["texts"]["description"],
        default=None,
        serialization_alias=models_limitations["dimension"]["texts"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["texts"]["validation_alias"][0],
            models_limitations["dimension"]["texts"]["validation_alias"][1],
            models_limitations["dimension"]["texts"]["validation_alias"][2],
        ),
        max_length=models_limitations["dimension"]["texts"]["max_length"],
    )
    attributes: Optional[list[DimensionAttributeRequest]] = Field(
        description=models_limitations["dimension"]["attributes"]["description"],
        default=None,
        max_length=models_limitations["dimension"]["attributes"]["max_length"],
        serialization_alias=models_limitations["dimension"]["attributes"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["attributes"]["validation_alias"][0],
            models_limitations["dimension"]["attributes"]["validation_alias"][1],
        ),
    )
    prompt: Optional[AIPrompt] = Field(
        default=None,
        description=models_limitations["dimension"]["prompt"]["description"],
        serialization_alias=models_limitations["dimension"]["prompt"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension"]["prompt"]["validation_alias"][0],
            models_limitations["dimension"]["prompt"]["validation_alias"][1],
            models_limitations["dimension"]["prompt"]["validation_alias"][2],
        ),
    )

    @model_validator(mode="after")
    def validate_precision_obligation_validation_for_type(self) -> Any:
        if self.type is None and self.precision is None:
            return self
        if hasattr(self, "type"):
            _raise_if_needed_precision_absent(self.type, self.precision)
        if hasattr(self, "precision") and self.precision is None and not self.type:
            raise ValueError("Precision cannot be changed without specifying the type")

        return self


class ChangeDictionaryStuctureActionsEnum(StrEnum):
    """Типы действий с измерениями."""

    UPDATE = "update"
    CREATE = "create"
    DELETE = "delete"


class DimensionTextFieldEnum(StrEnum):
    SHORT_TEXT = "txtshort"
    MEDIUM_TEXT = "txtmedium"
    LONG_TEXT = "txtlong"


class DefaultDimensionEnum(StrEnum):
    CALENDAR_DAY = "calendar_day"
    DATEFROM = "datefrom"
    DATETO = "dateto"
    LANGUAGE_TAG = "language_tag"
    HIERARCHY_TEXTNODES = "hierarchy_textnodes"
