"""Базовый класс для полей DSO и Composite"""

from typing import Any, Optional

from pydantic import AliasChoices, ConfigDict, Field, field_validator, model_validator
from pydantic_core import core_schema

from src.config import models_limitations
from src.models.any_field import AnyField
from src.models.consts import DATA_TYPES
from src.models.database import DatabaseTypeEnum
from src.models.dimension import DimensionTypeEnum
from src.models.field import BaseFieldType, BaseFieldTypeEnum, RefTypeMixin
from src.models.label import Label
from src.models.measure import MeasureTypeEnum
from src.models.utils import insert_short_label_to_labels_from_ref_labels


class ObjectField(RefTypeMixin):
    """Поля DSO и Composite для чтения из базы данных или кэша"""

    name: str = Field(
        description=models_limitations["object_field"]["name"]["description"],
        pattern=models_limitations["object_field"]["name"]["pattern"],
        min_length=models_limitations["object_field"]["name"]["min_length"],
        max_length=models_limitations["object_field"]["name"]["max_length"],
    )
    labels: list[Label] = Field(
        description=models_limitations["object_field"]["labels"]["description"],
        max_length=models_limitations["object_field"]["labels"]["max_length"],
        default=[],
        serialization_alias=models_limitations["object_field"]["labels"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["object_field"]["labels"]["validation_alias"][0],
            models_limitations["object_field"]["labels"]["validation_alias"][1],
        ),
    )
    any_field: Optional["AnyField"] = Field(exclude=True, default=None)
    model_config = ConfigDict(from_attributes=True)
    sql_name: Optional["str"] = Field(
        description=models_limitations["object_field"]["sql_name"]["description"],
        serialization_alias=models_limitations["object_field"]["sql_name"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["object_field"]["sql_name"]["validation_alias"][0],
            models_limitations["object_field"]["sql_name"]["validation_alias"][1],
        ),
        pattern=models_limitations["object_field"]["sql_name"]["pattern"],
        min_length=models_limitations["object_field"]["sql_name"]["min_length"],
        max_length=models_limitations["object_field"]["sql_name"]["max_length"],
        default=None,
    )
    field_data_type: Optional[str] = Field(default=None, exclude=True)
    precision: Optional[int] = Field(default=None, exclude=True)
    scale: Optional[int] = Field(default=None, exclude=True)
    sql_column_type: Optional[str] = Field(
        default=None,
        validate_default=True,
        description=models_limitations["object_field"]["sql_column_type"]["description"],
        serialization_alias=models_limitations["object_field"]["sql_column_type"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["object_field"]["sql_column_type"]["validation_alias"][0],
            models_limitations["object_field"]["sql_column_type"]["validation_alias"][1],
        ),
        pattern=models_limitations["object_field"]["sql_column_type"]["pattern"],
        min_length=models_limitations["object_field"]["sql_column_type"]["min_length"],
        max_length=models_limitations["object_field"]["sql_column_type"]["max_length"],
    )

    @model_validator(mode="before")
    @classmethod
    def add_additional_field_to_object_field(cls, object_field_obj: Any) -> Any:
        """
        Добавляет перед валидацией дополнительное поле ref_type в валидируемый объект, если это возможно, а также заполняет отсутсвующие labels.
        """
        if (
            hasattr(object_field_obj, "dimension")
            or hasattr(object_field_obj, "measure")
            or hasattr(object_field_obj, "any_field")
        ):
            if object_field_obj.field_type == BaseFieldTypeEnum.DIMENSION and object_field_obj.dimension:
                object_field_obj.ref_type = BaseFieldType(
                    ref_object_type=object_field_obj.field_type,
                    ref_object=object_field_obj.dimension.name,
                )
                replaced_labels = [Label.model_validate(label) for label in object_field_obj.labels]
                ref_object_lables = [Label.model_validate(label) for label in object_field_obj.dimension.labels]
                insert_short_label_to_labels_from_ref_labels(replaced_labels, ref_object_lables)
                object_field_obj.replaced_labels = replaced_labels
                object_field_obj.field_data_type = object_field_obj.dimension.type
                object_field_obj.precision = object_field_obj.dimension.precision
            elif object_field_obj.field_type == BaseFieldTypeEnum.MEASURE and object_field_obj.measure:
                object_field_obj.ref_type = BaseFieldType(
                    ref_object_type=object_field_obj.field_type,
                    ref_object=object_field_obj.measure.name,
                )
                replaced_labels = [Label.model_validate(label) for label in object_field_obj.labels]
                ref_object_lables = [Label.model_validate(label) for label in object_field_obj.measure.labels]
                insert_short_label_to_labels_from_ref_labels(replaced_labels, ref_object_lables)
                object_field_obj.replaced_labels = replaced_labels
                object_field_obj.field_data_type = object_field_obj.measure.type
                object_field_obj.precision = object_field_obj.measure.precision
                object_field_obj.scale = object_field_obj.measure.scale
            elif object_field_obj.field_type == BaseFieldTypeEnum.ANYFIELD and object_field_obj.any_field:
                object_field_obj.ref_type = BaseFieldType(
                    ref_object_type=object_field_obj.field_type,
                    ref_object=object_field_obj.any_field,
                )
                replaced_labels = [Label.model_validate(label) for label in object_field_obj.labels]
                ref_object_lables = [Label.model_validate(label) for label in object_field_obj.any_field.labels]
                insert_short_label_to_labels_from_ref_labels(replaced_labels, ref_object_lables)
                object_field_obj.replaced_labels = replaced_labels
                object_field_obj.field_data_type = object_field_obj.any_field.type
                object_field_obj.precision = object_field_obj.any_field.precision
                object_field_obj.scale = object_field_obj.any_field.scale
            elif object_field_obj.field_type is not None:
                raise ValueError("There must be at least one reference to the object.")
        return object_field_obj

    @field_validator("sql_column_type", mode="after")
    @classmethod
    def check_passwords_match(cls, value: Optional[str], all_fields: core_schema.ValidationInfo) -> Optional[str]:
        if all_fields.context and all_fields.context.get("database_type") and all_fields.data.get("field_data_type"):
            database_type = all_fields.context.get("database_type")
            field_data_type = all_fields.data.get("field_data_type")
            precision = all_fields.data.get("precision")
            scale = all_fields.data.get("scale")
            if database_type in (DatabaseTypeEnum.POSTGRESQL, DatabaseTypeEnum.GREENPLUM) and field_data_type in (
                DimensionTypeEnum.TIME,
                DimensionTypeEnum.DATETIME,
                DimensionTypeEnum.TIMESTAMP,
            ):
                return DATA_TYPES[database_type][field_data_type] + f"({precision})"
            elif field_data_type == MeasureTypeEnum.DECIMAL:
                return DATA_TYPES[database_type][field_data_type] + f"({precision},{scale})"
            return DATA_TYPES[database_type][field_data_type]
        return value


class ObjectFieldRequest(RefTypeMixin):
    """Поля DSO и Composite для создания или изменения"""

    name: str = Field(
        description=models_limitations["object_field"]["name"]["description"],
        pattern=models_limitations["object_field"]["name"]["pattern"],
        min_length=models_limitations["object_field"]["name"]["min_length"],
        max_length=models_limitations["object_field"]["name"]["max_length"],
    )
    labels: list[Label] = Field(
        description=models_limitations["object_field"]["labels"]["description"],
        default=[],
        max_length=models_limitations["object_field"]["labels"]["max_length"],
    )
    sql_name: Optional["str"] = Field(
        default=None,
        description=models_limitations["object_field"]["sql_name"]["description"],
        serialization_alias=models_limitations["object_field"]["sql_name"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["object_field"]["sql_name"]["validation_alias"][0],
            models_limitations["object_field"]["sql_name"]["validation_alias"][1],
        ),
        pattern=models_limitations["object_field"]["sql_name"]["pattern"],
        min_length=models_limitations["object_field"]["sql_name"]["min_length"],
        max_length=models_limitations["object_field"]["sql_name"]["max_length"],
    )
