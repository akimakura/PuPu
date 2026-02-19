"""Базовые классы для полей measure, dimension, anyfield, от которых наследуются другие схемы, описывающие поля"""

from enum import StrEnum
from typing import TYPE_CHECKING, Optional, Union

from pydantic import AliasChoices, BaseModel, Field, field_validator
from pydantic_core import core_schema
from typing_extensions import Annotated

from src.config import models_limitations
from src.models.label import Label

if TYPE_CHECKING:
    from src.models.any_field import AnyField


class AggregationTypeEnum(StrEnum):
    SUMMATION = "SUMMATION"
    MAXIMUM = "MAXIMUM"
    MINIMUM = "MINIMUM"
    NO_AGGREGATION = "NO AGGREGATION"


class SemanticType(StrEnum):
    DIMENSION = "DIMENSION"
    MEASURE = "MEASURE"


class BaseField(BaseModel):
    name: str = Field(
        description=models_limitations["base_field"]["name"]["description"],
        min_length=models_limitations["base_field"]["name"]["min_length"],
        pattern=models_limitations["base_field"]["name"]["pattern"],
        max_length=models_limitations["base_field"]["name"]["max_length"],
    )
    type: str = Field(
        description=models_limitations["base_field"]["type"]["description"],
        max_length=models_limitations["base_field"]["type"]["max_length"],
    )
    precision: int = Field(
        description="Общая длина поля.",
        ge=models_limitations["base_field"]["precision"]["ge"],
        le=models_limitations["base_field"]["precision"]["le"],
    )
    labels: list[Label] = Field(
        description=models_limitations["base_field"]["labels"]["description"],
        default=[],
        max_length=models_limitations["base_field"]["labels"]["max_length"],
    )


class BaseFieldTypeEnum(StrEnum):
    """Тип ссылочного объекта."""

    ANYFIELD = "ANYFIELD"
    DIMENSION = "DIMENSION"
    MEASURE = "MEASURE"


measure_field_type = Annotated[
    str,
    Field(
        description="Measure",
        min_length=models_limitations["object_name_32"]["min_length"],
        max_length=models_limitations["object_name_32"]["max_length"],
        pattern=models_limitations["object_name_32"]["pattern"],
    ),
]
dimension_field_type = Annotated[
    str,
    Field(
        description="Dimension",
        min_length=models_limitations["object_name_32"]["min_length"],
        max_length=models_limitations["object_name_32"]["max_length"],
        pattern=models_limitations["object_name_32"]["pattern"],
    ),
]


class BaseFieldType(BaseModel):
    """Тип ссылочного поля."""

    ref_object_type: BaseFieldTypeEnum = Field(
        description=models_limitations["base_field_type"]["ref_object_type"]["description"],
        serialization_alias=models_limitations["base_field_type"]["ref_object_type"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["base_field_type"]["ref_object_type"]["validation_alias"][0],
            models_limitations["base_field_type"]["ref_object_type"]["validation_alias"][1],
        ),
    )
    ref_object: Union["AnyField", measure_field_type, dimension_field_type] = Field(
        description=models_limitations["base_field_type"]["ref_object"]["description"],
        serialization_alias=models_limitations["base_field_type"]["ref_object"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["base_field_type"]["ref_object"]["validation_alias"][0],
            models_limitations["base_field_type"]["ref_object"]["validation_alias"][1],
        ),
    )

    @field_validator("ref_object")
    @classmethod
    def validate_connection(
        cls, value: Union["str", "AnyField"], values: core_schema.ValidationInfo, **kwargs: dict
    ) -> Union["str", "AnyField"]:
        if values.data.get("ref_object_type") != BaseFieldTypeEnum.ANYFIELD and not isinstance(value, str):
            raise ValueError("refObject can't be Anyfield")
        return value


class RefTypeMixin(BaseModel):
    """Миксин добавляющий поля semantic_type и ref_type."""

    ref_type: BaseFieldType = Field(
        description=models_limitations["dimension_attribute"]["ref_type"]["description"],
        validation_alias=AliasChoices(
            models_limitations["dimension_attribute"]["ref_type"]["validation_alias"][0],
            models_limitations["dimension_attribute"]["ref_type"]["validation_alias"][1],
        ),
        serialization_alias=models_limitations["dimension_attribute"]["ref_type"]["serialization_alias"],
    )
    semantic_type: SemanticType = Field(
        description=models_limitations["object_field"]["semantic_type"]["description"],
        serialization_alias=models_limitations["object_field"]["semantic_type"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["object_field"]["semantic_type"]["validation_alias"][0],
            models_limitations["object_field"]["semantic_type"]["validation_alias"][1],
        ),
    )

    @field_validator("semantic_type")
    @classmethod
    def semantic_type_validator(
        cls, semantic_type: SemanticType, all_fields: core_schema.ValidationInfo, **kwargs: dict
    ) -> Optional[SemanticType]:
        """
        Проверяет semantic_type на соответсвие ref_type.
        """
        if all_fields.data.get("ref_type") is None:
            raise ValueError("Field ref_type cannot be null. You may have passed an invalid 'type'.")
        if (
            all_fields.data["ref_type"].ref_object_type != BaseFieldTypeEnum.ANYFIELD
            and semantic_type != all_fields.data["ref_type"].ref_object_type
        ):
            raise ValueError("The semantic type cannot differ from the ref_type for the case of dimension and measure.")
        return semantic_type
