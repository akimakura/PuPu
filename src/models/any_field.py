"""Схемы Pydantic для описания поля DSO типа Measure (показатель)."""

from enum import StrEnum
from typing import Any, Literal, Optional

from pydantic import AliasChoices, ConfigDict, Field, field_validator, model_validator
from pydantic_core import core_schema

from src.config import models_limitations
from src.models.field import AggregationTypeEnum, BaseField


class AnyFieldTypeEnum(StrEnum):
    INTEGER = "INTEGER"
    STRING = "STRING"
    DATE = "DATE"
    TIME = "TIME"
    DATETIME = "DATETIME"
    TIMESTAMP = "TIMESTAMP"
    BOOLEAN = "BOOLEAN"
    FLOAT = "FLOAT"
    DECIMAL = "DECIMAL"
    UUID = "UUID"
    ARRAY_STRING = "ARRAY[STRING]"
    ARRAY_INTEGER = "ARRAY[INTEGER]"
    JSON = "JSON"


ANY_FIELD_WITH_REQUIRED_PRECISION = {
    AnyFieldTypeEnum.STRING,
    AnyFieldTypeEnum.INTEGER,
    AnyFieldTypeEnum.ARRAY_STRING,
    AnyFieldTypeEnum.DECIMAL,
    AnyFieldTypeEnum.FLOAT,
}


def _raise_if_needed_precision_absent(any_field_type: AnyFieldTypeEnum | str | None, precision: int | None) -> None:
    if any_field_type in ANY_FIELD_WITH_REQUIRED_PRECISION and not precision:
        raise ValueError(f"Precision is required for any_field types: {ANY_FIELD_WITH_REQUIRED_PRECISION}")


class AnyField(BaseField):
    """Поле без справочника."""

    type: Literal[
        AnyFieldTypeEnum.INTEGER,
        AnyFieldTypeEnum.STRING,
        AnyFieldTypeEnum.DATE,
        AnyFieldTypeEnum.TIME,
        AnyFieldTypeEnum.DATETIME,
        AnyFieldTypeEnum.TIMESTAMP,
        AnyFieldTypeEnum.BOOLEAN,
        AnyFieldTypeEnum.FLOAT,
        AnyFieldTypeEnum.DECIMAL,
        AnyFieldTypeEnum.UUID,
        AnyFieldTypeEnum.ARRAY_STRING,
        AnyFieldTypeEnum.ARRAY_INTEGER,
        AnyFieldTypeEnum.JSON,
    ] = Field(
        description=models_limitations["any_field"]["type"]["description"],
        max_length=models_limitations["any_field"]["type"]["max_length"],
    )
    precision: Optional[int] = Field(  # type: ignore[assignment]
        description="Общая длина поля.",
        ge=models_limitations["base_field"]["precision"]["ge"],
        le=models_limitations["base_field"]["precision"]["le"],
        default=None,
        validate_default=False,
    )
    scale: Optional[int] = Field(
        description=models_limitations["any_field"]["scale"]["description"],
        ge=models_limitations["any_field"]["scale"]["ge"],
        le=models_limitations["any_field"]["scale"]["le"],
        default=None,
    )
    aggregation_type: Optional[
        Literal[
            AggregationTypeEnum.SUMMATION,
            AggregationTypeEnum.MAXIMUM,
            AggregationTypeEnum.NO_AGGREGATION,
            AggregationTypeEnum.MINIMUM,
        ]
    ] = Field(
        description=models_limitations["any_field"]["aggregation_type"]["description"],
        serialization_alias=models_limitations["any_field"]["aggregation_type"]["serialization_alias"],
        max_length=models_limitations["any_field"]["aggregation_type"]["max_length"],
        validation_alias=AliasChoices(
            models_limitations["any_field"]["aggregation_type"]["validation_alias"][0],
            models_limitations["any_field"]["aggregation_type"]["validation_alias"][1],
        ),
        default=None,
    )
    allow_null_values: bool = Field(
        description=models_limitations["any_field"]["allow_null_values"]["description"],
        serialization_alias=models_limitations["any_field"]["allow_null_values"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["any_field"]["allow_null_values"]["validation_alias"][0],
            models_limitations["any_field"]["allow_null_values"]["validation_alias"][1],
        ),
        default=False,
    )
    model_config = ConfigDict(from_attributes=True)

    @model_validator(mode="before")
    @classmethod
    def add_allow_null_values_default(cls, any_field_obj: Any) -> Any:
        if hasattr(any_field_obj, "allow_null_values") and getattr(any_field_obj, "allow_null_values") is None:
            any_field_obj.allow_null_values = False
        return any_field_obj

    @field_validator("scale")
    @classmethod
    def scale_validator(
        cls, scale_field: Optional[int], other_fields: core_schema.ValidationInfo, **kwargs: dict
    ) -> Optional[str | int]:
        type = other_fields.data.get("type")
        if scale_field is None and type == AnyFieldTypeEnum.DECIMAL:
            raise ValueError("The scale cannot be null for decimal any_field")
        return scale_field

    @model_validator(mode="after")
    def validate_precision(self) -> Any:
        if not self.type and not self.precision:
            return self
        if hasattr(self, "type"):
            _raise_if_needed_precision_absent(self.type, self.precision)
        if hasattr(self, "precision") and self.precision is None and not self.type:
            raise ValueError("Precision cannot be changed without specifying the type")
        return self
