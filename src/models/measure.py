"""Схемы Pydantic для описания поля DSO типа Measure (показатель)."""

from enum import StrEnum
from typing import Any, Optional

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic_core import core_schema
from typing_extensions import Annotated

from src.config import models_limitations
from src.integration.aor.model import AorType
from src.models.field import AggregationTypeEnum
from src.models.label import Label
from src.models.model import ModelStatus
from src.models.version import Versioned


class MeasureTypeEnum(StrEnum):
    INTEGER = "INTEGER"
    FLOAT = "FLOAT"
    DECIMAL = "DECIMAL"


class DimensionValue(BaseModel):
    dimension_name: str = Field(
        description=models_limitations["object_name_32"]["description"],
        min_length=models_limitations["object_name_32"]["min_length"],
        max_length=models_limitations["object_name_32"]["max_length"],
        pattern=models_limitations["object_name_32"]["pattern"],
        serialization_alias=models_limitations["dimension_value"]["dimension_id"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension_value"]["dimension_id"]["validation_alias"][0],
            models_limitations["dimension_value"]["dimension_id"]["validation_alias"][1],
        ),
    )
    dimension_value: Optional[str] = Field(
        description=models_limitations["dimension_value"]["dimension_value"]["description"],
        serialization_alias=models_limitations["dimension_value"]["dimension_value"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension_value"]["dimension_value"]["validation_alias"][0],
            models_limitations["dimension_value"]["dimension_value"]["validation_alias"][1],
        ),
        max_length=models_limitations["dimension_value"]["dimension_value"]["max_length"],
        pattern=models_limitations["dimension_value"]["dimension_value"]["pattern"],
        default=None,
    )
    model_config = ConfigDict(from_attributes=True)

    @model_validator(mode="before")
    @classmethod
    def add_additional_field_to_dimension_value(cls, dimension_value_obj: Any) -> Any:
        """
        Добавляет перед валидацией дополнительное поле dimension_name в валидируемый объект, если это возможно.
        """
        if hasattr(dimension_value_obj, "dimension") and dimension_value_obj.dimension:
            dimension_value_obj.dimension_name = dimension_value_obj.dimension.name
        elif hasattr(dimension_value_obj, "dimension_id") and isinstance(dimension_value_obj.dimension_id, str):
            dimension_value_obj.dimension_name = dimension_value_obj.dimension_id
        return dimension_value_obj


class DimensionValueRequest(BaseModel):
    dimension_id: str = Field(
        serialization_alias=models_limitations["dimension_value"]["dimension_id"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension_value"]["dimension_id"]["validation_alias"][0],
            models_limitations["dimension_value"]["dimension_id"]["validation_alias"][1],
            models_limitations["dimension_value"]["dimension_id"]["validation_alias"][2],
        ),
        description=models_limitations["object_name_32"]["description"],
        min_length=models_limitations["object_name_32"]["min_length"],
        max_length=models_limitations["object_name_32"]["max_length"],
        pattern=models_limitations["object_name_32"]["pattern"],
    )
    dimension_value: Optional[str] = Field(
        description=models_limitations["dimension_value"]["dimension_value"]["description"],
        serialization_alias=models_limitations["dimension_value"]["dimension_value"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["dimension_value"]["dimension_value"]["validation_alias"][0],
            models_limitations["dimension_value"]["dimension_value"]["validation_alias"][1],
        ),
        max_length=models_limitations["dimension_value"]["dimension_value"]["max_length"],
        pattern=models_limitations["dimension_value"]["dimension_value"]["pattern"],
        default=None,
    )
    model_config = ConfigDict(from_attributes=True)


class MeasureCreateRequest(BaseModel):
    """Показатель (некоторое число)."""

    name: str = Field(
        description=models_limitations["object_name_32"]["description"],
        min_length=models_limitations["object_name_32"]["min_length"],
        max_length=models_limitations["object_name_32"]["max_length"],
        pattern=models_limitations["object_name_32"]["pattern"],
    )
    precision: int = Field(
        description=models_limitations["measure"]["precision"]["description"],
        ge=models_limitations["measure"]["precision"]["ge"],
        le=models_limitations["measure"]["precision"]["le"],
    )
    labels: list[Label] = Field(
        description=models_limitations["base_field"]["labels"]["description"],
        default=[],
        max_length=models_limitations["base_field"]["labels"]["max_length"],
    )
    type: MeasureTypeEnum = Field(
        description=models_limitations["measure"]["type"]["description"],
        max_length=models_limitations["measure"]["type"]["max_length"],
    )
    scale: Optional[int] = Field(
        description=models_limitations["measure"]["scale"]["description"],
        ge=models_limitations["measure"]["scale"]["ge"],
        le=models_limitations["measure"]["scale"]["le"],
    )
    auth_relevant: bool = Field(
        description=models_limitations["measure"]["auth_relevant"]["description"],
        serialization_alias=models_limitations["measure"]["auth_relevant"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["measure"]["auth_relevant"]["validation_alias"][0],
            models_limitations["measure"]["auth_relevant"]["validation_alias"][1],
        ),
        default=False,
    )
    allow_null_values: bool = Field(
        description=models_limitations["measure"]["allow_null_values"]["description"],
        serialization_alias=models_limitations["measure"]["allow_null_values"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["measure"]["allow_null_values"]["validation_alias"][0],
            models_limitations["measure"]["allow_null_values"]["validation_alias"][1],
        ),
        default=False,
    )
    aggregation_type: AggregationTypeEnum = Field(
        description=models_limitations["measure"]["aggregation_type"]["description"],
        serialization_alias=models_limitations["measure"]["aggregation_type"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["measure"]["aggregation_type"]["validation_alias"][0],
            models_limitations["measure"]["aggregation_type"]["validation_alias"][1],
        ),
        max_length=models_limitations["measure"]["aggregation_type"]["max_length"],
    )
    filter: list[DimensionValueRequest] = Field(
        description=models_limitations["measure"]["filter"]["description"],
        max_length=models_limitations["measure"]["filter"]["max_length"],
        default=[],
    )
    unit_of_measure: Optional[
        DimensionValueRequest
        | Annotated[
            str,
            Field(
                description=models_limitations["object_name_32"]["description"],
                min_length=models_limitations["object_name_32"]["min_length"],
                max_length=models_limitations["object_name_32"]["max_length"],
                pattern=models_limitations["object_name_32"]["pattern"],
            ),
        ]
    ] = Field(
        description=models_limitations["measure"]["unit_of_measure"]["description"],
        validation_alias=AliasChoices(
            models_limitations["measure"]["unit_of_measure"]["validation_alias"][0],
            models_limitations["measure"]["unit_of_measure"]["validation_alias"][1],
        ),
        serialization_alias=models_limitations["measure"]["unit_of_measure"]["serialization_alias"],
        default=None,
    )

    @field_validator("scale")
    @classmethod
    def scale_validator(
        cls, scale_field: Optional[int], other_fields: core_schema.ValidationInfo, **kwargs: dict
    ) -> Optional[str | int]:
        type = other_fields.data.get("type")
        if scale_field is None and type == MeasureTypeEnum.DECIMAL:
            raise ValueError("The scale cannot be null for decimal measure")
        return scale_field


class Measure(Versioned, BaseModel):
    """Показатель (некоторое число)."""

    name: str = Field(
        description=models_limitations["object_name_32"]["description"],
        min_length=models_limitations["object_name_32"]["min_length"],
        max_length=models_limitations["object_name_32"]["max_length"],
        pattern=models_limitations["object_name_32"]["pattern"],
    )
    aor_type: AorType = Field(
        default=AorType.MEASURE,
        serialization_alias=models_limitations["aor_type"]["serialization_alias"],
        validation_alias=AliasChoices(*models_limitations["aor_type"]["validation_alias"]),
    )
    precision: int = Field(
        description=models_limitations["measure"]["precision"]["description"],
        ge=models_limitations["measure"]["precision"]["ge"],
        le=models_limitations["measure"]["precision"]["le"],
    )
    labels: list[Label] = Field(
        description=models_limitations["base_field"]["labels"]["description"],
        default=[],
        max_length=models_limitations["base_field"]["labels"]["max_length"],
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
        description=models_limitations["measure"]["models_names"]["description"],
        min_length=models_limitations["measure"]["models_names"]["min_length"],
        max_length=models_limitations["measure"]["models_names"]["max_length"],
        serialization_alias=models_limitations["measure"]["models_names"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["measure"]["models_names"]["validation_alias"][0],
            models_limitations["measure"]["models_names"]["validation_alias"][1],
        ),
    )
    models_statuses: list[ModelStatus] = Field(
        description=models_limitations["measure"]["models_statuses"]["description"],
        min_length=models_limitations["measure"]["models_statuses"]["min_length"],
        max_length=models_limitations["measure"]["models_statuses"]["max_length"],
        serialization_alias=models_limitations["measure"]["models_statuses"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["measure"]["models_statuses"]["validation_alias"][0],
            models_limitations["measure"]["models_statuses"]["validation_alias"][1],
        ),
    )

    type: MeasureTypeEnum = Field(
        description=models_limitations["measure"]["type"]["description"],
        max_length=models_limitations["measure"]["type"]["max_length"],
    )
    scale: Optional[int] = Field(
        description=models_limitations["measure"]["scale"]["description"],
        ge=models_limitations["measure"]["scale"]["ge"],
        le=models_limitations["measure"]["scale"]["le"],
    )
    auth_relevant: bool = Field(
        description=models_limitations["measure"]["auth_relevant"]["description"],
        serialization_alias=models_limitations["measure"]["auth_relevant"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["measure"]["auth_relevant"]["validation_alias"][0],
            models_limitations["measure"]["auth_relevant"]["validation_alias"][1],
        ),
        default=False,
    )
    allow_null_values: bool = Field(
        description=models_limitations["measure"]["allow_null_values"]["description"],
        serialization_alias=models_limitations["measure"]["allow_null_values"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["measure"]["allow_null_values"]["validation_alias"][0],
            models_limitations["measure"]["allow_null_values"]["validation_alias"][1],
        ),
        default=False,
    )
    aggregation_type: AggregationTypeEnum = Field(
        description=models_limitations["measure"]["aggregation_type"]["description"],
        serialization_alias=models_limitations["measure"]["aggregation_type"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["measure"]["aggregation_type"]["validation_alias"][0],
            models_limitations["measure"]["aggregation_type"]["validation_alias"][1],
        ),
        max_length=models_limitations["measure"]["aggregation_type"]["max_length"],
    )
    filter: list[DimensionValue] = Field(
        description=models_limitations["measure"]["filter"]["description"],
        max_length=models_limitations["measure"]["filter"]["max_length"],
        default=[],
    )
    unit_of_measure: Optional[
        DimensionValue
        | Annotated[
            str,
            Field(
                description=models_limitations["object_name_32"]["description"],
                min_length=models_limitations["object_name_32"]["min_length"],
                max_length=models_limitations["object_name_32"]["max_length"],
                pattern=models_limitations["object_name_32"]["pattern"],
            ),
        ]
    ] = Field(
        description=models_limitations["measure"]["unit_of_measure"]["description"],
        validation_alias=AliasChoices(
            models_limitations["measure"]["unit_of_measure"]["validation_alias"][0],
            models_limitations["measure"]["unit_of_measure"]["validation_alias"][1],
        ),
        serialization_alias=models_limitations["measure"]["unit_of_measure"]["serialization_alias"],
        default=None,
    )
    model_config = ConfigDict(from_attributes=True)

    @model_validator(mode="before")
    @classmethod
    def add_additional_fields_to_measure(cls, measure_obj: Any) -> Any:
        """
        Добавляет перед валидацией дополнительные поля в валидируемый объект, если это возможно.
        Поля: dimension_name, unit_of_measure.
        """
        if hasattr(measure_obj, "models"):
            measure_obj.models_names = [model.name for model in measure_obj.models]
            models_id_mapping = {model.id: model.name for model in measure_obj.models}
            measure_obj.models_statuses = [
                ModelStatus(
                    name=models_id_mapping[model_relation.model_id],
                    status=model_relation.status,
                    msg=model_relation.msg,
                )
                for model_relation in measure_obj.model_relations
                if models_id_mapping.get(model_relation.model_id)
            ]
        if hasattr(measure_obj, "dimension"):
            measure_obj.dimension_name = measure_obj.dimension.name if measure_obj.dimension is not None else None

        if hasattr(measure_obj, "dimension_id") and isinstance(measure_obj.dimension_id, str):
            measure_obj.dimension_name = measure_obj.dimension_id
        if hasattr(measure_obj, "allow_null_values") and getattr(measure_obj, "allow_null_values") is None:
            measure_obj.allow_null_values = False
        if (
            hasattr(measure_obj, "dimension_value")
            and hasattr(measure_obj, "dimension_name")
            and measure_obj.dimension_name
            and measure_obj.dimension_value
        ):
            measure_obj.unit_of_measure = DimensionValue(
                dimension_name=measure_obj.dimension_name, dimension_value=measure_obj.dimension_value
            )
        elif hasattr(measure_obj, "dimension_name") and measure_obj.dimension_name:
            measure_obj.unit_of_measure = measure_obj.dimension_name
        return measure_obj


class MeasureV0(Measure):
    models_statuses: list[ModelStatus] = Field(default=[], exclude=True)


class MeasureV1(Measure):
    models_names: list[str] = Field(default=[], exclude=True)


class MeasureEditRequest(BaseModel):
    type: Optional[MeasureTypeEnum] = Field(
        description=models_limitations["measure"]["type"]["description"],
        max_length=models_limitations["measure"]["type"]["max_length"],
        default=None,
    )
    precision: Optional[int] = Field(
        description=models_limitations["measure"]["precision"]["description"],
        ge=models_limitations["measure"]["precision"]["ge"],
        le=models_limitations["measure"]["precision"]["le"],
        default=None,
    )
    labels: Optional[list[Label]] = Field(
        description=models_limitations["base_field"]["labels"]["description"],
        default=None,
        max_length=models_limitations["base_field"]["labels"]["max_length"],
    )
    scale: Optional[int] = Field(
        description=models_limitations["measure"]["scale"]["description"],
        ge=models_limitations["measure"]["scale"]["ge"],
        le=models_limitations["measure"]["scale"]["le"],
        default=None,
    )
    auth_relevant: Optional[bool] = Field(
        description=models_limitations["measure"]["auth_relevant"]["description"],
        serialization_alias=models_limitations["measure"]["auth_relevant"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["measure"]["auth_relevant"]["validation_alias"][0],
            models_limitations["measure"]["auth_relevant"]["validation_alias"][1],
        ),
        default=None,
    )
    allow_null_values: Optional[bool] = Field(
        description=models_limitations["measure"]["allow_null_values"]["description"],
        serialization_alias=models_limitations["measure"]["allow_null_values"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["measure"]["allow_null_values"]["validation_alias"][0],
            models_limitations["measure"]["allow_null_values"]["validation_alias"][1],
        ),
        default=None,
    )
    aggregation_type: Optional[AggregationTypeEnum] = Field(
        description=models_limitations["measure"]["aggregation_type"]["description"],
        serialization_alias=models_limitations["measure"]["aggregation_type"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["measure"]["aggregation_type"]["validation_alias"][0],
            models_limitations["measure"]["aggregation_type"]["validation_alias"][1],
        ),
        default=None,
        max_length=models_limitations["measure"]["aggregation_type"]["max_length"],
    )
    filter: Optional[list[DimensionValueRequest]] = Field(
        description=models_limitations["measure"]["filter"]["description"],
        max_length=models_limitations["measure"]["filter"]["max_length"],
        default=None,
    )
    unit_of_measure: Optional[
        DimensionValueRequest
        | Annotated[
            str,
            Field(
                description=models_limitations["object_name_32"]["description"],
                min_length=models_limitations["object_name_32"]["min_length"],
                max_length=models_limitations["object_name_32"]["max_length"],
                pattern=models_limitations["object_name_32"]["pattern"],
            ),
        ]
    ] = Field(
        description=models_limitations["measure"]["unit_of_measure"]["description"],
        validation_alias=AliasChoices(
            models_limitations["measure"]["unit_of_measure"]["validation_alias"][0],
            models_limitations["measure"]["unit_of_measure"]["validation_alias"][1],
        ),
        serialization_alias=models_limitations["measure"]["unit_of_measure"]["serialization_alias"],
        default=None,
    )
