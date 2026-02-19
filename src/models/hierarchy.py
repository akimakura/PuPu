"""Схемы Pydantic для описания иерархий признаков."""

from enum import StrEnum
from typing import Annotated, Any, Optional, Self

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, computed_field, model_validator

from src.config import models_limitations
from src.db.hierarchy import AggregationType, HierarchyStructureType, TimeDependencyType
from src.integration.aor.model import AorType
from src.models.label import Label
from src.models.version import Versioned

DEFAULT_HIERARCHY_EXPANSION = 3


class HierarchyCreateRequest(BaseModel):
    name: str = Field(
        description=models_limitations["object_name_32"]["description"],
        min_length=models_limitations["object_name_32"]["min_length"],
        max_length=models_limitations["object_name_32"]["max_length"],
        pattern=models_limitations["object_name_32"]["pattern"],
    )
    default_expansion: Optional[int] = Field(
        description=models_limitations["hierarchy"]["default_expansion"]["description"],
        serialization_alias=models_limitations["hierarchy"]["default_expansion"]["serialization_alias"],
        validation_alias=AliasChoices(
            *models_limitations["hierarchy"]["default_expansion"]["validation_alias"],
        ),
        ge=models_limitations["hierarchy"]["default_expansion"]["grater_equals_value"],
        le=models_limitations["hierarchy"]["default_expansion"]["less_equals_value"],
        default=DEFAULT_HIERARCHY_EXPANSION,
    )
    structure_type: Optional[HierarchyStructureType] = Field(
        default=HierarchyStructureType.MIXED,
        description=models_limitations["hierarchy"]["structure_type"]["description"],
        serialization_alias=models_limitations["hierarchy"]["structure_type"]["serialization_alias"],
        validation_alias=AliasChoices(*models_limitations["hierarchy"]["structure_type"]["validation_alias"]),
    )
    time_dependency_type: Optional[TimeDependencyType] = Field(
        default=None,
        description=models_limitations["hierarchy"]["time_dependency_type"]["description"],
        serialization_alias=models_limitations["hierarchy"]["time_dependency_type"]["serialization_alias"],
        validation_alias=AliasChoices(*models_limitations["hierarchy"]["time_dependency_type"]["validation_alias"]),
    )
    aggregation_type: Optional[AggregationType] = Field(
        default=AggregationType.SUM.value,
        description=models_limitations["hierarchy"]["aggregation_type"]["description"],
        serialization_alias=models_limitations["hierarchy"]["aggregation_type"]["serialization_alias"],
        validation_alias=AliasChoices(*models_limitations["hierarchy"]["aggregation_type"]["validation_alias"]),
    )
    default_hierarchy: Optional[bool] = Field(
        description=models_limitations["hierarchy"]["default_hierarchy"]["description"],
        serialization_alias=models_limitations["hierarchy"]["default_hierarchy"]["serialization_alias"],
        default=False,
        validation_alias=AliasChoices(*models_limitations["hierarchy"]["default_hierarchy"]["validation_alias"]),
    )
    is_time_dependent: Optional[bool] = Field(
        description=models_limitations["hierarchy"]["is_time_dependent"]["description"],
        serialization_alias=models_limitations["hierarchy"]["is_time_dependent"]["serialization_alias"],
        default=False,
        validation_alias=AliasChoices(*models_limitations["hierarchy"]["is_time_dependent"]["validation_alias"]),
    )
    input_on_nodes: Optional[bool] = Field(
        description=models_limitations["hierarchy"]["input_on_nodes"]["description"],
        serialization_alias=models_limitations["hierarchy"]["input_on_nodes"]["serialization_alias"],
        default=False,
        validation_alias=AliasChoices(*models_limitations["hierarchy"]["input_on_nodes"]["validation_alias"]),
    )
    labels: list[Label] = Field(
        description=models_limitations["base_field"]["labels"]["description"],
        default=[],
        max_length=models_limitations["base_field"]["labels"]["max_length"],
    )
    additional_dimensions: list[str] = Field(
        description=models_limitations["hierarchy"]["additional_dimensions"]["description"],
        default=[],
        validation_alias=AliasChoices(
            *models_limitations["hierarchy"]["additional_dimensions"]["validation_alias"],
        ),
        max_length=models_limitations["hierarchy"]["additional_dimensions"]["max_items"],
        serialization_alias=models_limitations["hierarchy"]["additional_dimensions"]["serialization_alias"],
    )
    is_versioned: bool = Field(
        default=False,
        description=models_limitations["hierarchy"]["is_versioned"]["description"],
        validation_alias=AliasChoices(
            *models_limitations["hierarchy"]["is_versioned"]["validation_alias"],
        ),
        serialization_alias=models_limitations["hierarchy"]["is_versioned"]["serialization_alias"],
    )

    @model_validator(mode="after")
    def validate_is_time_dependent(self) -> Self:
        if self.is_time_dependent and not self.time_dependency_type:
            self.time_dependency_type = TimeDependencyType.NODE
        return self


class HierarchyEditRequest(BaseModel):
    default_expansion: Optional[int] = Field(
        description=models_limitations["hierarchy"]["default_expansion"]["description"],
        serialization_alias=models_limitations["hierarchy"]["default_expansion"]["serialization_alias"],
        ge=models_limitations["hierarchy"]["default_expansion"]["grater_equals_value"],
        le=models_limitations["hierarchy"]["default_expansion"]["less_equals_value"],
        default=None,
        validation_alias=AliasChoices(
            *models_limitations["hierarchy"]["default_expansion"]["validation_alias"],
        ),
    )
    time_dependency_type: Optional[TimeDependencyType] = Field(
        description=models_limitations["hierarchy"]["time_dependency_type"]["description"],
        serialization_alias=models_limitations["hierarchy"]["time_dependency_type"]["serialization_alias"],
        validation_alias=AliasChoices(*models_limitations["hierarchy"]["time_dependency_type"]["validation_alias"]),
        default=None,
    )
    aggregation_type: Optional[AggregationType] = Field(
        description=models_limitations["hierarchy"]["aggregation_type"]["description"],
        serialization_alias=models_limitations["hierarchy"]["aggregation_type"]["serialization_alias"],
        validation_alias=AliasChoices(*models_limitations["hierarchy"]["aggregation_type"]["validation_alias"]),
        default=None,
    )
    default_hierarchy: Optional[bool] = Field(
        description=models_limitations["hierarchy"]["default_hierarchy"]["description"],
        serialization_alias=models_limitations["hierarchy"]["default_hierarchy"]["serialization_alias"],
        default=False,
        validation_alias=AliasChoices(*models_limitations["hierarchy"]["default_hierarchy"]["validation_alias"]),
    )
    is_time_dependent: Optional[bool] = Field(
        description=models_limitations["hierarchy"]["is_time_dependent"]["description"],
        serialization_alias=models_limitations["hierarchy"]["is_time_dependent"]["serialization_alias"],
        default=False,
        validation_alias=AliasChoices(*models_limitations["hierarchy"]["is_time_dependent"]["validation_alias"]),
    )
    input_on_nodes: Optional[bool] = Field(
        description=models_limitations["hierarchy"]["input_on_nodes"]["description"],
        serialization_alias=models_limitations["hierarchy"]["input_on_nodes"]["serialization_alias"],
        default=False,
        validation_alias=AliasChoices(*models_limitations["hierarchy"]["input_on_nodes"]["validation_alias"]),
    )
    labels: Optional[list[Label]] = Field(
        description=models_limitations["base_field"]["labels"]["description"],
        default=[],
        max_length=models_limitations["base_field"]["labels"]["max_length"],
    )
    additional_dimensions: list[str] = Field(
        description=models_limitations["hierarchy"]["additional_dimensions"]["description"],
        default=[],
        validation_alias=AliasChoices(
            *models_limitations["hierarchy"]["additional_dimensions"]["validation_alias"],
        ),
        max_length=models_limitations["hierarchy"]["additional_dimensions"]["max_items"],
        serialization_alias=models_limitations["hierarchy"]["additional_dimensions"]["serialization_alias"],
    )
    is_versioned: bool = Field(
        default=False,
        description=models_limitations["hierarchy"]["is_versioned"]["description"],
        validation_alias=AliasChoices(
            *models_limitations["hierarchy"]["is_versioned"]["validation_alias"],
        ),
        serialization_alias=models_limitations["hierarchy"]["is_versioned"]["serialization_alias"],
    )

    class Config:
        extra = "ignore"

    @model_validator(mode="after")
    def validate_time_dependency(self) -> Self:
        if (
            self.is_time_dependent
            and not self.time_dependency_type
            or self.time_dependency_type is None
            and self.is_time_dependent
        ):
            raise ValueError("time_dependency_type must be specified if is_time_dependent is True")

        return self


class HierarchyDataStorages(BaseModel):
    hierarchy_versions_table: str = Field(
        description=models_limitations["hierarchy"]["data_storages"]["hierarchy_versions_table"]["description"],
        validation_alias=AliasChoices(
            *models_limitations["hierarchy"]["data_storages"]["hierarchy_versions_table"]["validation_alias"],
        ),
        serialization_alias=models_limitations["hierarchy"]["data_storages"]["hierarchy_versions_table"][
            "serialization_alias"
        ],
        max_length=models_limitations["object_name_64"]["max_length"],
        min_length=models_limitations["object_name_64"]["min_length"],
        pattern=models_limitations["object_name_64"]["pattern"],
    )
    hierarchy_texts_versions_table: str = Field(
        description=models_limitations["hierarchy"]["data_storages"]["hierarchy_texts_versions_table"]["description"],
        validation_alias=AliasChoices(
            *models_limitations["hierarchy"]["data_storages"]["hierarchy_texts_versions_table"]["validation_alias"],
        ),
        serialization_alias=models_limitations["hierarchy"]["data_storages"]["hierarchy_texts_versions_table"][
            "serialization_alias"
        ],
        max_length=models_limitations["object_name_64"]["max_length"],
        min_length=models_limitations["object_name_64"]["min_length"],
        pattern=models_limitations["object_name_64"]["pattern"],
    )
    hierarchy_nodes_table: str = Field(
        description=models_limitations["hierarchy"]["data_storages"]["hierarchy_nodes_table"]["description"],
        validation_alias=AliasChoices(
            *models_limitations["hierarchy"]["data_storages"]["hierarchy_nodes_table"]["validation_alias"],
        ),
        serialization_alias=models_limitations["hierarchy"]["data_storages"]["hierarchy_nodes_table"][
            "serialization_alias"
        ],
        max_length=models_limitations["object_name_64"]["max_length"],
        min_length=models_limitations["object_name_64"]["min_length"],
        pattern=models_limitations["object_name_64"]["pattern"],
    )
    hierarchy_text_nodes_table: str = Field(
        description=models_limitations["hierarchy"]["data_storages"]["hierarchy_text_nodes_table"]["description"],
        validation_alias=AliasChoices(
            *models_limitations["hierarchy"]["data_storages"]["hierarchy_text_nodes_table"]["validation_alias"],
        ),
        serialization_alias=models_limitations["hierarchy"]["data_storages"]["hierarchy_text_nodes_table"][
            "serialization_alias"
        ],
        max_length=models_limitations["object_name_64"]["max_length"],
        min_length=models_limitations["object_name_64"]["min_length"],
        pattern=models_limitations["object_name_64"]["pattern"],
    )


class HierarchyPvDictionaryOut(BaseModel):
    """Модель PVD для отображения в ответе по иерархии."""

    name: Optional[str] = Field(
        default=None,
        description=models_limitations["hierarchy"]["pv_dictionary"]["name"]["description"],
        serialization_alias=models_limitations["hierarchy"]["pv_dictionary"]["name"]["serialization_alias"],
        validation_alias=AliasChoices(
            *models_limitations["hierarchy"]["pv_dictionary"]["name"]["validation_alias"],
        ),
    )
    domain_name: Optional[str] = Field(
        default=None,
        description=models_limitations["hierarchy"]["pv_dictionary"]["domain_name"]["description"],
        serialization_alias=models_limitations["hierarchy"]["pv_dictionary"]["domain_name"]["serialization_alias"],
        validation_alias=AliasChoices(
            *models_limitations["hierarchy"]["pv_dictionary"]["domain_name"]["validation_alias"],
        ),
    )
    domain_label: Optional[str] = Field(
        default=None,
        description=models_limitations["hierarchy"]["pv_dictionary"]["domain_label"]["description"],
        serialization_alias=models_limitations["hierarchy"]["pv_dictionary"]["domain_label"]["serialization_alias"],
        validation_alias=AliasChoices(
            *models_limitations["hierarchy"]["pv_dictionary"]["domain_label"]["validation_alias"],
        ),
    )

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class HierarchyPvdCreateRequest(BaseModel):
    """Тело запроса на создание/обновление иерархии в PVD."""

    name: Optional[str] = Field(
        default=None,
        description="Имя иерархии в PVD. Если не указано — формируется автоматически.",
        validation_alias=AliasChoices("name"),
    )
    domain_name: Optional[str] = Field(
        default=None,
        description="Имя домена в PVD. Если не указано — берётся из настроек.",
        validation_alias=AliasChoices("domainName", "domain_name"),
    )
    domain_label: Optional[str] = Field(
        default=None,
        description="Описание домена в PVD. Если не указано — берётся из настроек.",
        validation_alias=AliasChoices("domainLabel", "domain_label"),
    )


class HierarchyMetaOut(Versioned, BaseModel):
    name: str = Field(
        description=models_limitations["object_name_32"]["description"],
        min_length=models_limitations["object_name_32"]["min_length"],
        max_length=models_limitations["object_name_32"]["max_length"],
        pattern=models_limitations["object_name_32"]["pattern"],
    )
    aor_type: AorType = Field(
        default=AorType.HIERARCHY,
        serialization_alias=models_limitations["aor_type"]["serialization_alias"],
        validation_alias=AliasChoices(*models_limitations["aor_type"]["validation_alias"]),
    )
    default_expansion: Optional[int] = Field(
        description=models_limitations["hierarchy"]["default_expansion"]["description"],
        serialization_alias=models_limitations["hierarchy"]["default_expansion"]["serialization_alias"],
        validation_alias=AliasChoices(
            *models_limitations["hierarchy"]["default_expansion"]["validation_alias"],
        ),
        ge=models_limitations["hierarchy"]["default_expansion"]["grater_equals_value"],
        le=models_limitations["hierarchy"]["default_expansion"]["less_equals_value"],
        default=None,
    )
    structure_type: Optional[HierarchyStructureType] = Field(
        description=models_limitations["hierarchy"]["structure_type"]["description"],
        serialization_alias=models_limitations["hierarchy"]["structure_type"]["serialization_alias"],
        validation_alias=AliasChoices(*models_limitations["hierarchy"]["structure_type"]["validation_alias"]),
        default=None,
    )
    time_dependency_type: Optional[TimeDependencyType] = Field(
        description=models_limitations["hierarchy"]["time_dependency_type"]["description"],
        serialization_alias=models_limitations["hierarchy"]["time_dependency_type"]["serialization_alias"],
        validation_alias=AliasChoices(*models_limitations["hierarchy"]["time_dependency_type"]["validation_alias"]),
        default=None,
    )
    pv_dictionary: Optional[HierarchyPvDictionaryOut] = Field(
        description=models_limitations["hierarchy"]["pv_dictionary"]["description"],
        validation_alias=AliasChoices(
            *models_limitations["hierarchy"]["pv_dictionary"]["validation_alias"],
        ),
        serialization_alias=models_limitations["hierarchy"]["pv_dictionary"]["serialization_alias"],
        default=None,
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
        description=models_limitations["hierarchy"]["models"]["description"],
        min_length=models_limitations["hierarchy"]["models"]["min_length"],
        max_length=models_limitations["hierarchy"]["models"]["max_length"],
        serialization_alias=models_limitations["hierarchy"]["models"]["serialization_alias"],
        validation_alias=AliasChoices(
            *models_limitations["hierarchy"]["models"]["validation_alias"],
        ),
    )

    aggregation_type: Optional[AggregationType] = Field(
        description=models_limitations["hierarchy"]["aggregation_type"]["description"],
        serialization_alias=models_limitations["hierarchy"]["aggregation_type"]["serialization_alias"],
        validation_alias=AliasChoices(*models_limitations["hierarchy"]["aggregation_type"]["validation_alias"]),
        default=None,
    )
    default_hierarchy: bool = Field(
        description=models_limitations["hierarchy"]["default_hierarchy"]["description"],
        serialization_alias=models_limitations["hierarchy"]["default_hierarchy"]["serialization_alias"],
        validation_alias=AliasChoices(*models_limitations["hierarchy"]["default_hierarchy"]["validation_alias"]),
        default=False,
    )
    is_time_dependent: bool = Field(
        description=models_limitations["hierarchy"]["is_time_dependent"]["description"],
        serialization_alias=models_limitations["hierarchy"]["is_time_dependent"]["serialization_alias"],
        validation_alias=AliasChoices(*models_limitations["hierarchy"]["is_time_dependent"]["validation_alias"]),
        default=False,
    )
    is_versioned: bool = Field(
        default=False,
        description=models_limitations["hierarchy"]["is_versioned"]["description"],
        validation_alias=AliasChoices(
            *models_limitations["hierarchy"]["is_versioned"]["validation_alias"],
        ),
        serialization_alias=models_limitations["hierarchy"]["is_versioned"]["serialization_alias"],
    )
    input_on_nodes: bool = Field(
        description=models_limitations["hierarchy"]["input_on_nodes"]["description"],
        serialization_alias=models_limitations["hierarchy"]["input_on_nodes"]["serialization_alias"],
        validation_alias=AliasChoices(*models_limitations["hierarchy"]["input_on_nodes"]["validation_alias"]),
        default=False,
    )
    labels: list[Label] = Field(
        description=models_limitations["base_field"]["labels"]["description"],
        default=[],
        max_length=models_limitations["base_field"]["labels"]["max_length"],
    )
    base_dimension: Optional[str] = Field(
        description=models_limitations["hierarchy"]["base_dimension"]["description"],
        serialization_alias=models_limitations["hierarchy"]["base_dimension"]["serialization_alias"],
        validation_alias=AliasChoices(*models_limitations["hierarchy"]["base_dimension"]["validation_alias"]),
        max_length=models_limitations["object_name_32"]["max_length"],
        min_length=models_limitations["object_name_32"]["min_length"],
        pattern=models_limitations["object_name_32"]["pattern"],
        default=None,
    )
    data_storages: HierarchyDataStorages = Field(
        description=models_limitations["hierarchy"]["data_storages"]["description"],
        serialization_alias=models_limitations["hierarchy"]["data_storages"]["serialization_alias"],
        validation_alias=AliasChoices(
            *models_limitations["hierarchy"]["data_storages"]["validation_alias"],
        ),
    )
    additional_dimensions: list[str] = Field(
        description=models_limitations["hierarchy"]["additional_dimensions"]["description"],
        default=[],
        serialization_alias=models_limitations["hierarchy"]["additional_dimensions"]["serialization_alias"],
        validation_alias=AliasChoices(
            *models_limitations["hierarchy"]["additional_dimensions"]["validation_alias"],
        ),
        max_length=models_limitations["hierarchy"]["additional_dimensions"]["max_items"],
    )

    model_config = ConfigDict(
        from_attributes=True,
    )

    @model_validator(mode="before")
    @classmethod
    def add_additional_fields(cls, hierarchy_obj: Any) -> Any:
        if isinstance(hierarchy_obj, dict):
            return hierarchy_obj
        if hasattr(hierarchy_obj, "models") and hierarchy_obj.models:
            hierarchy_obj.models_names = [model.name for model in hierarchy_obj.models]
        else:
            hierarchy_obj.models_names = []

        hierarchy_obj.data_storages = {
            "hierarchy_versions_table": hierarchy_obj.data_storage_versions,
            "hierarchy_texts_versions_table": hierarchy_obj.data_storage_text_versions,
            "hierarchy_nodes_table": hierarchy_obj.data_storage_nodes,
            "hierarchy_text_nodes_table": hierarchy_obj.data_storage_text_nodes,
        }

        if hasattr(hierarchy_obj, "pv_dictionary") and hierarchy_obj.pv_dictionary is not None:
            pv_dict = hierarchy_obj.pv_dictionary
            if not isinstance(pv_dict, HierarchyPvDictionaryOut):
                hierarchy_obj.__dict__["pv_dictionary"] = HierarchyPvDictionaryOut(
                    name=pv_dict.object_name,
                    domain_name=pv_dict.domain_name,
                    domain_label=pv_dict.domain_label,
                )

        return hierarchy_obj

    @model_validator(mode="after")
    def validate_time_dependency(self) -> Self:
        if not self.is_time_dependent:
            self.time_dependency_type = None
        return self

    @computed_field(alias=models_limitations["aor_name"]["serialization_alias"])  # type: ignore
    @property
    def aor_name(self) -> str:
        return f"{self.base_dimension}__{self.name}" if self.base_dimension else self.name


class HierarchyCopyStatus(StrEnum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"


class HierarchyCopyResponse(BaseModel):
    tenant: str
    hierarchy_name: str = Field(
        description=models_limitations["hierarchy_copy_response"]["hierarchy_name"]["description"],
        min_length=models_limitations["hierarchy_copy_response"]["hierarchy_name"]["min_length"],
        max_length=models_limitations["hierarchy_copy_response"]["hierarchy_name"]["max_length"],
        serialization_alias=models_limitations["hierarchy_copy_response"]["hierarchy_name"]["serialization_alias"],
        validation_alias=AliasChoices(
            *models_limitations["hierarchy_copy_response"]["hierarchy_name"]["validation_alias"],
        ),
    )
    names_of_models: list[
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
        description=models_limitations["hierarchy_copy_response"]["models"]["description"],
        min_length=models_limitations["hierarchy_copy_response"]["models"]["min_length"],
        max_length=models_limitations["hierarchy_copy_response"]["models"]["max_length"],
        serialization_alias=models_limitations["hierarchy_copy_response"]["models"]["serialization_alias"],
        validation_alias=AliasChoices(
            *models_limitations["hierarchy_copy_response"]["models"]["validation_alias"],
        ),
    )
    result: HierarchyCopyStatus | None = None
    comment: str | None = None
