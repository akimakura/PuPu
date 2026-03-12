"""Схемы Pydantic для описания DSO."""

from enum import StrEnum
from typing import Annotated, Any, Optional, Union

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic_core import core_schema

from src.config import models_limitations
from src.integration.aor.model import AorType
from src.models.database_object import DatabaseObject
from src.models.enum import InformationCategoryEnum
from src.models.label import Label
from src.models.model import ModelStatus
from src.models.object_field import ObjectField, ObjectFieldRequest
from src.models.version import Versioned


class DataStorageEnum(StrEnum):
    CUBELIKE = "CUBELIKE"
    TABLE = "TABLE"
    DIMENSION_TEXTS = "DIMENSION_TEXTS"
    DIMENSION_ATTRIBUTES = "DIMENSION_ATTRIBUTES"
    DIMENSION_VALUES = "DIMENSION_VALUES"
    TABLE_REPLICATED = "TABLE_REPLICATED"
    CUBELIKE_REPLICATED = "CUBELIKE_REPLICATED"

    HIERARCHY_VERSIONS = "HIERARCHY_VERSIONS"
    HIERARCHY_TEXTVERSIONS = "HIERARCHY_TEXTVERSIONS"
    HIERARCHY_NODES = "HIERARCHY_NODES"
    HIERARCHY_TEXTNODES = "HIERARCHY_TEXTNODES"

    @staticmethod
    def is_dimension_related(instance: Union["DataStorageEnum", str]) -> bool:
        if isinstance(instance, str):
            instance = DataStorageEnum(instance)

        return instance in [
            DataStorageEnum.DIMENSION_TEXTS,
            DataStorageEnum.DIMENSION_ATTRIBUTES,
            DataStorageEnum.DIMENSION_VALUES,
            DataStorageEnum.HIERARCHY_VERSIONS,
            DataStorageEnum.HIERARCHY_TEXTVERSIONS,
            DataStorageEnum.HIERARCHY_NODES,
            DataStorageEnum.HIERARCHY_TEXTNODES,
        ]

    @staticmethod
    def is_ment_to_be_replicated(instance: Union["DataStorageEnum", str]) -> bool:
        if isinstance(instance, str):
            instance = DataStorageEnum(instance)

        return instance in [
            DataStorageEnum.TABLE_REPLICATED,
            DataStorageEnum.CUBELIKE_REPLICATED,
            DataStorageEnum.DIMENSION_TEXTS,
            DataStorageEnum.DIMENSION_ATTRIBUTES,
            DataStorageEnum.DIMENSION_VALUES,
            DataStorageEnum.HIERARCHY_VERSIONS,
            DataStorageEnum.HIERARCHY_TEXTVERSIONS,
            DataStorageEnum.HIERARCHY_NODES,
            DataStorageEnum.HIERARCHY_TEXTNODES,
        ]


class DataStorageField(ObjectField):
    """Схема для чтение поля DSO."""

    is_sharding_key: bool = Field(
        description=models_limitations["data_storage_field"]["is_sharding_key"]["description"],
        serialization_alias=models_limitations["data_storage_field"]["is_sharding_key"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["data_storage_field"]["is_sharding_key"]["validation_alias"][0],
            models_limitations["data_storage_field"]["is_sharding_key"]["validation_alias"][1],
        ),
        default=False,
    )
    is_key: bool = Field(
        description=models_limitations["data_storage_field"]["is_key"]["description"],
        serialization_alias=models_limitations["data_storage_field"]["is_key"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["data_storage_field"]["is_key"]["validation_alias"][0],
            models_limitations["data_storage_field"]["is_key"]["validation_alias"][1],
        ),
        default=False,
    )
    is_tech_field: bool = Field(
        default=False,
        exclude=True,
        serialization_alias=models_limitations["data_storage_field"]["is_tech_field"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["data_storage_field"]["is_tech_field"]["validation_alias"][0],
            models_limitations["data_storage_field"]["is_tech_field"]["validation_alias"][1],
        ),
    )
    allow_null_values_local: bool = Field(
        description=models_limitations["data_storage_field"]["allow_null_values_local"]["description"],
        serialization_alias=models_limitations["data_storage_field"]["allow_null_values_local"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["data_storage_field"]["allow_null_values_local"]["validation_alias"][0],
            models_limitations["data_storage_field"]["allow_null_values_local"]["validation_alias"][1],
        ),
        default=False,
    )

    @model_validator(mode="before")
    @classmethod
    def add_allow_null_values_local_default(cls, ds_field_obj: Any) -> Any:
        if hasattr(ds_field_obj, "allow_null_values_local") and getattr(ds_field_obj, "allow_null_values_local") is None:
            ds_field_obj.allow_null_values_local = False
        return ds_field_obj


class DataStorageFieldRequest(ObjectFieldRequest):
    """Схема для изменения/создание поля DSO."""

    is_sharding_key: bool = Field(
        description=models_limitations["data_storage_field"]["is_sharding_key"]["description"],
        serialization_alias=models_limitations["data_storage_field"]["is_sharding_key"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["data_storage_field"]["is_sharding_key"]["validation_alias"][0],
            models_limitations["data_storage_field"]["is_sharding_key"]["validation_alias"][1],
        ),
        default=False,
    )
    is_key: bool = Field(
        description=models_limitations["data_storage_field"]["is_key"]["description"],
        serialization_alias=models_limitations["data_storage_field"]["is_key"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["data_storage_field"]["is_key"]["validation_alias"][0],
            models_limitations["data_storage_field"]["is_key"]["validation_alias"][1],
        ),
        default=False,
    )
    is_tech_field: bool = Field(
        default=False,
        serialization_alias=models_limitations["data_storage_field"]["is_tech_field"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["data_storage_field"]["is_tech_field"]["validation_alias"][0],
            models_limitations["data_storage_field"]["is_tech_field"]["validation_alias"][1],
        ),
    )
    allow_null_values_local: Optional[bool] = Field(
        description=models_limitations["data_storage_field"]["allow_null_values_local"]["description"],
        serialization_alias=models_limitations["data_storage_field"]["allow_null_values_local"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["data_storage_field"]["allow_null_values_local"]["validation_alias"][0],
            models_limitations["data_storage_field"]["allow_null_values_local"]["validation_alias"][1],
        ),
        default=None,
    )


class DataStorage(Versioned, BaseModel):
    """
    Схема для чтения dataStorage из базы или кэша
    Абстракция над технической таблицей для хранения метаинформации.
    """

    name: str = Field(
        description=models_limitations["object_name_64"]["description"],
        min_length=models_limitations["object_name_64"]["min_length"],
        max_length=models_limitations["object_name_64"]["max_length"],
        pattern=models_limitations["object_name_64"]["pattern"],
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
    planning_enabled: bool = Field(
        description=models_limitations["data_storage"]["planning_enabled"]["description"],
        default=False,
        serialization_alias=models_limitations["data_storage"]["planning_enabled"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["data_storage"]["planning_enabled"]["validation_alias"][0],
            models_limitations["data_storage"]["planning_enabled"]["validation_alias"][1],
        ),
    )
    sharding_key: Optional[str] = Field(exclude=True, default=None)
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
    tenant_id: Optional[str] = Field(exclude=True, default=None)
    type: DataStorageEnum = Field(
        description=models_limitations["data_storage"]["type"]["description"],
    )
    aor_type: AorType = Field(
        default=AorType.DATASTORAGE,
        serialization_alias=models_limitations["aor_type"]["serialization_alias"],
        validation_alias=AliasChoices(*models_limitations["aor_type"]["validation_alias"]),
    )
    log_data_storage_name: Optional[str] = Field(
        description=models_limitations["object_name_64"]["description"],
        min_length=models_limitations["object_name_64"]["min_length"],
        max_length=models_limitations["object_name_64"]["max_length"],
        pattern=models_limitations["object_name_64"]["pattern"],
        serialization_alias=models_limitations["data_storage"]["log_data_storage_name"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["data_storage"]["log_data_storage_name"]["validation_alias"][0],
            models_limitations["data_storage"]["log_data_storage_name"]["validation_alias"][1],
        ),
        default=None,
    )
    database_objects: list[DatabaseObject] = Field(
        description=models_limitations["data_storage"]["db_objects"]["description"],
        min_length=models_limitations["data_storage"]["db_objects"]["min_length"],
        max_length=models_limitations["data_storage"]["db_objects"]["max_length"],
        serialization_alias=models_limitations["data_storage"]["db_objects"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["data_storage"]["db_objects"]["validation_alias"][0],
            models_limitations["data_storage"]["db_objects"]["validation_alias"][1],
            models_limitations["data_storage"]["db_objects"]["validation_alias"][2],
        ),
    )
    labels: list[Label] = Field(
        description=models_limitations["data_storage"]["labels"]["description"],
        default=[],
        max_length=models_limitations["data_storage"]["labels"]["max_length"],
    )
    fields: list[DataStorageField] = Field(
        description=models_limitations["data_storage"]["fields"]["description"],
        max_length=models_limitations["data_storage"]["fields"]["max_length"],
        min_length=models_limitations["data_storage"]["fields"]["min_length"],
    )
    model_config = ConfigDict(
        from_attributes=True,
    )

    @model_validator(mode="before")
    @classmethod
    def add_additional_fields_to_measure(cls, dso_obj: Any) -> Any:
        """
        Добавляет перед валидацией дополнительные поля в валидируемый объект, если это возможно.
        Поля: models_names.
        """
        if hasattr(dso_obj, "log_data_storage") and dso_obj.log_data_storage:
            dso_obj.log_data_storage_name = dso_obj.log_data_storage.name
        if hasattr(dso_obj, "models"):
            dso_obj.models_names = [model.name for model in dso_obj.models]
            models_id_mapping = {model.id: model.name for model in dso_obj.models}
            dso_obj.models_statuses = [
                ModelStatus(
                    name=models_id_mapping[model_relation.model_id],
                    status=model_relation.status,
                    msg=model_relation.msg,
                )
                for model_relation in dso_obj.model_relations
                if models_id_mapping.get(model_relation.model_id)
            ]
        return dso_obj

    @field_validator("fields", mode="after")
    @classmethod
    def get_fields(
        cls, ds_fields: list[DataStorageField], all_fields: core_schema.ValidationInfo
    ) -> list[DataStorageField]:
        if all_fields.context and all_fields.context.get("ignore_tech_fields"):
            new_fields = []
            for ds_field in ds_fields:
                if ds_field.is_tech_field:
                    continue
                new_fields.append(ds_field)
            return new_fields
        return ds_fields


class DataStorageV0(DataStorage):
    models_statuses: list[ModelStatus] = Field(default=[], exclude=True)


class DataStorageV1(DataStorage):
    models_names: list[str] = Field(default=[], exclude=True)


class DataStorageEditRequest(BaseModel):
    """
    Схема для редактирования dataStorage
    """

    planning_enabled: Optional[bool] = Field(
        description=models_limitations["data_storage"]["planning_enabled"]["description"],
        serialization_alias=models_limitations["data_storage"]["planning_enabled"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["data_storage"]["planning_enabled"]["validation_alias"][0],
            models_limitations["data_storage"]["planning_enabled"]["validation_alias"][1],
        ),
        default=None,
    )
    information_category: Optional[InformationCategoryEnum] = Field(
        description=models_limitations["information_category"]["description"],
        default=None,
        serialization_alias=models_limitations["information_category"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["information_category"]["validation_alias"][0],
            models_limitations["information_category"]["validation_alias"][1],
        ),
    )
    log_data_storage_name: Optional[str] = Field(
        description=models_limitations["object_name_64"]["description"],
        min_length=models_limitations["object_name_64"]["min_length"],
        max_length=models_limitations["object_name_64"]["max_length"],
        pattern=models_limitations["object_name_64"]["pattern"],
        serialization_alias=models_limitations["data_storage"]["log_data_storage_name"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["data_storage"]["log_data_storage_name"]["validation_alias"][0],
            models_limitations["data_storage"]["log_data_storage_name"]["validation_alias"][1],
        ),
        default=None,
    )
    type: Optional[DataStorageEnum] = Field(
        description=models_limitations["data_storage"]["type"]["description"],
        default=None,
    )
    database_objects: Optional[list[DatabaseObject]] = Field(
        default=None,
        description=models_limitations["data_storage"]["db_objects"]["description"],
        min_length=models_limitations["data_storage"]["db_objects"]["min_length"],
        max_length=models_limitations["data_storage"]["db_objects"]["max_length"],
        serialization_alias=models_limitations["data_storage"]["db_objects"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["data_storage"]["db_objects"]["validation_alias"][0],
            models_limitations["data_storage"]["db_objects"]["validation_alias"][1],
            models_limitations["data_storage"]["db_objects"]["validation_alias"][2],
        ),
    )
    labels: Optional[list[Label]] = Field(
        description=models_limitations["data_storage"]["labels"]["description"],
        default=None,
        max_length=models_limitations["data_storage"]["labels"]["max_length"],
    )
    fields: Optional[list[DataStorageFieldRequest]] = Field(
        description=models_limitations["data_storage"]["fields"]["description"],
        default=None,
        max_length=models_limitations["data_storage"]["fields"]["max_length"],
        min_length=models_limitations["data_storage"]["fields"]["min_length"],
    )


class DataStorageCreateRequest(BaseModel):
    """Схема для создания dataStorage."""

    name: str = Field(
        description=models_limitations["data_storage"]["name"]["description"],
        min_length=models_limitations["data_storage"]["name"]["min_length"],
        max_length=models_limitations["data_storage"]["name"]["max_length"],
        pattern=models_limitations["data_storage"]["name"]["pattern"],
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
    log_data_storage_name: Optional[str] = Field(
        description=models_limitations["object_name_64"]["description"],
        min_length=models_limitations["object_name_64"]["min_length"],
        max_length=models_limitations["object_name_64"]["max_length"],
        pattern=models_limitations["object_name_64"]["pattern"],
        serialization_alias=models_limitations["data_storage"]["log_data_storage_name"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["data_storage"]["log_data_storage_name"]["validation_alias"][0],
            models_limitations["data_storage"]["log_data_storage_name"]["validation_alias"][1],
        ),
        default=None,
    )
    planning_enabled: bool = Field(
        description=models_limitations["data_storage"]["planning_enabled"]["description"],
        default=False,
        serialization_alias=models_limitations["data_storage"]["planning_enabled"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["data_storage"]["planning_enabled"]["validation_alias"][0],
            models_limitations["data_storage"]["planning_enabled"]["validation_alias"][1],
        ),
    )
    type: DataStorageEnum = Field(
        description=models_limitations["data_storage"]["type"]["description"],
    )
    database_objects: Optional[list[DatabaseObject]] = Field(
        default=None,
        description=models_limitations["data_storage"]["db_objects"]["description"],
        min_length=models_limitations["data_storage"]["db_objects"]["min_length"],
        max_length=models_limitations["data_storage"]["db_objects"]["max_length"],
        serialization_alias=models_limitations["data_storage"]["db_objects"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["data_storage"]["db_objects"]["validation_alias"][0],
            models_limitations["data_storage"]["db_objects"]["validation_alias"][1],
            models_limitations["data_storage"]["db_objects"]["validation_alias"][2],
        ),
    )
    labels: list[Label] = Field(
        description=models_limitations["data_storage"]["labels"]["description"],
        default=[],
        max_length=models_limitations["data_storage"]["labels"]["max_length"],
    )
    fields: list[DataStorageFieldRequest] = Field(
        description=models_limitations["data_storage"]["fields"]["description"],
        max_length=models_limitations["data_storage"]["fields"]["max_length"],
        min_length=models_limitations["data_storage"]["fields"]["min_length"],
    )


class DataStorageLogsFieldEnum(StrEnum):
    TIMESTAMP = "timestamp"
    BATCHID = "batchid"
    ACTION = "action"
    OPERATION = "operation"
    USERID = "userid"


class HierarchyDimensionsEnum(StrEnum):
    LANGUAGE_TAG = "language_tag"
