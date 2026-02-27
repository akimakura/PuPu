from enum import StrEnum
from typing import Any, Optional
from uuid import UUID, uuid4

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, model_validator

from src.config import models_limitations
from src.integration.aor.model import AorType
from src.models.database import Database
from src.models.label import Label
from src.models.version import Versioned


class Model(Versioned):
    """
    Схема модели для чтения из кэша или бд,
    Модель - совокупность: признаков, показателей, хранилищ,
    композитов, других объектов принадлежащей одной области системы.
    """

    name: str = Field(
        description=models_limitations["object_name_32"]["description"],
        min_length=models_limitations["object_name_32"]["min_length"],
        max_length=models_limitations["object_name_32"]["max_length"],
        pattern=models_limitations["object_name_32"]["pattern"],
    )
    aor_type: AorType = Field(
        default=AorType.MODEL,
        serialization_alias=models_limitations["aor_type"]["serialization_alias"],
        validation_alias=AliasChoices(*models_limitations["aor_type"]["validation_alias"]),
    )
    labels: list[Label] = Field(
        description=models_limitations["model"]["labels"]["description"],
        default=[],
        examples=models_limitations["model"]["labels"]["examples"],
        max_length=models_limitations["model"]["labels"]["max_length"],
    )
    tenant_id: Optional[str] = Field(exclude=True, default=None)
    schema_name: str = Field(
        description=models_limitations["model"]["schema_name"]["description"],
        serialization_alias=models_limitations["model"]["schema_name"]["serialization_alias"],
        min_length=models_limitations["model"]["schema_name"]["min_length"],
        max_length=models_limitations["model"]["schema_name"]["max_length"],
        pattern=models_limitations["model"]["schema_name"]["pattern"],
        validation_alias=AliasChoices(
            models_limitations["model"]["schema_name"]["validation_alias"][0],
            models_limitations["model"]["schema_name"]["validation_alias"][1],
        ),
    )
    database: Optional[Database] = Field(exclude=True, default=None)
    database_name: str = Field(
        description=models_limitations["object_name_32"]["description"],
        min_length=models_limitations["object_name_32"]["min_length"],
        max_length=models_limitations["object_name_32"]["max_length"],
        pattern=models_limitations["object_name_32"]["pattern"],
        serialization_alias=models_limitations["model"]["database_id"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["model"]["database_id"]["validation_alias"][0],
            models_limitations["model"]["database_id"]["validation_alias"][1],
        ),
    )
    dimension_tech_fields: bool = Field(
        default=False,
        serialization_alias=models_limitations["model"]["dimension_tech_fields"]["serialization_alias"],
        validation_alias=AliasChoices(*models_limitations["model"]["dimension_tech_fields"]["validation_alias"]),
    )
    aor_space_id: Optional[UUID] = Field(
        default=None,
        serialization_alias=models_limitations["model"]["aor_space_id"]["serialization_alias"],
        validation_alias=AliasChoices(*models_limitations["model"]["aor_space_id"]["validation_alias"]),
    )
    model_config = ConfigDict(from_attributes=True)

    @model_validator(mode="before")
    @classmethod
    def add_additional_field_to_model(cls, model_obj: Any) -> Any:
        """
        Добавляет перед валидацией дополнительное поле database_name в валидируемый объект, если это возможно.
        """
        if hasattr(model_obj, "database"):
            model_obj.database_name = model_obj.database.name
        return model_obj

class ModelEditRequest(BaseModel):
    """
    Схема модели для обновления patch запрсоом.
    """

    labels: Optional[list[Label]] = Field(
        description=models_limitations["model"]["labels"]["description"],
        default=None,
        max_length=models_limitations["model"]["labels"]["max_length"],
    )
    schema_name: Optional[str] = Field(
        description=models_limitations["model"]["schema_name"]["description"],
        min_length=models_limitations["model"]["schema_name"]["min_length"],
        max_length=models_limitations["model"]["schema_name"]["max_length"],
        pattern=models_limitations["model"]["schema_name"]["pattern"],
        serialization_alias=models_limitations["model"]["schema_name"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["model"]["schema_name"]["validation_alias"][0],
            models_limitations["model"]["schema_name"]["validation_alias"][1],
        ),
        default=None,
    )
    database_id: Optional[str] = Field(
        description=models_limitations["object_name_32"]["description"],
        min_length=models_limitations["object_name_32"]["min_length"],
        max_length=models_limitations["object_name_32"]["max_length"],
        pattern=models_limitations["object_name_32"]["pattern"],
        serialization_alias=models_limitations["model"]["database_id"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["model"]["database_id"]["validation_alias"][0],
            models_limitations["model"]["database_id"]["validation_alias"][1],
            models_limitations["model"]["database_id"]["validation_alias"][2],
        ),
        default=None,
    )
    dimension_tech_fields: Optional[bool] = Field(
        default=None,
        serialization_alias=models_limitations["model"]["dimension_tech_fields"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["model"]["dimension_tech_fields"]["validation_alias"][0],
            models_limitations["model"]["dimension_tech_fields"]["validation_alias"][1],
        ),
    )
    aor_space_id: Optional[UUID] = Field(
        default=None,
        serialization_alias=models_limitations["model"]["aor_space_id"]["serialization_alias"],
        validation_alias=AliasChoices(*models_limitations["model"]["aor_space_id"]["validation_alias"]),
    )


class ModelCreateRequest(BaseModel):
    """
    Схема модели для создания post запрсоом.
    """

    name: str = Field(
        description=models_limitations["object_name_32"]["description"],
        min_length=models_limitations["object_name_32"]["min_length"],
        max_length=models_limitations["object_name_32"]["max_length"],
        pattern=models_limitations["object_name_32"]["pattern"],
    )
    labels: list[Label] = Field(
        description=models_limitations["model"]["labels"]["description"],
        default=[],
        max_length=models_limitations["model"]["labels"]["max_length"],
    )
    schema_name: str = Field(
        description=models_limitations["model"]["schema_name"]["description"],
        min_length=models_limitations["model"]["schema_name"]["min_length"],
        max_length=models_limitations["model"]["schema_name"]["max_length"],
        serialization_alias=models_limitations["model"]["schema_name"]["serialization_alias"],
        pattern=models_limitations["model"]["schema_name"]["pattern"],
        validation_alias=AliasChoices(
            models_limitations["model"]["schema_name"]["validation_alias"][0],
            models_limitations["model"]["schema_name"]["validation_alias"][1],
        ),
    )
    database_id: str = Field(
        description=models_limitations["object_name_32"]["description"],
        min_length=models_limitations["object_name_32"]["min_length"],
        max_length=models_limitations["object_name_32"]["max_length"],
        pattern=models_limitations["object_name_32"]["pattern"],
        serialization_alias=models_limitations["model"]["database_id"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["model"]["database_id"]["validation_alias"][0],
            models_limitations["model"]["database_id"]["validation_alias"][1],
            models_limitations["model"]["database_id"]["validation_alias"][2],
        ),
    )
    dimension_tech_fields: bool = Field(
        default=False,
        serialization_alias=models_limitations["model"]["dimension_tech_fields"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["model"]["dimension_tech_fields"]["validation_alias"][0],
            models_limitations["model"]["dimension_tech_fields"]["validation_alias"][1],
        ),
    )
    aor_space_id: Optional[UUID] = Field(
        default_factory=lambda: uuid4(),
        serialization_alias=models_limitations["model"]["aor_space_id"]["serialization_alias"],
        validation_alias=AliasChoices(*models_limitations["model"]["aor_space_id"]["validation_alias"]),
    )


class ModelStatusEnum(StrEnum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    PENDING = "PENDING"


class ModelStatus(BaseModel):
    name: str = Field(
        description=models_limitations["object_name_32"]["description"],
        max_length=models_limitations["object_name_32"]["max_length"],
        min_length=models_limitations["object_name_32"]["min_length"],
        pattern=models_limitations["object_name_32"]["pattern"],
    )
    status: ModelStatusEnum = Field(
        description=models_limitations["model_status"]["status"]["description"],
    )
    msg: Optional[str] = Field(
        description=models_limitations["model_status"]["msg"]["description"],
        max_length=models_limitations["model_status"]["msg"]["max_length"],
        min_length=models_limitations["model_status"]["msg"]["min_length"],
        default=None,
    )
