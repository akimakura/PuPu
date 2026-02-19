"""Схемы Pydantic для описания Database."""

from enum import StrEnum
from typing import Literal, Optional
from uuid import UUID

from pydantic import AliasChoices, BaseModel, ConfigDict, Field

from src.config import models_limitations


class AORCreateRequestTypesEnum(StrEnum):
    """Типы запросов для создания AOR."""

    MODEL = "MODEL"
    HIERARCHY = "HIERARCHY"
    DIMENSION = "DIMENSION"
    MEASURE = "MEASURE"
    COMPOSITE = "COMPOSITE"
    DATASTORAGE = "DATASTORAGE"
    DATABASEOBJECT = "DATABASEOBJECT"
    DATABASE = "DATABASE"
    AIGROUP = "AIGROUP"


class CommandEnum(StrEnum):
    DELETE = "DELETE"
    CREATE = "CREATE"
    COPY = "COPY"
    UPDATE = "UPDATE"


class CreateAorRequest(BaseModel):
    """Модель запроса на создание в AOR."""

    is_deleted: bool = Field(
        default=False,
        validation_alias=AliasChoices(*models_limitations["create_aor_request"]["is_deleted"]["validation_alias"]),
        serialization_alias=models_limitations["create_aor_request"]["is_deleted"]["serialization_alias"],
    )
    type: AORCreateRequestTypesEnum = Field(
        validation_alias=AliasChoices(*models_limitations["create_aor_request"]["type"]["validation_alias"]),
        serialization_alias=models_limitations["create_aor_request"]["type"]["serialization_alias"],
    )
    name: str = Field(
        validation_alias=AliasChoices(*models_limitations["create_aor_request"]["name"]["validation_alias"]),
        serialization_alias=models_limitations["create_aor_request"]["name"]["serialization_alias"],
    )
    space_id: Optional[UUID] = Field(
        default=None,
        validation_alias=AliasChoices(*models_limitations["create_aor_request"]["space_id"]["validation_alias"]),
        serialization_alias=models_limitations["create_aor_request"]["space_id"]["serialization_alias"],
    )
    with_parents: bool = Field(
        default=True,
        validation_alias=AliasChoices(*models_limitations["create_aor_request"]["with_parents"]["validation_alias"]),
        serialization_alias=models_limitations["create_aor_request"]["with_parents"]["serialization_alias"],
    )
    name_suffix: str = Field(
        default="",
        validation_alias=AliasChoices(*models_limitations["create_aor_request"]["name_suffix"]["validation_alias"]),
        serialization_alias=models_limitations["create_aor_request"]["name_suffix"]["serialization_alias"],
    )
    parent_name_suffix: str = Field(
        default="",
        validation_alias=AliasChoices(
            *models_limitations["create_aor_request"]["parent_name_suffix"]["validation_alias"]
        ),
        serialization_alias=models_limitations["create_aor_request"]["parent_name_suffix"]["serialization_alias"],
    )
    version_suffix: str = Field(
        default="",
        validation_alias=AliasChoices(*models_limitations["create_aor_request"]["version_suffix"]["validation_alias"]),
        serialization_alias=models_limitations["create_aor_request"]["version_suffix"]["serialization_alias"],
    )
    parent_version_suffix: str = Field(
        default="",
        validation_alias=AliasChoices(
            *models_limitations["create_aor_request"]["parent_version_suffix"]["validation_alias"]
        ),
        serialization_alias=models_limitations["create_aor_request"]["parent_version_suffix"]["serialization_alias"],
    )
    dim_with_attributes: bool = Field(
        default=True,
        validation_alias=AliasChoices(
            *models_limitations["create_model_aor_request"]["dim_with_attributes"]["validation_alias"]
        ),
        serialization_alias=models_limitations["create_model_aor_request"]["dim_with_attributes"][
            "serialization_alias"
        ],
    )
    depends_no_attrs_versions: bool = Field(
        default=False,
        validation_alias=AliasChoices(
            *models_limitations["create_model_aor_request"]["depends_no_attrs_versions"]["validation_alias"]
        ),
        serialization_alias=models_limitations["create_model_aor_request"]["depends_no_attrs_versions"][
            "serialization_alias"
        ],
    )
    model_config = ConfigDict(from_attributes=True)


class CreateModelAorRequest(BaseModel):
    """Модель запроса на создание в AOR."""

    type: Literal[
        AORCreateRequestTypesEnum.MODEL,
        AORCreateRequestTypesEnum.MEASURE,
        AORCreateRequestTypesEnum.DIMENSION,
        AORCreateRequestTypesEnum.DATASTORAGE,
        AORCreateRequestTypesEnum.COMPOSITE,
        AORCreateRequestTypesEnum.DATABASEOBJECT,
    ] = Field(
        validation_alias=AliasChoices(*models_limitations["create_model_aor_request"]["type"]["validation_alias"]),
        serialization_alias=models_limitations["create_model_aor_request"]["type"]["serialization_alias"],
    )
    space_id: Optional[UUID] = Field(
        default=None,
        validation_alias=AliasChoices(*models_limitations["create_model_aor_request"]["space_id"]["validation_alias"]),
        serialization_alias=models_limitations["create_model_aor_request"]["space_id"]["serialization_alias"],
    )
    name_suffix: str = Field(
        default="",
        validation_alias=AliasChoices(
            *models_limitations["create_model_aor_request"]["name_suffix"]["validation_alias"]
        ),
        serialization_alias=models_limitations["create_model_aor_request"]["name_suffix"]["serialization_alias"],
    )
    parent_name_suffix: str = Field(
        default="",
        validation_alias=AliasChoices(
            *models_limitations["create_model_aor_request"]["parent_name_suffix"]["validation_alias"]
        ),
        serialization_alias=models_limitations["create_model_aor_request"]["parent_name_suffix"]["serialization_alias"],
    )
    version_suffix: str = Field(
        default="",
        validation_alias=AliasChoices(
            *models_limitations["create_model_aor_request"]["version_suffix"]["validation_alias"]
        ),
        serialization_alias=models_limitations["create_model_aor_request"]["version_suffix"]["serialization_alias"],
    )
    parent_version_suffix: str = Field(
        default="",
        validation_alias=AliasChoices(
            *models_limitations["create_model_aor_request"]["parent_version_suffix"]["validation_alias"]
        ),
        serialization_alias=models_limitations["create_model_aor_request"]["parent_version_suffix"][
            "serialization_alias"
        ],
    )
    dim_with_attributes: bool = Field(
        default=True,
        validation_alias=AliasChoices(
            *models_limitations["create_model_aor_request"]["dim_with_attributes"]["validation_alias"]
        ),
        serialization_alias=models_limitations["create_model_aor_request"]["dim_with_attributes"][
            "serialization_alias"
        ],
    )
    depends_no_attrs_versions: bool = Field(
        default=False,
        validation_alias=AliasChoices(
            *models_limitations["create_model_aor_request"]["depends_no_attrs_versions"]["validation_alias"]
        ),
        serialization_alias=models_limitations["create_model_aor_request"]["depends_no_attrs_versions"][
            "serialization_alias"
        ],
    )
    with_parents: bool = Field(
        default=True,
        validation_alias=AliasChoices(
            *models_limitations["create_model_aor_request"]["with_parents"]["validation_alias"]
        ),
        serialization_alias=models_limitations["create_model_aor_request"]["with_parents"]["serialization_alias"],
    )
    model_config = ConfigDict(from_attributes=True)
