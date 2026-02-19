from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, model_validator
from typing_extensions import Annotated

from src.config import models_limitations
from src.models.exceptions import SemanticObjectRelationException
from src.models.label import Label


class Tenant(BaseModel):
    """
    Тенант.
    Схема для чтения из базы и redis.
    """

    name: str = Field(
        description=models_limitations["object_name_32"]["description"],
        min_length=models_limitations["object_name_32"]["min_length"],
        max_length=models_limitations["object_name_32"]["max_length"],
        pattern=models_limitations["object_name_32"]["pattern"],
    )
    labels: list[Label] = Field(
        description=models_limitations["tenant"]["labels"]["description"],
        default=[],
        max_length=models_limitations["tenant"]["labels"]["max_length"],
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
        description=models_limitations["tenant"]["models_names"]["description"],
        max_length=models_limitations["tenant"]["models_names"]["max_length"],
        serialization_alias=models_limitations["tenant"]["models_names"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["tenant"]["models_names"]["validation_alias"][0],
            models_limitations["tenant"]["models_names"]["validation_alias"][1],
        ),
        default=[],
    )

    model_config = ConfigDict(from_attributes=True)

    @model_validator(mode="before")
    @classmethod
    def add_additional_field_to_tenant(cls, tenant_obj: Any) -> Any:
        """
        Добавляет перед валидацией дополнительное поле models_names в валидируемый объект, если это возможно.
        """
        if hasattr(tenant_obj, "models"):
            tenant_obj.models_names = [model.name for model in tenant_obj.models]
        return tenant_obj


class TenantCreateRequest(BaseModel):
    """
    Тенант.
    Схема для записи.
    """

    name: str = Field(
        description=models_limitations["object_name_32"]["description"],
        min_length=models_limitations["object_name_32"]["min_length"],
        max_length=models_limitations["object_name_32"]["max_length"],
        pattern=models_limitations["object_name_32"]["pattern"],
    )
    labels: list[Label] = Field(
        description=models_limitations["tenant"]["labels"]["description"],
        default=[],
        max_length=models_limitations["tenant"]["labels"]["max_length"],
    )
    models: list[
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
        description=models_limitations["tenant"]["models"]["description"],
        max_length=models_limitations["tenant"]["models"]["max_length"],
        default=[],
    )


class TenantEditRequest(BaseModel):
    """
    Тенант.
    Схема для записи.
    """

    labels: Optional[list[Label]] = Field(
        description=models_limitations["tenant"]["labels"]["description"],
        default=None,
        max_length=models_limitations["tenant"]["labels"]["max_length"],
    )
    models: Optional[
        list[
            Annotated[
                str,
                Field(
                    description=models_limitations["object_name_32"]["description"],
                    min_length=models_limitations["object_name_32"]["min_length"],
                    max_length=models_limitations["object_name_32"]["max_length"],
                    pattern=models_limitations["object_name_32"]["pattern"],
                ),
            ]
        ]
    ] = Field(
        description=models_limitations["tenant"]["models"]["description"],
        max_length=models_limitations["tenant"]["models"]["max_length"],
        default=None,
    )


@dataclass
class SemanticObjects:
    """Список имен объектов семантики для обобщенного представления"""

    dimensions: list[str] = field(default_factory=list)
    data_storages: list[str] = field(default_factory=list)
    measures: list[str] = field(default_factory=list)
    composites: list[str] = field(default_factory=list)

    def is_empty(self) -> bool:
        """Проверка на пустоту списка объектов семантики"""
        return (
            not bool(self.dimensions)
            and not bool(self.data_storages)
            and not bool(self.measures)
            and not bool(self.composites)
        )

    def raise_if_not_empty(self) -> None:
        """Выбрасывает ошибку, если список объектов семантики не пуст"""
        if self.is_empty():
            return

        raise SemanticObjectRelationException(self)


@dataclass
class BaseModelData:
    """Основные данные по dimension, measure, data_storage, composite и имя модели, к которой относится."""

    id: int
    name: str
    model_name: str


class SemanticObjectsTypeEnum(str, Enum):
    """Типы объектов семантики"""

    DIMENSION = "DIMENSION"
    MEASURE = "MEASURE"
    DATA_STORAGE = "DATA_STORAGE"
    COMPOSITE = "COMPOSITE"
    DATABASE_OBJECT = "DATABASE_OBJECT"
    HIERARCHY = "HIERARCHY"
    PV_DICTIONARY = "PV_DICTIONARY"

    class Config:
        arbitrary_types_allowed = True


class TenantObjectSearchRequest(BaseModel):
    """Параметры запроса для поиска по объектам семантики"""

    query: str = Field(
        description=models_limitations["tenant"]["search"]["description"],
        default=None,
        max_length=models_limitations["tenant"]["search"]["max_length"],
    )
    object_type: Optional[SemanticObjectsTypeEnum] = Field(
        alias="type",
        description=models_limitations["tenant"]["type"]["description"],
        default=None,
        max_length=models_limitations["tenant"]["type"]["max_length"],
    )
    model: Optional[str] = Field(
        alias="modelName",
        description=models_limitations["tenant"]["modelName"]["description"],
        default=None,
        max_length=models_limitations["tenant"]["modelName"]["max_length"],
    )


class TenantSearchObjectResponse(BaseModel):
    """Объект, возвращаемый после поиска объектов семантики"""

    results: dict

    model_config = ConfigDict(from_attributes=True)


class FindWhereUsedRequest(BaseModel):
    """Параметры запроса для поиска объектов семантики, связанных с объектом"""

    object_type: Optional[SemanticObjectsTypeEnum] = Field(
        alias="type",
        description=models_limitations["tenant"]["type"]["description"],
        max_length=models_limitations["tenant"]["type"]["max_length"],
    )
