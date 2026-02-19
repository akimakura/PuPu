"""Pydantic схемы для объектов базы данных из внешней системы."""

from enum import StrEnum
from typing import Optional

from pydantic import AliasChoices, BaseModel, ConfigDict, Field

from src.config import models_limitations
from src.models.model import Model


class DbObjectTypeEnum(StrEnum):
    """
    Перечисление типов объектов базы данных.

    Описывает возможные типы объектов, с которыми может работать система.
    """

    VIEW = "VIEW"
    TABLE = "TABLE"
    REPLICATED_TABLE = "REPLICATED_TABLE"
    DISTRIBUTED_TABLE = "DISTRIBUTED_TABLE"
    DICTIONARY = "DICTIONARY"


class DatabaseObjectRelationTypeEnum(StrEnum):
    """Тип связи database_object с семантическим объектом."""

    PARENT = "PARENT"


class SpecificAttributeNameEnum(StrEnum):
    SHARDING_KEY = "SHARDING_KEY"


class DataBaseObjectSpecificAttribute(BaseModel):
    """Атрибуты объекта базы данных специфические для системы"""

    name: str = Field(
        description=models_limitations["object_specific_attribute"]["name"]["description"],
        pattern=models_limitations["object_specific_attribute"]["name"]["pattern"],
        min_length=models_limitations["object_specific_attribute"]["name"]["min_length"],
        max_length=models_limitations["object_specific_attribute"]["name"]["max_length"],
    )
    value: str = Field(
        description=models_limitations["object_specific_attribute"]["value"]["description"],
        pattern=models_limitations["object_specific_attribute"]["value"]["pattern"],
        min_length=models_limitations["object_specific_attribute"]["value"]["min_length"],
        max_length=models_limitations["object_specific_attribute"]["value"]["max_length"],
    )
    model_config = ConfigDict(from_attributes=True)


class DatabaseObject(BaseModel):
    """
    Схема для чтения объектов базы данных из бд или кэша.
    Описание технического объекта базы данных внешней системы.
    """

    models: Optional[list[Model]] = Field(exclude=True, default=None)
    tenant_id: Optional[str] = Field(exclude=True, default=None)
    schema_name: Optional[str] = Field(
        description=models_limitations["db_object"]["schema_name"]["description"],
        serialization_alias=models_limitations["db_object"]["schema_name"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["db_object"]["schema_name"]["validation_alias"][0],
            models_limitations["db_object"]["schema_name"]["validation_alias"][1],
        ),
        pattern=models_limitations["db_object"]["schema_name"]["pattern"],
        min_length=models_limitations["db_object"]["schema_name"]["min_length"],
        max_length=models_limitations["db_object"]["schema_name"]["max_length"],
        default=None,
    )
    type: DbObjectTypeEnum = Field(
        description=models_limitations["db_object"]["type"]["description"],
        serialization_alias=models_limitations["db_object"]["type"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["db_object"]["type"]["validation_alias"][0],
            models_limitations["db_object"]["type"]["validation_alias"][1],
        ),
    )
    name: str = Field(
        description=models_limitations["db_object"]["name"]["description"],
        pattern=models_limitations["db_object"]["name"]["pattern"],
        min_length=models_limitations["db_object"]["name"]["min_length"],
        max_length=models_limitations["db_object"]["name"]["max_length"],
    )
    specific_attributes: list[DataBaseObjectSpecificAttribute] = Field(
        description=models_limitations["db_object"]["specific_attributes"]["description"],
        serialization_alias=models_limitations["db_object"]["specific_attributes"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["db_object"]["specific_attributes"]["validation_alias"][0],
            models_limitations["db_object"]["specific_attributes"]["validation_alias"][1],
        ),
        max_length=models_limitations["db_object"]["specific_attributes"]["max_length"],
        default=[],
    )
    model_config = ConfigDict(from_attributes=True)

    def __repr__(self) -> str:
        return f"{self.schema_name}.{self.name}"


class DatabaseObjectRequest(BaseModel):
    """
    Схема для запроса на получение dso по database_object.
    Описание технического объекта базы данных внешней системы.
    """

    schema_name: str = Field(
        description=models_limitations["db_object"]["schema_name"]["description"],
        serialization_alias=models_limitations["db_object"]["schema_name"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["db_object"]["schema_name"]["validation_alias"][0],
            models_limitations["db_object"]["schema_name"]["validation_alias"][1],
        ),
        pattern=models_limitations["db_object"]["schema_name"]["pattern"],
        min_length=models_limitations["db_object"]["schema_name"]["min_length"],
        max_length=models_limitations["db_object"]["schema_name"]["max_length"],
    )
    name: str = Field(
        description=models_limitations["db_object"]["name"]["description"],
        pattern=models_limitations["db_object"]["name"]["pattern"],
        min_length=models_limitations["db_object"]["name"]["min_length"],
        max_length=models_limitations["db_object"]["name"]["max_length"],
    )
    type: Optional[DbObjectTypeEnum] = Field(
        description=models_limitations["db_object"]["type"]["description"],
        serialization_alias=models_limitations["db_object"]["type"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["db_object"]["type"]["validation_alias"][0],
            models_limitations["db_object"]["type"]["validation_alias"][1],
        ),
        default=None,
    )

    specific_attributes: Optional[list[DataBaseObjectSpecificAttribute]] = Field(
        description=models_limitations["db_object"]["specific_attributes"]["description"],
        serialization_alias=models_limitations["db_object"]["specific_attributes"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["db_object"]["specific_attributes"]["validation_alias"][0],
            models_limitations["db_object"]["specific_attributes"]["validation_alias"][1],
        ),
        max_length=models_limitations["db_object"]["specific_attributes"]["max_length"],
        default=None,
    )

    model_config = ConfigDict(from_attributes=True)


class DatabaseObjectNames(BaseModel):
    """
    Имена и схемы объектов базы данных для конкретного DataStorage.
    Модель нужна для удобного внутреннего представления.
    """

    table_name: Optional[str] = Field(default=None, description="имя dbObject с типом 'TABLE'.")
    table_schema: Optional[str] = Field(default=None, description="схема dbObject с типом 'TABLE'.")
    distributed_name: Optional[str] = Field(default=None, description="имя dbObject с типом 'DISTRIBUTED_TABLE'.")
    distributed_schema: Optional[str] = Field(default=None, description="схема dbObject с типом 'DISTRIBUTED_TABLE'.")
    dictionary_name: Optional[str] = Field(default=None, description="имя dbObject с типом 'DICTIONARY'.")
    dictionary_schema: Optional[str] = Field(default=None, description="схема dbObject с типом 'DICTIONARY'.")
    type: DbObjectTypeEnum = Field(description="Тип объекта.")
