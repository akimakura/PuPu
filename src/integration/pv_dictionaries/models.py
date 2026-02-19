from enum import StrEnum
from typing import Optional
from urllib.parse import urljoin

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator
from pydantic_core import core_schema

from src.config import models_limitations, settings
from src.models.model import ModelStatusEnum


class PVDictionary(BaseModel):
    """Поля PV Dictionaries"""

    object_name: Optional[str] = Field(
        description=models_limitations["pv_dictionary"]["object_name"]["description"],
        pattern=models_limitations["pv_dictionary"]["object_name"]["pattern"],
        min_length=models_limitations["pv_dictionary"]["object_name"]["min_length"],
        max_length=models_limitations["pv_dictionary"]["object_name"]["max_length"],
        validation_alias=AliasChoices(
            models_limitations["pv_dictionary"]["object_name"]["validation_alias"][0],
            models_limitations["pv_dictionary"]["object_name"]["validation_alias"][1],
        ),
        serialization_alias=models_limitations["pv_dictionary"]["object_name"]["serialization_alias"],
        default=None,
    )
    domain_name: str = Field(
        description=models_limitations["pv_dictionary"]["domain_name"]["description"],
        pattern=models_limitations["pv_dictionary"]["domain_name"]["pattern"],
        min_length=models_limitations["pv_dictionary"]["domain_name"]["min_length"],
        max_length=models_limitations["pv_dictionary"]["domain_name"]["max_length"],
        validation_alias=AliasChoices(
            models_limitations["pv_dictionary"]["domain_name"]["validation_alias"][0],
            models_limitations["pv_dictionary"]["domain_name"]["validation_alias"][1],
        ),
        serialization_alias=models_limitations["pv_dictionary"]["domain_name"]["serialization_alias"],
        default=settings.PV_DICTIONARIES_DEFAULT_DOMAIN_NAME,
    )
    domain_label: str = Field(
        description=models_limitations["pv_dictionary"]["domain_label"]["description"],
        pattern=models_limitations["pv_dictionary"]["domain_label"]["pattern"],
        min_length=models_limitations["pv_dictionary"]["domain_label"]["min_length"],
        max_length=models_limitations["pv_dictionary"]["domain_label"]["max_length"],
        validation_alias=AliasChoices(
            models_limitations["pv_dictionary"]["domain_label"]["validation_alias"][0],
            models_limitations["pv_dictionary"]["domain_label"]["validation_alias"][1],
        ),
        serialization_alias=models_limitations["pv_dictionary"]["domain_label"]["serialization_alias"],
        default=settings.PV_DICTIONARIES_DEFAULT_DOMAIN_LABEL,
    )
    status: ModelStatusEnum = Field(default=ModelStatusEnum.PENDING)
    msg: Optional[str] = Field(
        default=None,
        description=models_limitations["model_status"]["msg"]["description"],
        max_length=models_limitations["model_status"]["msg"]["max_length"],
        min_length=models_limitations["model_status"]["msg"]["min_length"],
    )
    link: Optional[str] = Field(
        default=None,
        validate_default=True,
        description=models_limitations["pv_dictionary"]["link"]["description"],
        validation_alias=AliasChoices(
            models_limitations["pv_dictionary"]["link"]["validation_alias"][0],
            models_limitations["pv_dictionary"]["link"]["validation_alias"][1],
        ),
        serialization_alias=models_limitations["pv_dictionary"]["link"]["serialization_alias"],
    )

    @field_validator("link", mode="after")
    @classmethod
    def get_link(cls, link_value: str | None, all_fields: core_schema.ValidationInfo) -> str | None:
        if link_value:
            return link_value
        url = settings.PV_DICTIONARIES_FRONT_URL or settings.PV_DICTIONARIES_URL
        if not url:
            return None
        url = urljoin(
            url,
            f"{settings.PV_DICTIONARIES_FRONT_GET_DICTIONARY.format(all_fields.data.get('domain_name'))}{all_fields.data.get('object_name')}",
        )
        return url

    model_config = ConfigDict(from_attributes=True)


class PVDictionaryWithoutName(BaseModel):
    """Поля PV Dictionaries без имени словаря."""

    domain_name: str = Field(
        description=models_limitations["pv_dictionary"]["domain_name"]["description"],
        pattern=models_limitations["pv_dictionary"]["domain_name"]["pattern"],
        min_length=models_limitations["pv_dictionary"]["domain_name"]["min_length"],
        max_length=models_limitations["pv_dictionary"]["domain_name"]["max_length"],
        validation_alias=AliasChoices(
            models_limitations["pv_dictionary"]["domain_name"]["validation_alias"][0],
            models_limitations["pv_dictionary"]["domain_name"]["validation_alias"][1],
        ),
        serialization_alias=models_limitations["pv_dictionary"]["domain_name"]["serialization_alias"],
        default=settings.PV_DICTIONARIES_DEFAULT_DOMAIN_NAME,
    )
    domain_label: str = Field(
        description=models_limitations["pv_dictionary"]["domain_label"]["description"],
        pattern=models_limitations["pv_dictionary"]["domain_label"]["pattern"],
        min_length=models_limitations["pv_dictionary"]["domain_label"]["min_length"],
        max_length=models_limitations["pv_dictionary"]["domain_label"]["max_length"],
        validation_alias=AliasChoices(
            models_limitations["pv_dictionary"]["domain_label"]["validation_alias"][0],
            models_limitations["pv_dictionary"]["domain_label"]["validation_alias"][1],
        ),
        serialization_alias=models_limitations["pv_dictionary"]["domain_label"]["serialization_alias"],
        default=settings.PV_DICTIONARIES_DEFAULT_DOMAIN_LABEL,
    )
    model_config = ConfigDict(from_attributes=True)


class PVDictionaryVersion(BaseModel):
    """Поля версия PV Dictionary"""

    object_id: int = Field(exclude=True)
    object_name: str = Field(exclude=True)
    version_code: Optional[str] = Field(
        default=None,
        serialization_alias=models_limitations["pv_dictionary_version"]["version_code"]["serialization_alias"],
    )
    description: str = Field(default="")


class PVLabels(BaseModel):
    """Модель для удобного доступа к лейблам."""

    ru_short: Optional[str] = Field(default=None, description="Короткий русский текст.")
    ru_long: Optional[str] = Field(default=None, description="Длинный русский текст.")
    en_short: Optional[str] = Field(default=None, description="Короткий английский текст.")
    en_long: Optional[str] = Field(default=None, description="Длинный английский текст.")
    short: Optional[str] = Field(default=None, description="Короткий текст.")
    long: Optional[str] = Field(default=None, description="Длинный текст.")
    other: Optional[str] = Field(default=None, description="Любой текст.")


class PVAttributeType(StrEnum):
    TEXT = "TEXT"
    ATTRIBUTE = "ATTRIBUTE"
    DIMENSION_KEY = "DIMENSION_KEY"


class PVDictionaryType(StrEnum):
    DIMENSION = "DIMENSION"


class PVHierarchyPayload(BaseModel):
    """Тело запроса на создание или обновление иерархии в PVD."""

    hierarchy_name: str = Field(serialization_alias="hierarchyName")
    display_name: Optional[str] = Field(default=None, serialization_alias="displayName")
    description: Optional[str] = Field(default=None, serialization_alias="description")
    is_versioned: bool = Field(default=False, serialization_alias="isVersioned")
    is_time_dependent: bool = Field(default=False, serialization_alias="isTimeDependent")
    timedependent_type: Optional[str] = Field(default=None, serialization_alias="timedependentType")
    dictionary_name_list: list[str] = Field(default_factory=list, serialization_alias="dictionaryNameList")


class PVAttribute(BaseModel):
    """Модель атрибутов в PVDictionary"""

    name: str = Field(description="Имя атрибута.")
    label: str = Field(description="Лейбл атрибута.")
    is_key: bool = Field(description="Флаг ключевого атрибута.", default=False)
    type: str = Field(description="Тип атрибута.")
    key: Optional[str] = Field(description="Имя ссылочного ключа атрибута.", default=None)
    dictionary: Optional[str] = Field(description="Имя ссылочного справочника.", default=None)
    description: Optional[str] = Field(description="Описание атрибута.", default=None)
    scale: int = Field(description="Количество знаков после запятой.")
    precision: int = Field(description="Длина атрибута.")
    regex: str = Field(description="Привязанное регулярное выражение.")
    time_dependency: bool = Field(description="Времязависимость", default=False)
    pv_attribute_type: Optional[PVAttributeType] = Field(default=None, description="Семантический тип атрибута")
