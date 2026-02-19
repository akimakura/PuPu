"""Схемы pydantic для описания текстов"""

from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from src.config import models_limitations


class LabelType(StrEnum):
    SHORT = "SHORT"
    MEDIUM = "MEDIUM"
    LONG = "LONG"


class Language(StrEnum):
    RU = "ru-ru"
    EN = "en-en"


class Label(BaseModel):
    """Тексты, которые описывают различные сущности системы на разных языках(признаки, атрибуты, показатели, dso и тд)."""

    language: str = Field(
        description=models_limitations["label"]["language"]["description"],
        max_length=models_limitations["label"]["language"]["max_length"],
        pattern=models_limitations["label"]["language"]["pattern"],
        examples=models_limitations["label"]["language"]["examples"],
    )
    type: Literal[LabelType.SHORT, LabelType.LONG] = Field(
        description=models_limitations["label"]["type"]["description"],
        max_length=models_limitations["label"]["type"]["max_length"],
    )
    text: str = Field(
        description=models_limitations["label"]["text"]["description"],
        max_length=models_limitations["label"]["text"]["max_length"],
        min_length=models_limitations["label"]["text"]["min_length"],
        pattern=models_limitations["label"]["text"]["pattern"],
    )
    model_config = ConfigDict(from_attributes=True, use_enum_values=True)
