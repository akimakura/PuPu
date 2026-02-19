from enum import StrEnum
from typing import Optional

from pydantic import BaseModel


class SortDirectionEnum(StrEnum):
    desc = "desc"
    asc = "asc"


class ContentTypeHeaderEnum(StrEnum):
    XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    CSV = "text/csv"


class Pagination(BaseModel):
    """Модель пагинации."""

    offset: Optional[int] = None
    limit: Optional[int] = None
    sort_direction: SortDirectionEnum = SortDirectionEnum.asc


class DimensionsFileColumnEnum(StrEnum):
    """Колонки для импорта Dimensions из файла."""

    NAME = "Имя"
    SHORT_LABEL = "Короткое описание"
    LONG_LABEL = "Длинное описание"
    REF = "Ссылочное измерение"
    DATA_TYPE = "Тип данных"
    LENGTH = "Длина"
    VIRTUAL = "Виртуальный (без основных данных)"
    SHORT_TEXT = "Тексты SHORT"
    MEDIUM_TEXT = "Тексты MEDIUM"
    LONG_TEXT = "Тексты LONG"
    TEXT_TIME_DEPENDENCY = "Тексты зависят от времени"
    TEXT_LANG_DEPENDENCY = "Тексты зависят от языка"
    AUTH_RELEVANT = "Проверка полномочий на значения"
    CASE_SENSITIVE = "Регистрозависимость значений"


class DimensionAttributesFileColumnEnum(StrEnum):
    """Колонки для импорта атрибутов Dimension из файла."""

    DIMENSION_NAME = "Имя измерения, для которого создаётся атрибут"
    NAME = "Имя атрибута"
    TIME_DEPENDENCY = "Атрибут зависит от времени"
    SHORT_LABEL = "Короткое описание"
    LONG_LABEL = "Длинное описание"
    SEMANTIC_TYPE = "Семантический тип (DIMENSION или MEASURE)"
    REF = "Имя объекта, на который ссылается атрибут (DIMENSION или MEASURE)"
    DATA_TYPE = "Тип данных"
    LENGTH = "Длина (или количество знаков до запятой)"
    SCALE = "Количество знаков после запятой"
    AGGREGATION_TYPE = "Тип суммирования по времени (SUMMATION / MAXIMUM / MINIMUM / NO AGGREGATION)"


class DataStorageFileColumnEnum(StrEnum):
    """Колонки для импорта DataStorage из файла."""

    NAME = "Имя"
    SHORT_LABEL = "Короткое описание"
    LONG_LABEL = "Длинное описание"
    PLAN = "Актуально для планирования"
    TYPE = "Тип"


class DataStorageFieldsFileColumnEnum(StrEnum):
    """Колонки для импорта полей DataStorage из файла."""

    DATA_STORAGE_NAME = "Имя хранилища данных, для которого создаётся поле"
    FIELD_NAME = "Имя поля"
    KEY = "Ключевое поле"
    SHARDING_KEY = "Ключ шардирования"
    SHORT_LABEL = "Короткое описание"
    LONG_LABEL = "Длинное описание"
    SEMANTIC_TYPE = "Семантический тип"
    REF = "Имя объекта, на который ссылается поле"
    DATA_TYPE = "Тип данных"
    LENGTH = "Длина (или количество знаков до запятой)"
    SCALE = "Количество знаков после запятой"
    AGGREGATION_TYPE = "Тип суммирования по времени"
