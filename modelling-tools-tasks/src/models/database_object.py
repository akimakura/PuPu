from enum import StrEnum
from typing import Optional

from pydantic import BaseModel, Field

from src.integrations.modelling_tools_api.codegen import DbObject


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


class DatabaseObjectGenerationResult(BaseModel):
    table: DbObject
    sql_expression: Optional[str] = None
    executed: bool = False
    error: Optional[str] = None
