from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any, Optional
from uuid import UUID

from pydantic import AliasChoices, BaseModel, Field

from src.config import settings


class AorKafkaObjectParent(BaseModel):
    parent_type: str = Field(description="Тип родительского объекта.")
    parent_name: str = Field(description="Имя родительского объекта.")
    parent_version: str = Field(description="Версия родительского объекта.")
    parent_external_id: str = Field(description="ИД родительской версии объекта.")
    service_id: Optional[UUID] = settings.AOR_SERVICE_UUID


class AorType(StrEnum):
    MODEL = "sl_model"
    HIERARCHY = "sl_hierarchy"
    DIMENSION = "sl_dimension"
    MEASURE = "sl_measure"
    COMPOSITE = "sl_composite"
    DATABASEOBJECT = "sl_databaseobject"
    DATASTORAGE = "sl_datastorage"
    DATABASE = "sl_database"
    AIGROUP = "sl_aigroup"


class JsonData(BaseModel):
    is_deleted: bool = Field(
        default=False,
        validation_alias=AliasChoices("isDeleted", "is_deleted"),
        serialization_alias="isDeleted",
        description="Флаг, указывающий на удаление объекта.",
    )
    tenant: str = Field(description="Идентификатор тенанта.")
    data_json: dict[str, Any] = Field(
        default={},
        validation_alias=AliasChoices("objectJson", "data_json"),
        serialization_alias="objectJson",
    )


class CreateAorCommand(BaseModel):
    """
    Create aor command.
    """

    type: AorType = Field(description="Тип объекта.")
    name: Annotated[str, Field(min_length=1, strict=True)] = Field(description="Имя объекта (техническое).")
    version: Annotated[str, Field(min_length=1, strict=True)] = Field(description="Версия объекта.")
    description: Annotated[str, Field(min_length=1, strict=True)] = Field(
        description="Описание объекта. Представляет собой объект с сообщением приветствия."
    )
    space_id: UUID = Field(
        default=settings.AOR_SEMANTIAC_SPACE_UUID, description="Прикладная область (идентификатор пространства)."
    )
    service_id: UUID = Field(
        default=settings.AOR_SERVICE_UUID, description="Идентификатор сервиса, к которому принадлежит объект."
    )
    data_json: JsonData = Field(description="Данные, которые нужно мигрировать между проектами.")
    deployed_by: Annotated[str, Field(min_length=1, strict=True)] = Field(
        description="Логин пользователя из токена авторизации."
    )
    external_object_id: str = Field(
        description="Внешний идентификатор объекта (externalObjectId, сгенерированный UUID)."
    )
    parents: Optional[list[AorKafkaObjectParent]] = None


class PushAorCommand(CreateAorCommand):
    deployed_at: datetime = Field(
        ...,
        description="Дата и время деплоя (текущее время).",
        examples=["2025-04-01T07:20:37.402402224Z"],
    )
