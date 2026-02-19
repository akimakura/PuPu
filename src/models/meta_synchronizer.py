from enum import StrEnum
from typing import Optional

from pydantic import AliasChoices, BaseModel, Field

from src.config import models_limitations


class GeneratorResult(StrEnum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    IGNORED = "IGNORED"


class MetaSynchronizerResponse(BaseModel):
    """
    Результат генерации синхронизатора.
    """

    tenant_id: str = Field(
        serialization_alias=models_limitations["generated_object"]["tenant_id"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["generated_object"]["tenant_id"]["validation_alias"][0],
            models_limitations["generated_object"]["tenant_id"]["validation_alias"][1],
        ),
        description=models_limitations["generated_object"]["tenant_id"]["description"],
    )
    model: str = Field(
        serialization_alias=models_limitations["generated_object"]["model"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["generated_object"]["model"]["validation_alias"][0],
            models_limitations["generated_object"]["model"]["validation_alias"][1],
        ),
        description=models_limitations["generated_object"]["model"]["description"],
    )
    object_name: str = Field(
        serialization_alias=models_limitations["generated_object"]["object_name"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["generated_object"]["object_name"]["validation_alias"][0],
            models_limitations["generated_object"]["object_name"]["validation_alias"][1],
        ),
        description=models_limitations["generated_object"]["object_name"]["description"],
    )
    msg: Optional[str] = Field(
        default=None,
        description=models_limitations["generated_object"]["comment"]["description"],
    )
    result: GeneratorResult = Field(description=models_limitations["generated_object"]["result"]["description"])


class DetailsMetaSynchronizerResponse(BaseModel):
    detail: list[MetaSynchronizerResponse]


class DetailMetaSynchronizerResponse(BaseModel):
    detail: MetaSynchronizerResponse
