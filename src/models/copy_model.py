from enum import StrEnum
from typing import Optional

from pydantic import AliasChoices, BaseModel, Field

from src.config import models_limitations


class ObjectCopyResult(StrEnum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    IGNORED = "IGNORED"


class CopyModelRequest(BaseModel):
    """Request для ручек копирования объекта в модель."""

    models: list[str] = Field(
        serialization_alias=models_limitations["object_copy_request"]["model_names"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["object_copy_request"]["model_names"]["validation_alias"][0],
            models_limitations["object_copy_request"]["model_names"]["validation_alias"][1],
        ),
        description=models_limitations["object_copy_request"]["model_names"]["description"],
        min_length=models_limitations["object_copy_request"]["model_names"]["min_length"],
    )
    objects: list[str] = Field(
        description=models_limitations["object_copy_request"]["objects"]["description"],
        min_length=models_limitations["object_copy_request"]["objects"]["min_length"],
    )


class ObjectCopyResponse(BaseModel):
    """Response для ручек копирования объекта в модель."""

    tenant_id: str = Field(
        serialization_alias=models_limitations["object_copy_response"]["tenant_id"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["object_copy_response"]["tenant_id"]["validation_alias"][0],
            models_limitations["object_copy_response"]["tenant_id"]["validation_alias"][1],
        ),
        description=models_limitations["object_copy_response"]["tenant_id"]["description"],
    )
    object_name: str = Field(
        serialization_alias=models_limitations["object_copy_response"]["object_name"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["object_copy_response"]["object_name"]["validation_alias"][0],
            models_limitations["object_copy_response"]["object_name"]["validation_alias"][1],
        ),
        description=models_limitations["object_copy_response"]["object_name"]["description"],
    )
    models: list[str] = Field(
        serialization_alias=models_limitations["object_copy_response"]["model_names"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["object_copy_response"]["model_names"]["validation_alias"][0],
            models_limitations["object_copy_response"]["model_names"]["validation_alias"][1],
        ),
        description=models_limitations["object_copy_response"]["model_names"]["description"],
        min_length=models_limitations["object_copy_response"]["model_names"]["min_length"],
    )
    result: ObjectCopyResult = Field(
        description=models_limitations["object_copy_response"]["result"]["description"],
    )
    msg: Optional[str] = Field(
        default=None,
        description=models_limitations["object_copy_response"]["comment"]["description"],
    )


class DetailsObjectCopyReponse(BaseModel):
    detail: list[ObjectCopyResponse]


class DetailObjectCopyReponse(BaseModel):
    detail: ObjectCopyResponse
