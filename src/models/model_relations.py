from typing import Optional

from pydantic import AliasChoices, BaseModel, Field

from src.config import models_limitations
from src.models.model import ModelStatusEnum
from src.models.tenant import SemanticObjectsTypeEnum


class ChangeObjectStatusRequest(BaseModel):
    schema_name: Optional[str] = Field(
        default=None,
        description=models_limitations["change_status_query"]["schema_name"]["description"],
        min_length=models_limitations["change_status_query"]["schema_name"]["min_length"],
        max_length=models_limitations["change_status_query"]["schema_name"]["max_length"],
        pattern=models_limitations["change_status_query"]["schema_name"]["pattern"],
        serialization_alias=models_limitations["change_status_query"]["schema_name"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["change_status_query"]["schema_name"]["validation_alias"][0],
            models_limitations["change_status_query"]["schema_name"]["validation_alias"][1],
        ),
    )
    model: str = Field(
        min_length=models_limitations["object_name_32"]["min_length"],
        max_length=models_limitations["object_name_32"]["max_length"],
        pattern=models_limitations["object_name_32"]["pattern"],
        serialization_alias=models_limitations["change_status_query"]["model"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["change_status_query"]["model"]["validation_alias"][0],
            models_limitations["change_status_query"]["model"]["validation_alias"][1],
        ),
    )
    object_name: str = Field(
        min_length=models_limitations["object_name_64"]["min_length"],
        max_length=models_limitations["object_name_64"]["max_length"],
        pattern=models_limitations["object_name_64"]["pattern"],
        serialization_alias=models_limitations["change_status_query"]["object_name"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["change_status_query"]["object_name"]["validation_alias"][0],
            models_limitations["change_status_query"]["object_name"]["validation_alias"][1],
        ),
    )
    object_type: SemanticObjectsTypeEnum = Field(
        serialization_alias=models_limitations["change_status_query"]["object_type"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["change_status_query"]["object_type"]["validation_alias"][0],
            models_limitations["change_status_query"]["object_type"]["validation_alias"][1],
        ),
    )
    status: ModelStatusEnum = Field()
    msg: Optional[str] = Field(
        default=None,
        max_length=models_limitations["model_status"]["msg"]["max_length"],
        min_length=models_limitations["model_status"]["msg"]["min_length"],
        description=models_limitations["model_status"]["msg"]["description"],
    )


class ChangeObjectStatusResponse(BaseModel):
    schema_name: Optional[str] = Field(
        default=None,
        description=models_limitations["change_status_query"]["schema_name"]["description"],
        min_length=models_limitations["change_status_query"]["schema_name"]["min_length"],
        max_length=models_limitations["change_status_query"]["schema_name"]["max_length"],
        pattern=models_limitations["change_status_query"]["schema_name"]["pattern"],
        serialization_alias=models_limitations["change_status_query"]["schema_name"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["change_status_query"]["schema_name"]["validation_alias"][0],
            models_limitations["change_status_query"]["schema_name"]["validation_alias"][1],
        ),
    )
    model: str = Field(
        min_length=models_limitations["object_name_32"]["min_length"],
        max_length=models_limitations["object_name_32"]["max_length"],
        pattern=models_limitations["object_name_32"]["pattern"],
        serialization_alias=models_limitations["change_status_query"]["model"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["change_status_query"]["model"]["validation_alias"][0],
            models_limitations["change_status_query"]["model"]["validation_alias"][1],
        ),
    )
    object_name: str = Field(
        min_length=models_limitations["object_name_64"]["min_length"],
        max_length=models_limitations["object_name_64"]["max_length"],
        pattern=models_limitations["object_name_64"]["pattern"],
        serialization_alias=models_limitations["change_status_query"]["object_name"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["change_status_query"]["object_name"]["validation_alias"][0],
            models_limitations["change_status_query"]["object_name"]["validation_alias"][1],
        ),
    )
    object_type: SemanticObjectsTypeEnum = Field(
        serialization_alias=models_limitations["change_status_query"]["object_type"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["change_status_query"]["object_type"]["validation_alias"][0],
            models_limitations["change_status_query"]["object_type"]["validation_alias"][1],
        ),
    )
    status: ModelStatusEnum = Field()
    msg: Optional[str] = Field(
        default=None,
        max_length=models_limitations["model_status"]["msg"]["max_length"],
        min_length=models_limitations["model_status"]["msg"]["min_length"],
        description=models_limitations["model_status"]["msg"]["description"],
    )
    updated: bool = Field(default=True)
