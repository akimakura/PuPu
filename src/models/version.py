from datetime import datetime
from typing import Optional

from pydantic import AliasChoices, BaseModel, ConfigDict, Field

from src.config import models_limitations


class Versioned(BaseModel):
    version: int = Field(
        le=models_limitations["versioned"]["version"]["max_length"],
        ge=models_limitations["versioned"]["version"]["min_length"],
    )
    timestamp: datetime = Field(
        serialization_alias=models_limitations["versioned"]["timestamp"]["serialization_alias"],
        validation_alias=AliasChoices(*models_limitations["versioned"]["timestamp"]["validation_alias"]),
    )
    user: Optional[str] = Field(
        default=None,
        pattern=models_limitations["versioned"]["user"]["pattern"],
        serialization_alias=models_limitations["versioned"]["user"]["serialization_alias"],
        validation_alias=AliasChoices(*models_limitations["versioned"]["user"]["validation_alias"]),
    )
    model_config = ConfigDict(json_encoders={datetime: lambda v: v.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]})
