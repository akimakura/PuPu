from pydantic import BaseModel, Field


class ImportFromFileResponse(BaseModel):
    """Возвращаемое значение для ручек массового импорта."""

    created: list[str] = Field(default=[])
    not_created: list[str] = Field(default=[])
    updated: list[str] = Field(default=[])
    not_updated: list[str] = Field(default=[])
