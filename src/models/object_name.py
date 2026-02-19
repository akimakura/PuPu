from pydantic import BaseModel, ConfigDict, Field

from src.config import models_limitations


class ObjectName(BaseModel):
    """Модель, используемая для получения имени из ссылки на объект"""

    name: str = Field(
        description=models_limitations["object_name"]["name"]["description"],
        min_length=models_limitations["object_name"]["name"]["min_length"],
        pattern=models_limitations["object_name"]["name"]["pattern"],
        max_length=models_limitations["object_name"]["name"]["max_length"],
    )

    model_config = ConfigDict(from_attributes=True)
