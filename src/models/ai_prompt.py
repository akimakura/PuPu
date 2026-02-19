import json
from typing import Any, Optional

from pydantic import AliasChoices, BaseModel, BeforeValidator, ConfigDict, Field, field_validator, model_validator
from pydantic_core import core_schema
from typing_extensions import Annotated

from src.config import models_limitations


def json_to_list(value: str | list) -> list:
    """
    Конвертирует строку в формате json в питоновский список
    или просто возвращает список, если была передана не строка.
    """
    if isinstance(value, str):
        return json.loads(value)
    return value


class GroupDescription(BaseModel):
    """Промпты с описанием группы справочника для LLM."""

    entity_name: str = Field(
        description=models_limitations["group_description"]["entity_name"]["description"],
        validation_alias=AliasChoices(
            models_limitations["group_description"]["entity_name"]["validation_alias"][0],
            models_limitations["group_description"]["entity_name"]["validation_alias"][1],
        ),
        serialization_alias=models_limitations["group_description"]["entity_name"]["serialization_alias"],
        min_length=models_limitations["group_description"]["entity_name"]["min_length"],
        max_length=models_limitations["group_description"]["entity_name"]["max_length"],
        pattern=models_limitations["group_description"]["entity_name"]["pattern"],
    )
    description: Optional[str] = Field(
        default=None,
        description=models_limitations["group_description"]["description"]["description"],
        validation_alias=AliasChoices(
            models_limitations["group_description"]["description"]["validation_alias"][0],
            models_limitations["group_description"]["description"]["validation_alias"][1],
        ),
        min_length=models_limitations["group_description"]["description"]["min_length"],
        max_length=models_limitations["group_description"]["description"]["max_length"],
        pattern=models_limitations["group_description"]["description"]["pattern"],
    )
    synonyms: Optional[str] = Field(
        default=None,
        min_length=models_limitations["group_description"]["synonyms"]["min_length"],
        max_length=models_limitations["group_description"]["synonyms"]["max_length"],
        pattern=models_limitations["group_description"]["synonyms"]["pattern"],
        validation_alias=AliasChoices(
            models_limitations["group_description"]["synonyms"]["validation_alias"][0],
            models_limitations["group_description"]["synonyms"]["validation_alias"][1],
        ),
    )
    few_shots: Optional[str] = Field(
        default=None,
        min_length=models_limitations["group_description"]["few_shots"]["min_length"],
        max_length=models_limitations["group_description"]["few_shots"]["max_length"],
        pattern=models_limitations["group_description"]["few_shots"]["pattern"],
        validation_alias=AliasChoices(
            models_limitations["group_description"]["few_shots"]["validation_alias"][0],
            models_limitations["group_description"]["few_shots"]["validation_alias"][1],
        ),
        serialization_alias=models_limitations["group_description"]["few_shots"]["serialization_alias"],
    )

    model_config = ConfigDict(from_attributes=True)


class AnalyticsDescription(BaseModel):
    """
    Дополнительные поля для описания AiPrompt
    """

    entity_name: Optional[str] = Field(
        default=None,
        description=models_limitations["analytics_description"]["entity_name"]["description"],
        pattern=models_limitations["analytics_description"]["entity_name"]["pattern"],
        min_length=models_limitations["analytics_description"]["entity_name"]["min_length"],
        max_length=models_limitations["analytics_description"]["entity_name"]["max_length"],
        validation_alias=AliasChoices(
            models_limitations["analytics_description"]["entity_name"]["validation_alias"][0],
            models_limitations["analytics_description"]["entity_name"]["validation_alias"][1],
        ),
        serialization_alias=models_limitations["analytics_description"]["entity_name"]["serialization_alias"],
    )
    description: Optional[str] = Field(
        default=None,
        pattern=models_limitations["analytics_description"]["description"]["pattern"],
        description=models_limitations["analytics_description"]["description"]["description"],
        min_length=models_limitations["analytics_description"]["description"]["min_length"],
        max_length=models_limitations["analytics_description"]["description"]["max_length"],
        validation_alias=AliasChoices(
            models_limitations["analytics_description"]["description"]["validation_alias"][0],
            models_limitations["analytics_description"]["description"]["validation_alias"][1],
        ),
        serialization_alias=models_limitations["analytics_description"]["description"]["serialization_alias"],
    )
    synonyms: Optional[str] = Field(
        default=None,
        pattern=models_limitations["analytics_description"]["synonyms"]["pattern"],
        min_length=models_limitations["analytics_description"]["synonyms"]["min_length"],
        max_length=models_limitations["analytics_description"]["synonyms"]["max_length"],
        validation_alias=AliasChoices(
            models_limitations["analytics_description"]["synonyms"]["validation_alias"][0],
            models_limitations["analytics_description"]["synonyms"]["validation_alias"][1],
        ),
        serialization_alias=models_limitations["analytics_description"]["synonyms"]["serialization_alias"],
    )
    few_shots: Optional[str] = Field(
        default=None,
        pattern=models_limitations["analytics_description"]["few_shots"]["pattern"],
        min_length=models_limitations["analytics_description"]["few_shots"]["min_length"],
        max_length=models_limitations["analytics_description"]["few_shots"]["max_length"],
        validation_alias=AliasChoices(
            models_limitations["analytics_description"]["few_shots"]["validation_alias"][0],
            models_limitations["analytics_description"]["few_shots"]["validation_alias"][1],
        ),
        serialization_alias=models_limitations["analytics_description"]["few_shots"]["serialization_alias"],
    )


class AIPrompt(BaseModel):
    """Промпты с описанием справочников для LLM."""

    analytic_role: Annotated[
        list[
            Annotated[
                str,
                Field(
                    min_length=models_limitations["ai_prompts"]["analytic_role"]["min_length"],
                    max_length=models_limitations["ai_prompts"]["analytic_role"]["max_length"],
                    pattern=models_limitations["ai_prompts"]["analytic_role"]["pattern"],
                ),
            ]
        ],
        BeforeValidator(json_to_list),
    ] = Field(
        description=models_limitations["ai_prompts"]["analytic_role"]["description"],
        serialization_alias=models_limitations["ai_prompts"]["analytic_role"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["ai_prompts"]["analytic_role"]["validation_alias"][0],
            models_limitations["ai_prompts"]["analytic_role"]["validation_alias"][1],
        ),
        min_length=models_limitations["ai_prompts"]["analytic_role"]["min_items"],
        max_length=models_limitations["ai_prompts"]["analytic_role"]["max_items"],
    )
    purpose: Annotated[
        list[
            Annotated[
                str,
                Field(
                    min_length=models_limitations["ai_prompts"]["purpose"]["min_length"],
                    max_length=models_limitations["ai_prompts"]["purpose"]["max_length"],
                    pattern=models_limitations["ai_prompts"]["purpose"]["pattern"],
                ),
            ]
        ],
        BeforeValidator(json_to_list),
    ] = Field(
        description=models_limitations["ai_prompts"]["purpose"]["description"],
        min_length=models_limitations["ai_prompts"]["purpose"]["min_items"],
        max_length=models_limitations["ai_prompts"]["purpose"]["max_items"],
    )
    key_features: Annotated[
        list[
            Annotated[
                str,
                Field(
                    min_length=models_limitations["ai_prompts"]["key_features"]["min_length"],
                    max_length=models_limitations["ai_prompts"]["key_features"]["max_length"],
                    pattern=models_limitations["ai_prompts"]["key_features"]["pattern"],
                ),
            ]
        ],
        BeforeValidator(json_to_list),
    ] = Field(
        description=models_limitations["ai_prompts"]["key_features"]["description"],
        serialization_alias=models_limitations["ai_prompts"]["key_features"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["ai_prompts"]["key_features"]["validation_alias"][0],
            models_limitations["ai_prompts"]["key_features"]["validation_alias"][1],
        ),
        min_length=models_limitations["ai_prompts"]["key_features"]["min_items"],
        max_length=models_limitations["ai_prompts"]["key_features"]["max_items"],
    )
    data_type: Annotated[
        list[
            Annotated[
                str,
                Field(
                    min_length=models_limitations["ai_prompts"]["data_type"]["min_length"],
                    max_length=models_limitations["ai_prompts"]["data_type"]["max_length"],
                    pattern=models_limitations["ai_prompts"]["data_type"]["pattern"],
                ),
            ]
        ],
        BeforeValidator(json_to_list),
    ] = Field(
        description=models_limitations["ai_prompts"]["data_type"]["description"],
        serialization_alias=models_limitations["ai_prompts"]["data_type"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["ai_prompts"]["data_type"]["validation_alias"][0],
            models_limitations["ai_prompts"]["data_type"]["validation_alias"][1],
        ),
        min_length=models_limitations["ai_prompts"]["data_type"]["min_items"],
        max_length=models_limitations["ai_prompts"]["data_type"]["max_items"],
    )
    subject_area: Annotated[
        list[
            Annotated[
                str,
                Field(
                    min_length=models_limitations["ai_prompts"]["subject_area"]["min_length"],
                    max_length=models_limitations["ai_prompts"]["subject_area"]["max_length"],
                    pattern=models_limitations["ai_prompts"]["subject_area"]["pattern"],
                ),
            ]
        ],
        BeforeValidator(json_to_list),
    ] = Field(
        description=models_limitations["ai_prompts"]["subject_area"]["description"],
        serialization_alias=models_limitations["ai_prompts"]["subject_area"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["ai_prompts"]["subject_area"]["validation_alias"][0],
            models_limitations["ai_prompts"]["subject_area"]["validation_alias"][1],
        ),
        min_length=models_limitations["ai_prompts"]["subject_area"]["min_items"],
        max_length=models_limitations["ai_prompts"]["subject_area"]["max_items"],
    )

    example_questions: Annotated[
        list[
            Annotated[
                str,
                Field(
                    min_length=models_limitations["ai_prompts"]["example_questions"]["min_length"],
                    max_length=models_limitations["ai_prompts"]["example_questions"]["max_length"],
                    pattern=models_limitations["ai_prompts"]["example_questions"]["pattern"],
                ),
            ]
        ],
        BeforeValidator(json_to_list),
    ] = Field(
        description=models_limitations["ai_prompts"]["example_questions"]["description"],
        serialization_alias=models_limitations["ai_prompts"]["example_questions"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["ai_prompts"]["example_questions"]["validation_alias"][0],
            models_limitations["ai_prompts"]["example_questions"]["validation_alias"][1],
        ),
        min_length=models_limitations["ai_prompts"]["example_questions"]["min_items"],
        max_length=models_limitations["ai_prompts"]["example_questions"]["max_items"],
    )
    markers: Annotated[
        list[
            Annotated[
                str,
                Field(
                    min_length=models_limitations["ai_prompts"]["markers"]["min_length"],
                    max_length=models_limitations["ai_prompts"]["markers"]["max_length"],
                    pattern=models_limitations["ai_prompts"]["markers"]["pattern"],
                ),
            ]
        ],
        BeforeValidator(json_to_list),
    ] = Field(
        description=models_limitations["ai_prompts"]["markers"]["description"],
        min_length=models_limitations["ai_prompts"]["markers"]["min_items"],
        max_length=models_limitations["ai_prompts"]["markers"]["max_items"],
    )
    notes: Annotated[
        list[
            Annotated[
                str,
                Field(
                    min_length=models_limitations["ai_prompts"]["notes"]["min_length"],
                    max_length=models_limitations["ai_prompts"]["notes"]["max_length"],
                    pattern=models_limitations["ai_prompts"]["notes"]["pattern"],
                ),
            ]
        ],
        BeforeValidator(json_to_list),
    ] = Field(
        description=models_limitations["ai_prompts"]["notes"]["description"],
        min_length=models_limitations["ai_prompts"]["notes"]["min_items"],
        max_length=models_limitations["ai_prompts"]["notes"]["max_items"],
    )
    related_dimensions: list[
        Annotated[
            str,
            Field(
                min_length=models_limitations["object_name_32"]["min_length"],
                max_length=models_limitations["object_name_32"]["max_length"],
                pattern=models_limitations["object_name_32"]["pattern"],
            ),
        ]
    ] = Field(
        default=[],
        validate_default=True,
        description=models_limitations["ai_prompts"]["related_dimensions"]["description"],
        min_length=models_limitations["ai_prompts"]["related_dimensions"]["min_items"],
        max_length=models_limitations["ai_prompts"]["related_dimensions"]["max_items"],
        serialization_alias=models_limitations["ai_prompts"]["related_dimensions"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["ai_prompts"]["related_dimensions"]["validation_alias"][0],
            models_limitations["ai_prompts"]["related_dimensions"]["validation_alias"][1],
        ),
    )
    ai_usage: bool = Field(
        default=False,
        description=models_limitations["ai_prompts"]["ai_usage"]["description"],
        serialization_alias=models_limitations["ai_prompts"]["ai_usage"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["ai_prompts"]["ai_usage"]["validation_alias"][0],
            models_limitations["ai_prompts"]["ai_usage"]["validation_alias"][1],
        ),
    )
    domain_id: Optional[int] = Field(
        default=None,
        description=models_limitations["ai_prompts"]["domain_id"]["description"],
        serialization_alias=models_limitations["ai_prompts"]["domain_id"]["serialization_alias"],
        le=models_limitations["ai_prompts"]["domain_id"]["le"],
        ge=models_limitations["ai_prompts"]["domain_id"]["ge"],
        validation_alias=AliasChoices(
            models_limitations["ai_prompts"]["domain_id"]["validation_alias"][0],
            models_limitations["ai_prompts"]["domain_id"]["validation_alias"][1],
        ),
    )
    group_id: Optional[int] = Field(
        default=None,
        serialization_alias=models_limitations["ai_prompts"]["group_id"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["ai_prompts"]["group_id"]["validation_alias"][0],
            models_limitations["ai_prompts"]["group_id"]["validation_alias"][1],
        ),
        le=models_limitations["ai_prompts"]["group_id"]["le"],
        ge=models_limitations["ai_prompts"]["group_id"]["ge"],
    )
    group: GroupDescription | None = Field(
        default=None,
        serialization_alias=models_limitations["ai_prompts"]["group"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["ai_prompts"]["group"]["validation_alias"][0],
            models_limitations["ai_prompts"]["group"]["validation_alias"][1],
        ),
    )
    vector_search: bool = Field(
        default=False,
        description=models_limitations["ai_prompts"]["vector_search"]["description"],
        serialization_alias=models_limitations["ai_prompts"]["vector_search"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["ai_prompts"]["vector_search"]["validation_alias"][0],
            models_limitations["ai_prompts"]["vector_search"]["validation_alias"][1],
        ),
    )
    fallback_to_llm_values: bool = Field(
        default=False,
        description=models_limitations["ai_prompts"]["fallback_to_llm_values"]["description"],
        serialization_alias=models_limitations["ai_prompts"]["fallback_to_llm_values"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["ai_prompts"]["fallback_to_llm_values"]["validation_alias"][0],
            models_limitations["ai_prompts"]["fallback_to_llm_values"]["validation_alias"][1],
        ),
    )
    preferable_columns: bool = Field(
        default=False,
        description=models_limitations["ai_prompts"]["preferable_columns"]["description"],
        serialization_alias=models_limitations["ai_prompts"]["preferable_columns"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["ai_prompts"]["preferable_columns"]["validation_alias"][0],
            models_limitations["ai_prompts"]["preferable_columns"]["validation_alias"][1],
        ),
    )
    analytic_descriptions: Optional[AnalyticsDescription] = Field(
        default=None,
        validate_default=True,
        validation_alias=AliasChoices(
            models_limitations["ai_prompts"]["analytic_descriptions"]["validation_alias"][0],
            models_limitations["ai_prompts"]["analytic_descriptions"]["validation_alias"][1],
        ),
        serialization_alias=models_limitations["ai_prompts"]["analytic_descriptions"]["serialization_alias"],
    )
    model_config = ConfigDict(
        from_attributes=True,
    )

    @field_validator("related_dimensions", mode="after")
    @classmethod
    def get_related_dimensions(cls, value: list[str], all_fields: core_schema.ValidationInfo) -> list[str]:
        if all_fields.context and all_fields.context.get("related_dimensions"):
            return all_fields.context.get("related_dimensions")
        if value:
            return value
        return []

    @model_validator(
        mode="before",
    )
    @classmethod
    def analytics_description_validator(
        cls,
        orm_model: Any,
    ) -> AnalyticsDescription:
        """
        Валидатор обновляет поля в зависимости от наличия группы
        """
        if not hasattr(orm_model, "group_id"):
            return orm_model

        orm_model.analytic_descriptions = AnalyticsDescription(
            entity_name=orm_model.entity_name,
            description=(
                orm_model.group.description
                if orm_model.group and orm_model.group.description
                else orm_model.description
            ),
            few_shots=orm_model.few_shots,
            synonyms=orm_model.synonyms,
        )
        return orm_model
