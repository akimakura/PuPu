import pytest

from src.db.dimension import AIPrompt
from src.models.ai_prompt import AIPrompt as AIPromptModel, AnalyticsDescription


@pytest.fixture
def prompts() -> list[AIPrompt]:
    return [
        AIPrompt(
            analytic_role="[\"test_analytic_role1\"]",
            purpose="[\"test_purpose1\"]",
            key_features="[\"test_key_features1\"]",
            data_type="[\"test_data_type1\"]",
            subject_area="[\"test_subject_area1\"]",
            example_questions="[\"example_questions1\", \"example_questions2\", \"example_questions3\"]",
            synonyms="[\"synonyms1\", \"synonyms2\", \"synonyms3\"]",
            markers="[\"markers1\", \"markers2\", \"markers3\"]",
            notes="[\"notes1\", \"notes2\", \"notes3\"]",
            ai_usage=False,
            domain_id=None,
            group_id=None,
            vector_search=False,
            fallback_to_llm_values=False,
            preferable_columns=False,
            description="description",
            entity_name="name",
            few_shots=None,
        ),
        AIPrompt(
            analytic_role="[\"test_analytic_role2\"]",
            purpose="[\"test_purpose2\"]",
            key_features="test_key_features2",
            data_type="[\"test_key_features2\"]",
            subject_area="[\"test_subject_area2\"]",
            example_questions="[\"example_questions2\", \"example_questions3\", \"example_questions4\"]",
            synonyms="[\"synonyms2\", \"synonyms3\", \"synonyms4\"]",
            markers="[\"markers2\", \"markers3\", \"markers4\"]",
            notes="[\"notes2\", \"notes3\", \"notes4\"]",
            ai_usage=False,
            domain_id=None,
            group_id=None,
            vector_search=False,
            fallback_to_llm_values=False,
            preferable_columns=False,
            description="description",
            entity_name="name",
            few_shots=None,
        ),
    ]


prompts_model = [
    AIPromptModel(
        analytic_role=["test_analytic_role1"],
        purpose=["test_purpose1"],
        key_features=["test_key_features1"],
        data_type=["test_data_type1"],
        subject_area=["test_subject_area1"],
        example_questions=["example_questions1", "example_questions2", "example_questions3"],
        markers=["markers1", "markers2", "markers3"],
        notes=["notes1", "notes2", "notes3"],
        related_dimensions=["test_dim5"],
        ai_usage=False,
        domain_id=None,
        vector_search=False,
        fallback_to_llm_values=False,
        preferable_columns=False,
        group=None,
        group_id=None,
        analytic_descriptions=AnalyticsDescription(
            entity_name="name",
            few_shots=None,
            description="description",
            synonyms='["synonyms1", "synonyms2", "synonyms3"]',
        ),
    ),
    AIPromptModel(
        analytic_role=["test_analytic_role2"],
        purpose=["test_purpose2"],
        key_features=["test_key_features2"],
        data_type=["test_data_type2"],
        subject_area=["test_subject_area2"],
        example_questions=["example_questions2", "example_questions3", "example_questions4"],
        markers=["markers2", "markers3", "markers4"],
        notes=["notes2", "notes3", "notes4"],
        related_dimensions=[],
        ai_usage=False,
        domain_id=None,
        vector_search=False,
        fallback_to_llm_values=False,
        preferable_columns=False,
        group=None,
        group_id=None,
        analytic_descriptions=AnalyticsDescription(
            entity_name="name",
            few_shots=None,
            description="description",
            synonyms='["synonyms2", "synonyms3", "synonyms4"]',
        ),
    ),
]
