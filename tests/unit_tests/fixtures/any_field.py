import pytest

from src.db.any_field import AnyField, AnyFieldLabel
from src.models.any_field import AggregationTypeEnum, AnyField as AnyFieldModel, AnyFieldTypeEnum
from src.models.label import Label, LabelType


@pytest.fixture
def any_fields() -> list[AnyField]:
    return [
        AnyField(
            id=1,
            name="test_anyfield1",
            type=AnyFieldTypeEnum.INTEGER,
            precision=12,
            labels=[
                AnyFieldLabel(
                    language="ru-ru",
                    type=LabelType.SHORT,
                    text="test",
                ),
                AnyFieldLabel(
                    language="ru-ru",
                    type=LabelType.LONG,
                    text="test",
                ),
            ],
            scale=None,
            aggregation_type=None,
        ),
        AnyField(
            id=2,
            name="test_anyfield2",
            type=AnyFieldTypeEnum.STRING,
            precision=12,
            labels=[],
            scale=None,
            aggregation_type=None,
        ),
        AnyField(
            id=3,
            name="test_anyfield3",
            type=AnyFieldTypeEnum.FLOAT,
            precision=12,
            labels=[],
            scale=8,
            aggregation_type=AggregationTypeEnum.MAXIMUM,
        ),
    ]


any_field_model_list = [
    AnyFieldModel(
        name="test_anyfield1",
        type=AnyFieldTypeEnum.INTEGER,
        precision=12,
        labels=[
            Label(
                language="ru-ru",
                type=LabelType.SHORT,
                text="test",
            ),
            Label(
                language="ru-ru",
                type=LabelType.LONG,
                text="test",
            ),
        ],
        scale=None,
        aggregation_type=None,
    ),
    AnyFieldModel(
        name="test_anyfield2", type=AnyFieldTypeEnum.STRING, precision=12, labels=[], scale=None, aggregation_type=None
    ),
    AnyFieldModel(
        name="test_anyfield3",
        type=AnyFieldTypeEnum.FLOAT,
        precision=12,
        labels=[],
        scale=8,
        aggregation_type=AggregationTypeEnum.MAXIMUM,
    ),
]
