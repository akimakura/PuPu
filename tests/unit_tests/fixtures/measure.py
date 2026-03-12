from datetime import datetime

import pytest

from src.db.measure import DimensionFilter, Measure, MeasureLabel
from src.db.model import Model
from src.models.field import AggregationTypeEnum
from src.models.label import Label, LabelType
from src.models.measure import (
    DimensionValue,
    DimensionValueRequest,
    Measure as MeasureModel,
    MeasureCreateRequest as MeasureCreateRequestModel,
    MeasureEditRequest as MeasureEditRequestModel,
    MeasureTypeEnum,
)
from src.models.model import ModelStatus, ModelStatusEnum


@pytest.fixture
def measures(models: list[Model]) -> list[Measure]:
    return [
        Measure(
            version=1,
            timestamp=datetime(2023, 1, 1, 1, 1, 1, 1),
            name="test1",
            type=MeasureTypeEnum.FLOAT,
            precision=12,
            aggregation_type=AggregationTypeEnum.MAXIMUM,
            scale=1,
            tenant_id="tenant1",
            auth_relevant=True,
            dimension_id="test_dim1",
            dimension_value="test_dim1_value",
            models=[models[0]],
            labels=[
                MeasureLabel(
                    id=1,
                    language="ru-ru",
                    type=LabelType.SHORT,
                    text="test",
                ),
                MeasureLabel(
                    id=4,
                    language="ru-ru",
                    type=LabelType.LONG,
                    text="test",
                ),
            ],
            filter=[
                DimensionFilter(dimension_id="test_dim2", dimension_value="test_dim2_value"),
                DimensionFilter(
                    dimension_id="test_dim3",
                ),
            ],
        ),
        Measure(
            version=1,
            timestamp=datetime(2023, 1, 1, 1, 1, 1, 1),
            name="test2",
            type=MeasureTypeEnum.INTEGER,
            precision=13,
            aggregation_type=AggregationTypeEnum.MAXIMUM,
            scale=1,
            tenant_id="tenant1",
            auth_relevant=True,
            dimension_id="test_dim4",
            dimension_value=None,
            models=[models[0]],
            labels=[
                MeasureLabel(
                    id=2,
                    language="ru-ru",
                    type=LabelType.SHORT,
                    text="test",
                ),
                MeasureLabel(
                    id=3,
                    language="ru-ru",
                    type=LabelType.LONG,
                    text="test",
                ),
            ],
            filter=[],
        ),
        Measure(
            version=1,
            timestamp=datetime(2023, 1, 1, 1, 1, 1, 1),
            name="test3",
            type=MeasureTypeEnum.INTEGER,
            precision=13,
            tenant_id="tenant1",
            aggregation_type=AggregationTypeEnum.MAXIMUM,
            scale=1,
            auth_relevant=True,
            dimension_id=None,
            dimension_value=None,
            models=[models[1]],
            labels=[
                MeasureLabel(
                    id=5,
                    language="ru-ru",
                    type=LabelType.SHORT,
                    text="test",
                ),
                MeasureLabel(
                    id=6,
                    language="ru-ru",
                    type=LabelType.LONG,
                    text="test",
                ),
            ],
            filter=[],
        ),
    ]


measure_model_list = [
    MeasureModel(
        version=1,
        timestamp=datetime(2023, 1, 1, 1, 1, 1, 1),
        name="test1",
        models_statuses=[
            ModelStatus(
                name="test_model1",
                status=ModelStatusEnum.SUCCESS,
                msg=None,
            )
        ],
        models_names=["test_model1"],
        type=MeasureTypeEnum.FLOAT,
        precision=12,
        aggregation_type=AggregationTypeEnum.MAXIMUM,
        scale=1,
        auth_relevant=True,
        unit_of_measure=DimensionValue(dimension_name="test_dim1", dimension_value="test_dim1_value"),
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
        filter=[
            DimensionValue(dimension_name="test_dim2", dimension_value="test_dim2_value"),
            DimensionValue(
                dimension_name="test_dim3",
                dimension_value=None,
            ),
        ],
    ),
    MeasureModel(
        version=1,
        timestamp=datetime(2023, 1, 1, 1, 1, 1, 1),
        name="test2",
        models_statuses=[
            ModelStatus(
                name="test_model1",
                status=ModelStatusEnum.SUCCESS,
                msg=None,
            )
        ],
        models_names=["test_model1"],
        type=MeasureTypeEnum.INTEGER,
        precision=13,
        aggregation_type=AggregationTypeEnum.MAXIMUM,
        scale=1,
        auth_relevant=True,
        unit_of_measure="test_dim4",
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
        filter=[],
    ),
    MeasureModel(
        version=1,
        timestamp=datetime(2023, 1, 1, 1, 1, 1, 1),
        name="test3",
        type=MeasureTypeEnum.INTEGER,
        models_statuses=[
            ModelStatus(
                name="test_model2",
                status=ModelStatusEnum.SUCCESS,
                msg=None,
            )
        ],
        models_names=["test_model2"],
        precision=13,
        aggregation_type=AggregationTypeEnum.MAXIMUM,
        scale=1,
        auth_relevant=True,
        unit_of_measure=None,
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
        filter=[],
    ),
    MeasureModel(
        version=1,
        timestamp=datetime(2023, 1, 1, 1, 1, 1, 1),
        name="test4",
        models_statuses=[
            ModelStatus(
                name="test_model1",
                status=ModelStatusEnum.SUCCESS,
                msg=None,
            )
        ],
        models_names=["test_model1"],
        type=MeasureTypeEnum.FLOAT,
        precision=12,
        aggregation_type=AggregationTypeEnum.MAXIMUM,
        scale=1,
        auth_relevant=True,
        unit_of_measure=DimensionValue(dimension_name="test_dim1", dimension_value="test_dim1_value"),
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
        filter=[
            DimensionValue(dimension_name="test_dim2", dimension_value="test_dim2_value"),
            DimensionValue(
                dimension_name="test_dim3",
                dimension_value=None,
            ),
        ],
    ),
    MeasureModel(
        version=1,
        timestamp=datetime(2023, 1, 1, 1, 1, 1, 1),
        name="test5",
        models_statuses=[
            ModelStatus(
                name="test_model1",
                status=ModelStatusEnum.SUCCESS,
                msg=None,
            )
        ],
        models_names=["test_model1"],
        type=MeasureTypeEnum.INTEGER,
        precision=13,
        aggregation_type=AggregationTypeEnum.MAXIMUM,
        scale=1,
        auth_relevant=True,
        allow_null_values=True,
        unit_of_measure="test_dim4",
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
        filter=[],
    ),
]


measure_model_create_list = [
    MeasureCreateRequestModel(
        name="test4",
        type=MeasureTypeEnum.FLOAT,
        precision=12,
        aggregation_type=AggregationTypeEnum.MAXIMUM,
        scale=1,
        auth_relevant=True,
        unit_of_measure=DimensionValueRequest(
            dimension_id="test_dim1",
            dimension_value="test_dim1_value",
        ),
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
        filter=[
            DimensionValueRequest(dimension_id="test_dim2", dimension_value="test_dim2_value"),
            DimensionValueRequest(
                dimension_id="test_dim3",
                dimension_value=None,
            ),
        ],
    ),
    MeasureCreateRequestModel(
        name="test5",
        type=MeasureTypeEnum.INTEGER,
        precision=13,
        aggregation_type=AggregationTypeEnum.MAXIMUM,
        scale=1,
        auth_relevant=True,
        allow_null_values=True,
        unit_of_measure="test_dim4",
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
        filter=[],
    ),
]


measure_model_update_list = [
    MeasureEditRequestModel(
        type=MeasureTypeEnum.FLOAT,
        precision=12,
        aggregation_type=AggregationTypeEnum.MAXIMUM,
        scale=1,
        auth_relevant=True,
        unit_of_measure="test_dim4",
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
        filter=[
            DimensionValueRequest(dimension_id="test_dim2", dimension_value="test_dim2_value"),
            DimensionValueRequest(
                dimension_id="test_dim3",
                dimension_value=None,
            ),
        ],
    ),
    MeasureEditRequestModel(
        type=MeasureTypeEnum.INTEGER,
        precision=22,
        aggregation_type=AggregationTypeEnum.MAXIMUM,
        scale=1,
        auth_relevant=False,
        allow_null_values=True,
        unit_of_measure=DimensionValueRequest(
            dimension_id="test_dim1",
            dimension_value="test_dim1_value",
        ),
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
        filter=[
            DimensionValueRequest(dimension_id="test_dim2", dimension_value="test_dim2_value"),
            DimensionValueRequest(
                dimension_id="test_dim3",
                dimension_value=None,
            ),
        ],
    ),
]
