from types import SimpleNamespace

import pytest

from src.db.any_field import AnyField
from src.db.dimension import Dimension
from src.db.measure import Measure
from src.models.any_field import AnyFieldTypeEnum
from src.models.field import BaseFieldTypeEnum, SemanticType
from src.models.measure import MeasureTypeEnum
from src.repository.utils import update_database_objects_schema_for_model
from src.repository.utils import (
    is_nullable_measure_field,
    resolve_data_storage_field_allow_null_values_local,
)


class _EnumLike:
    def __init__(self, value: str) -> None:
        self.value = value


def test_update_database_objects_schema_for_model_updates_only_target_model() -> None:
    target_table = SimpleNamespace(
        name="orders",
        type=_EnumLike("TABLE"),
        schema_name="old_schema",
        models=[SimpleNamespace(name="model_a")],
    )
    target_dictionary = SimpleNamespace(
        name="orders_d",
        type="DICTIONARY",
        schema_name="old_schema",
        models=[SimpleNamespace(name="model_a")],
    )
    other_model_object = SimpleNamespace(
        name="orders",
        type="TABLE",
        schema_name="other_schema",
        models=[SimpleNamespace(name="model_b")],
    )

    changed = update_database_objects_schema_for_model(
        database_objects=[target_table, target_dictionary, other_model_object],
        model_name="model_a",
        schema_update_database_objects=[
            SimpleNamespace(name="orders", type="TABLE", schema_name="new_schema"),
            SimpleNamespace(name="orders_d", type="DICTIONARY", schema_name="new_schema"),
        ],
    )

    assert changed is True
    assert target_table.schema_name == "new_schema"
    assert target_dictionary.schema_name == "new_schema"
    assert other_model_object.schema_name == "other_schema"


def test_update_database_objects_schema_for_model_returns_false_for_empty_update() -> None:
    target_table = SimpleNamespace(
        name="orders",
        type="TABLE",
        schema_name="old_schema",
        models=[SimpleNamespace(name="model_a")],
    )

    changed = update_database_objects_schema_for_model(
        database_objects=[target_table],
        model_name="model_a",
        schema_update_database_objects=[SimpleNamespace(name="orders", type="TABLE", schema_name=None)],
    )

    assert changed is False
    assert target_table.schema_name == "old_schema"


def test_resolve_data_storage_field_allow_null_values_local_inherits_measure_default() -> None:
    measure = Measure(name="revenue", type=MeasureTypeEnum.FLOAT, precision=12, allow_null_values=True)

    result = resolve_data_storage_field_allow_null_values_local(
        field={"semantic_type": SemanticType.MEASURE, "allow_null_values_local": None},
        field_type=BaseFieldTypeEnum.MEASURE,
        object_field=measure,
    )

    assert result is True


def test_resolve_data_storage_field_allow_null_values_local_rejects_dimension_true() -> None:
    dimension = Dimension(name="country", type="STRING", precision=3)

    with pytest.raises(ValueError):
        resolve_data_storage_field_allow_null_values_local(
            field={"semantic_type": SemanticType.DIMENSION, "allow_null_values_local": True},
            field_type=BaseFieldTypeEnum.DIMENSION,
            object_field=dimension,
        )


def test_resolve_data_storage_field_allow_null_values_local_uses_nested_anyfield_setting_only() -> None:
    any_field = AnyField(name="nullable_metric", type=AnyFieldTypeEnum.FLOAT, precision=12, allow_null_values=True)

    result = resolve_data_storage_field_allow_null_values_local(
        field={"semantic_type": SemanticType.MEASURE, "allow_null_values_local": True},
        field_type=BaseFieldTypeEnum.ANYFIELD,
        object_field=any_field,
    )

    assert result is False


def test_is_nullable_measure_field_returns_true_for_anyfield_measure() -> None:
    field = SimpleNamespace(
        semantic_type=SemanticType.MEASURE,
        field_type=BaseFieldTypeEnum.ANYFIELD,
        any_field=SimpleNamespace(allow_null_values=True),
    )

    assert is_nullable_measure_field(field) is True
