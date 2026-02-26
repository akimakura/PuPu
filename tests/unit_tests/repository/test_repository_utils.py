from types import SimpleNamespace

from src.repository.utils import update_database_objects_schema_for_model


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
