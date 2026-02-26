from src.config import settings
from src.utils.schema_override import (
    apply_schema_override_to_database_objects,
    apply_schema_override_to_model_payload,
    build_model_schema_env_key,
    get_model_schema_override,
)


def test_build_model_schema_env_key() -> None:
    key = build_model_schema_env_key("tenant-1", "ror.dev_lt")
    assert key == "MODEL_TENANT_1_ROR_DEV_LT_SCHEMA_NAME"


def test_get_model_schema_override_from_env(monkeypatch) -> None:
    monkeypatch.setattr(settings, "ENABLE_SWITCH_MODEL_SCHEMA", True)
    monkeypatch.setenv("MODEL_TENANT1_ROR_DEV_LT_SCHEMA_NAME", "s_global_dwh_new")

    assert get_model_schema_override("tenant1", "ror_dev_lt") == "s_global_dwh_new"


def test_get_model_schema_override_optional(monkeypatch) -> None:
    monkeypatch.setattr(settings, "ENABLE_SWITCH_MODEL_SCHEMA", True)
    monkeypatch.delenv("MODEL_TENANT1_UNKNOWN_SCHEMA_NAME", raising=False)

    assert get_model_schema_override("tenant1", "unknown") is None


def test_apply_schema_override_to_model_payload(monkeypatch) -> None:
    monkeypatch.setattr(settings, "ENABLE_SWITCH_MODEL_SCHEMA", True)
    monkeypatch.setenv("MODEL_TENANT1_ROR_DEV_LT_SCHEMA_NAME", "new_schema")
    payload = {"name": "ror_dev_lt", "schemaName": "old_schema"}

    changed = apply_schema_override_to_model_payload(payload, "tenant1")

    assert changed is True
    assert payload["schemaName"] == "new_schema"


def test_apply_schema_override_to_database_objects() -> None:
    db_objects = [{"name": "dso", "schemaName": "old_schema", "objectType": "TABLE"}]

    changed = apply_schema_override_to_database_objects(db_objects, "new_schema")

    assert changed is True
    assert db_objects[0]["schemaName"] == "new_schema"
