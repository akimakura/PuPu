import os
import re
from typing import Any

from src.config import settings

_NON_ALNUM_RE = re.compile(r"[^A-Z0-9]+")


def _normalize_token(value: str) -> str:
    """Нормализует часть имени переменной окружения в верхний регистр с `_`."""
    normalized = _NON_ALNUM_RE.sub("_", value.upper()).strip("_")
    return normalized


def build_model_schema_env_key(tenant_id: str, model_name: str) -> str:
    """Формирует имя env-переменной для override схемы модели."""
    tenant_token = _normalize_token(tenant_id)
    model_token = _normalize_token(model_name)
    return f"MODEL_{tenant_token}_{model_token}_SCHEMA_NAME"


def _build_legacy_model_schema_env_key(tenant_id: str, model_name: str) -> str:
    """Формирует legacy-имя env-переменной схемы модели для обратной совместимости."""
    tenant_token = _normalize_token(tenant_id)
    model_token = _normalize_token(model_name)
    return f"DB_{tenant_token}_{model_token}_SCHEMA"


def get_model_schema_override(tenant_id: str | None, model_name: str | None) -> str | None:
    """
    Возвращает переопределённую схему модели из переменных окружения.

    Сначала проверяет новый формат `MODEL_{TENANT}_{MODEL}_SCHEMA_NAME`,
    затем legacy-формат `DB_{TENANT}_{MODEL}_SCHEMA`.
    """
    if not settings.ENABLE_SWITCH_MODEL_SCHEMA or not tenant_id or not model_name:
        return None

    env_key = build_model_schema_env_key(tenant_id, model_name)
    value = getattr(settings, env_key, None) or os.getenv(env_key)
    if value:
        return value.strip() or None

    # Keep backward compatibility with legacy variable naming.
    legacy_key = _build_legacy_model_schema_env_key(tenant_id, model_name)
    legacy_value = getattr(settings, legacy_key, None) or os.getenv(legacy_key)
    if legacy_value:
        return legacy_value.strip() or None

    return None


def apply_schema_override_to_model_payload(payload: dict[str, Any], tenant_id: str) -> bool:
    """Применяет override схемы к payload модели и возвращает флаг изменения."""
    model_name = payload.get("name")
    override_schema = get_model_schema_override(tenant_id, model_name)
    if not override_schema:
        return False

    changed = False
    for key in ("schemaName", "schema_name"):
        if key in payload and payload[key] != override_schema:
            payload[key] = override_schema
            changed = True

    if "schemaName" not in payload and "schema_name" not in payload:
        payload["schemaName"] = override_schema
        changed = True

    return changed


def apply_schema_override_to_database_objects(database_objects: list[Any], schema_name: str) -> bool:
    """Подменяет схему во всех dbObjects и возвращает флаг фактического изменения."""
    changed = False
    for db_object in database_objects:
        if isinstance(db_object, dict):
            has_schema_name = "schema_name" in db_object
            has_schema_name_camel = "schemaName" in db_object
            if has_schema_name and db_object.get("schema_name") != schema_name:
                db_object["schema_name"] = schema_name
                changed = True
            if has_schema_name_camel and db_object.get("schemaName") != schema_name:
                db_object["schemaName"] = schema_name
                changed = True
            if not has_schema_name and not has_schema_name_camel:
                db_object["schemaName"] = schema_name
                changed = True
            continue

        current = getattr(db_object, "schema_name", None)
        if current != schema_name:
            setattr(db_object, "schema_name", schema_name)
            changed = True

    return changed
