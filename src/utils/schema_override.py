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


def _normalize_env_value(value: Any) -> str | None:
    """Нормализует значение env-параметра и отбрасывает пустые значения."""
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _read_env_schema_value(env_key: str) -> str | None:
    """Возвращает значение env-параметра из `settings` или системного окружения."""
    settings_value = _normalize_env_value(getattr(settings, env_key, None))
    if settings_value is not None:
        print(f"[SCHEMA_OVERRIDE_DEBUG] env_key={env_key} source=settings value={settings_value}")
        return settings_value
    os_value = _normalize_env_value(os.getenv(env_key))
    if os_value is not None:
        print(f"[SCHEMA_OVERRIDE_DEBUG] env_key={env_key} source=os value={os_value}")
    else:
        print(f"[SCHEMA_OVERRIDE_DEBUG] env_key={env_key} source=none value=None")
    return os_value


def get_model_schema_override(tenant_id: str | None, model_name: str | None) -> str | None:
    """
    Возвращает переопределённую схему модели из переменных окружения.

    Сначала проверяет новый формат `MODEL_{TENANT}_{MODEL}_SCHEMA_NAME`,
    затем legacy-формат `DB_{TENANT}_{MODEL}_SCHEMA`.
    Если `ENABLE_SWITCH_MODEL_SCHEMA` выключен, для обратной совместимости
    может использовать legacy-формат при включённом `ENABLE_SWITCH_HOST`.
    """
    if not tenant_id or not model_name:
        print(f"[SCHEMA_OVERRIDE_DEBUG] skip override: tenant_id={tenant_id!r} model_name={model_name!r}")
        return None

    enable_switch_model_schema = getattr(settings, "ENABLE_SWITCH_MODEL_SCHEMA", True)
    enable_legacy_model_schema_override = getattr(
        settings,
        "ENABLE_LEGACY_MODEL_SCHEMA_OVERRIDE",
        getattr(settings, "ENABLE_SWITCH_HOST", False),
    )
    print(
        "[SCHEMA_OVERRIDE_DEBUG] resolve override: "
        f"tenant={tenant_id} model={model_name} "
        f"enable_switch_model_schema={enable_switch_model_schema} "
        f"enable_legacy_model_schema_override={enable_legacy_model_schema_override}"
    )

    legacy_key = _build_legacy_model_schema_env_key(tenant_id, model_name)
    if enable_switch_model_schema:
        env_key = build_model_schema_env_key(tenant_id, model_name)
        value = _read_env_schema_value(env_key)
        if value is not None:
            print(f"[SCHEMA_OVERRIDE_DEBUG] override resolved by new env key={env_key} value={value}")
            return value

        legacy_value = _read_env_schema_value(legacy_key)
        if legacy_value is not None:
            print(
                f"[SCHEMA_OVERRIDE_DEBUG] override resolved by legacy fallback key={legacy_key} value={legacy_value}"
            )
            return legacy_value

    if enable_legacy_model_schema_override:
        legacy_value = _read_env_schema_value(legacy_key)
        if legacy_value is not None:
            print(f"[SCHEMA_OVERRIDE_DEBUG] override resolved by legacy key={legacy_key} value={legacy_value}")
            return legacy_value

    print(f"[SCHEMA_OVERRIDE_DEBUG] override not found for tenant={tenant_id} model={model_name}")
    return None


def apply_schema_override_to_model_payload(payload: dict[str, Any], tenant_id: str) -> bool:
    """Применяет override схемы к payload модели и возвращает флаг изменения."""
    model_name = payload.get("name")
    override_schema = get_model_schema_override(tenant_id, model_name)
    if not override_schema:
        print(f"[SCHEMA_OVERRIDE_DEBUG] model payload: no override tenant={tenant_id} model={model_name}")
        return False

    changed = False
    before_schema_name = payload.get("schemaName")
    before_schema_name_snake = payload.get("schema_name")
    for key in ("schemaName", "schema_name"):
        if key in payload and payload[key] != override_schema:
            payload[key] = override_schema
            changed = True

    if "schemaName" not in payload and "schema_name" not in payload:
        payload["schemaName"] = override_schema
        changed = True

    print(
        "[SCHEMA_OVERRIDE_DEBUG] model payload updated: "
        f"tenant={tenant_id} model={model_name} changed={changed} "
        f"before.schemaName={before_schema_name} "
        f"before.schema_name={before_schema_name_snake} "
        f"after.schemaName={payload.get('schemaName')} "
        f"after.schema_name={payload.get('schema_name')}"
    )
    return changed


def apply_schema_override_to_database_objects(database_objects: list[Any], schema_name: str) -> bool:
    """Подменяет схему во всех dbObjects и возвращает флаг фактического изменения."""
    before_schemas = []
    for db_object in database_objects:
        if isinstance(db_object, dict):
            before_schemas.append(db_object.get("schemaName") or db_object.get("schema_name"))
        else:
            before_schemas.append(getattr(db_object, "schema_name", None))
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

        if not hasattr(db_object, "schema_name"):
            continue
        if db_object.schema_name != schema_name:
            db_object.schema_name = schema_name
            changed = True

    after_schemas = []
    for db_object in database_objects:
        if isinstance(db_object, dict):
            after_schemas.append(db_object.get("schemaName") or db_object.get("schema_name"))
        else:
            after_schemas.append(getattr(db_object, "schema_name", None))
    print(
        f"[SCHEMA_OVERRIDE_DEBUG] dbObjects updated: changed={changed} target_schema={schema_name} "
        f"before={before_schemas} after={after_schemas}"
    )
    return changed
