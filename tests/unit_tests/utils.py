from typing import Any


def clear_uncompared_fields(
    data: Any,
) -> Any:
    """Очищает поля, которые не участвуют в сравнении."""
    ignore_fields = ["version", "updatedAt", "updatedBy"]
    for field in ignore_fields:
        if isinstance(data, dict):
            data.pop(field, None)
        if isinstance(data, list):
            for list_item in data:
                list_item.pop(field, None)
    return data
