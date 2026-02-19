from enum import EnumMeta
from typing import Any


class ContainedEnum(EnumMeta):
    """Metaclass для enum, чтобы он поддерживал операцию in."""

    def __contains__(cls, item: Any) -> bool:
        try:
            cls(item)
        except ValueError:
            return False
        return True
