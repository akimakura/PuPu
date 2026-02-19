import contextlib
import pathlib
import re
from typing import Any, Optional

import jwt
from py_common_lib.logger import EPMPYLogger
from py_common_lib.starlette_context_plugins import AuthorizationPlugin
from starlette_context import context
from starlette_context.errors import ContextDoesNotExistError

from src.config import settings

logger = EPMPYLogger(__name__)


def validate_path(path: Optional[str], comment: str = "") -> Optional[str]:
    """Проверяет существует ли такой путь"""
    if not path:
        return path
    path_obj = pathlib.Path(path)
    if path_obj.exists():
        return path
    raise ValueError(f"There is no file on the path: {path}. ({comment})")


def get_bool_from_str_or_bool(value: str | bool) -> bool:
    """
    Преобразовывает строку в bool значение.

    Преобразование идет по следующему алгоритму:
    0) bool -> bool
    1) "True" или "true" -> True
    2) "False" или "false" -> False
    3) "{число}" -> bool(int("{число}"))
    4) Все остальное bool(value)
    """
    if isinstance(value, bool):
        return value
    if value.isdigit():
        return bool(int(value))
    if value == "true" or value == "True":
        return True
    if value == "false" or value == "False":
        return False
    raise ValueError(f"Not valid string: '{value}'")


def read_file_as_bytes(filepath: str) -> bytes:
    """Прочитать файл бинарную строку."""
    with open(filepath, "rb") as file:
        return file.read()


def snake_to_camel(snake_str: str) -> str:
    """Переводит snake_case в camel_case."""
    characters = snake_str.split("_")
    return characters[0] + "".join(char.capitalize() for char in characters[1:])


def remove_parentheses_content(string: str) -> str:
    """
    Удаляет все подстроки, заключённые в круглые скобки, вместе с самими скобками из входной строки.

    Args:
        string (str): Исходная строка, из которой требуется удалить содержимое скобок.

    Returns:
        str: Строка без участков, ограниченных круглыми скобками.
    """
    return re.sub(r"\([^)]*\)", "", string)


def camel_to_snake(camel_str: str) -> str:
    """Переводит camel_case в snake_case."""
    snake_str = ""
    for i, char in enumerate(camel_str):
        if char.isupper() and i != 0:
            snake_str += "_" + char.lower()
        else:
            snake_str += char.lower()

    return snake_str


def get_username_by_token() -> Optional[str]:
    """
    Извлекает имя пользователя из JWT-токена, хранящегося в контексте.

    Пытается получить Bearer-токен из контекста, декодировать его без проверки подписи
    и вернуть claim 'sub' (subject). Возвращает None, если произошла ошибка на любом этапе
    или токен не найден.

    Returns:
        Optional[str]: Имя пользователя из полезной нагрузки токена или None, если извлечение провалилось
    """
    with contextlib.suppress(ContextDoesNotExistError):
        token = context.data.get(AuthorizationPlugin.key)
        if not token:
            return None
        token = token.replace("Bearer ", "")
        username = None
        try:
            decoded = jwt.decode(token, options={"verify_signature": False})
            username = decoded.get("sub")
        except jwt.DecodeError:
            logger.exception("Error decoding JWT")
        return username
    return None


def is_enable_generate_objects() -> bool:
    """
    Возвращает True, если включена генерация объектов в базах данных клиентов.
    Генерация разрешена только на dev стенде. На других стендах генерация запрщена.

    Returns:
        bool: True, если включена генерация объектов, иначе False.
    """
    return settings.ENABLE_GENERATE_OBJECTS


def get_not_null_or_raise(
    obj: Any, log_obj_name: str = "", log_attr_name: str = "", custom_raise_text: str = ""
) -> Any:
    """
    Возвращает объект, если он не NULL.

    Args:
        obj (Any): объект для проверки.
        log_obj_name (str): имя объекта для логирования.
        log_attr_name (str): имя атрибута объекта для логирования.
        custom_raise_text (str): имя атрибута объекта для логирования.

    Returns:
        Any: объект, который не равен NONE.

    Raises:
        ValueError: Если объект равен NONE.
    """
    if obj is None and not log_attr_name:
        raise ValueError(custom_raise_text or "Value is None")
    elif obj is None and log_attr_name and log_obj_name:
        raise ValueError(f"{log_obj_name}.{log_attr_name} is None")
    elif obj is None and log_attr_name:
        raise ValueError(f"{log_attr_name} is not set")
    elif obj is None and log_obj_name:
        raise ValueError(f"Object {log_obj_name} is None")
    return obj


def get_not_empty_or_raise(
    obj: Any, log_obj_name: str = "", log_attr_name: str = "", custom_raise_text: str = ""
) -> Any:
    """
    Возвращает объект, если он не пустой.

    Args:
        obj (Any): объект для проверки.
        log_obj_name (str): имя объекта для логирования.
        log_attr_name (str): имя атрибута объекта для логирования.
        custom_raise_text (str): имя атрибута объекта для логирования.

    Returns:
        Any: объект, который не пустой.

    Raises:
        ValueError: Если объект пустой.
    """
    if not obj and not log_attr_name:
        raise ValueError(custom_raise_text or "Value is empty")
    elif not obj and log_attr_name and log_obj_name:
        raise ValueError(f"{log_obj_name}.{log_attr_name} is empty")
    elif not obj and log_attr_name:
        raise ValueError(f"{log_attr_name} is empty")
    elif not obj and log_obj_name:
        raise ValueError(f"Object {log_obj_name} is empty")
    return obj
