"""
Декоратор для скрытия эндпоинтов.
"""

from functools import wraps
from http import HTTPStatus
from typing import Any

from fastapi import FastAPI, HTTPException

from src.config import settings

app = FastAPI()


def hide_endpoint(env_name: str) -> Any:
    """
    Скрывает эндпоинт, если значение переменной "env_name" в settings будет равно 1.
    Рекомендация к названию env_name: "HIDE_ИМЯ_ФУНКЦИИ_КОТОРУЮ_ХОТИМ_СКРЫВАТЬ"
    """

    def decorator(func: Any) -> Any:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            is_hide = getattr(settings, env_name)
            if is_hide:
                raise HTTPException(HTTPStatus.FORBIDDEN, "Endpoint disabled.")
            return await func(*args, **kwargs)

        wrapper.__hide_env__ = env_name  # type: ignore[attr-defined]
        return wrapper

    return decorator
