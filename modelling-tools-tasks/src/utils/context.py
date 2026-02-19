from typing import Any

from fastapi import Request
from py_common_lib.logger import EPMPYLogger

logger = EPMPYLogger(__name__)


class CallContext:
    """
    Класс для работы с контекстом вызова.
    Используется для передачи и обработки контекста в celery task, который, в свою очередь,
    используется для сбора метрик воркера в prometheus
    """

    def __init__(self) -> None:
        self._context: dict[str, Any] = {}

    def set_context_from_request(self, request: Request, headers: dict[str, Any] | None = None) -> None:
        """
        Заполнение контекста из объекта HTTP-запроса

        Args:
            request (Request): объект HTTP-запроса
            headers (dict | None): словарь с заголовками запроса
        """
        self._context.clear()
        self._context["method"] = request.scope.get("method", "")
        self._context["path"] = request.scope.get("path", "")
        self._context.update(headers if headers else {})

    def set_context_from_dict(self, context: dict[str, Any]) -> None:
        """
        Заполнение контекста из словаря

        Args:
            context (dict[str, Any]): контекст
        """
        self._context = context

    @property
    def context(self) -> dict[str, Any]:
        return self._context

    @property
    def method(self) -> str:
        return self._context.get("method", "")

    @property
    def path(self) -> str:
        return self._context.get("path", "")

    @property
    def headers(self) -> str:
        return self._context.get("headers", {})
