"""
Middleware для работы prometheus.
"""

from http import HTTPStatus
from typing import Any, Tuple

from starlette.requests import HTTPConnection
from starlette.routing import Match
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from src.metrics.business_metrics import MODELS


class PrometheusBusinessMetricsMiddleware:
    def __init__(self, app: ASGIApp, filter_unhandled_paths: bool = False) -> None:
        self.app = app
        self.filter_unhandled_paths = filter_unhandled_paths

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return

        status_code = None

        async def send_wrapper(message: Message) -> None:
            nonlocal status_code, send
            if message["type"] == "http.response.start":
                status_code = message["status"]
            await send(message)

        conn = HTTPConnection(scope)
        _, is_handled_path = self.get_path_template(conn, scope)
        if self._is_path_filtered(is_handled_path):
            await self.app(scope, receive, send)
            return
        try:
            await self.app(scope, receive, send_wrapper)
        except BaseException as e:
            status_code = HTTPStatus.INTERNAL_SERVER_ERROR
            raise e from None
        finally:
            params = self.get_path_params_by_conn(conn)
            if params.get("modelName"):
                MODELS.labels(model_name=params["modelName"]).inc()

    @staticmethod
    def get_path_template(conn: HTTPConnection, scope: Scope) -> Tuple[str, bool]:
        for route in scope["app"].routes:
            match, child_scope = route.matches(scope)
            if match == Match.FULL:
                return route.path, True

        return conn.url.path, False

    def get_path_params_by_conn(self, conn: HTTPConnection) -> dict[str, Any]:
        return conn.path_params

    def _is_path_filtered(self, is_handled_path: bool) -> bool:
        return self.filter_unhandled_paths and not is_handled_path
