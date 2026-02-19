import logging
from inspect import isawaitable
from typing import Any, Callable, Dict, Optional, cast

from fastapi import HTTPException, Request
from fastapi.routing import APIRoute
from py_common_lib.permissions import PermissionChecker
from py_common_lib.permissions.config import settings as permissions_settings
from py_common_lib.permissions.permissions_checker import get_current_user, http_bearer
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse, Response
from starlette.routing import Match

from src.cache import FastAPICache
from src.cache.types import CacheHeaderEnum, KeyBuilder
from src.config import settings
from src.utils.auth import api_key_auth

logger = logging.getLogger(__name__)


class ReadOnlyCacheMiddleware(BaseHTTPMiddleware):
    """Отдает кэшированный ответ до выполнения DI, если это безопасно."""

    async def dispatch(self, request: Request, call_next: Any) -> Response:
        logger.debug("CacheMiddleware invoked for %s %s", request.method, request.url.path)
        if not FastAPICache.get_enable():
            return await call_next(request)

        route = self._match_route(request)
        if route is None:
            return await call_next(request)

        endpoint = route.endpoint

        cache_config: Optional[Dict[str, Any]] = getattr(endpoint, "__fastapi_cache_config__", None)
        if not cache_config:
            return await call_next(request)

        try:
            await _enforce_route_access(request, route, endpoint)
        except HTTPException as exc:
            return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

        if request.method not in {"GET", "HEAD", "POST"}:
            return await call_next(request)

        cache_control_header = request.headers.get("Cache-Control")
        if cache_control_header == CacheHeaderEnum.NO_STORE:
            return await call_next(request)

        namespace: str = cache_config.get("namespace", "")
        key_builder: Optional[KeyBuilder] = cache_config.get("key_builder")

        try:
            backend = FastAPICache.get_backend()
            prefix = FastAPICache.get_prefix()
            cache_status_header = FastAPICache.get_cache_status_header()
        except AssertionError:
            return await call_next(request)

        key_builder = key_builder or FastAPICache.get_key_builder()

        try:
            cache_key = key_builder(
                endpoint,
                f"{prefix}:{namespace}",
                request=request,
                response=None,
                args=(),
                kwargs={},
            )
            if isawaitable(cache_key):
                cache_key = await cache_key
        except Exception:
            logger.exception("Error building cache key in middleware")
            return await call_next(request)

        try:
            ttl, cached = await backend.get_with_ttl(cache_key)
        except Exception:
            logger.exception("Error retrieving cache key '%s' in middleware:", cache_key)
            return await call_next(request)

        if cached is None or cache_control_header == CacheHeaderEnum.NO_CACHE:
            return await call_next(request)

        etag = f"W/{hash(cached)}"
        ttl_header = max(ttl, 0)
        headers = {
            "Cache-Control": f"max-age={ttl_header}",
            "ETag": etag,
            cache_status_header: "HIT",
        }

        if request.headers.get("if-none-match") == etag:
            return Response(status_code=304, headers=headers)

        return Response(content=cached, media_type="application/json", headers=headers)

    def _match_route(self, request: Request) -> Optional[APIRoute]:
        """Находит подходящий APIRoute для текущего запроса."""
        scope = request.scope
        for route in request.app.router.routes:
            if not isinstance(route, APIRoute):
                continue
            match, _ = route.matches(scope)
            if match == Match.FULL:
                return route
        return None


def _requires_api_key(route: APIRoute) -> bool:
    """Проверяет, подключена ли зависимость api_key_auth к маршруту."""
    dependant = getattr(route, "dependant", None)
    if dependant is None:
        return False
    for dep in dependant.dependencies:
        dep_call = getattr(dep, "call", None)
        dep_name = getattr(dep_call, "__name__", "") if dep_call else ""
        if dep_name == "api_key_auth":
            return True
    return False


def _get_permission_checker(route: APIRoute) -> Optional[PermissionChecker]:
    """Возвращает PermissionChecker, если он есть в зависимостях."""
    dependant = getattr(route, "dependant", None)
    if dependant is None:
        return None
    for dep in dependant.dependencies:
        dep_call = getattr(dep, "call", None)
        if isinstance(dep_call, PermissionChecker):
            return dep_call
    return None


async def _enforce_route_access(request: Request, route: APIRoute, endpoint: Any) -> None:
    """Проверяет hide/api-key/permissions перед выдачей кэша."""
    try:
        hide_env_name = getattr(endpoint, "__hide_env__", None)
        if hide_env_name and getattr(settings, hide_env_name, False):
            logger.debug("CacheMiddleware auth: endpoint hidden by env %s", hide_env_name)
            raise HTTPException(status_code=403, detail="Endpoint disabled.")
        if _requires_api_key(route):
            api_key_auth_fn = cast(Callable[[str], None], api_key_auth)
            api_key_auth_fn(request.headers.get(settings.APP_SECRET_HEADER) or "")
            logger.debug("CacheMiddleware auth: api key check passed")
        checker = _get_permission_checker(route)
        if checker is not None and permissions_settings.ENABLE_AUTH:
            token = await http_bearer(request)
            user = await get_current_user(token)
            checker(user)
            logger.debug("CacheMiddleware auth: permission check passed")
    except HTTPException as exc:
        logger.debug("CacheMiddleware auth: HTTPException %s: %s", exc.status_code, exc.detail)
        raise
    except Exception:
        logger.exception("Auth precheck failed")
        raise HTTPException(status_code=500, detail="Authorization failed.")
