import asyncio
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
from src.cache.backends.redis import RedisBackend
from src.cache.singleflight import MAX_WAIT_ROUNDS, REDIS_ERROR, SingleflightBarrier
from src.cache.types import Backend, CacheHeaderEnum, KeyBuilder
from src.config import settings
from src.utils.auth import api_key_auth

logger = logging.getLogger(__name__)


class ReadOnlyCacheMiddleware(BaseHTTPMiddleware):
    """Отдает кэшированный ответ до выполнения DI, если это безопасно.

    При промахе кэша использует паттерн Singleflight: только один запрос
    идёт в обработчик (и далее в БД), а остальные ожидают появления
    результата в кэше, что предотвращает перегрузку БД при холодном кэше.
    """

    _barrier: Optional[SingleflightBarrier] = None
    _barrier_checked: bool = False

    # ---- dispatch (точка входа) ----------------------------------------

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

        if cached is not None and cache_control_header != CacheHeaderEnum.NO_CACHE:
            return _build_cached_response(cached, ttl, cache_status_header, request)

        if cache_control_header == CacheHeaderEnum.NO_CACHE:
            return await call_next(request)

        # --- Singleflight: только один запрос идёт в БД при cache miss ---

        barrier = self._get_barrier(backend)
        if barrier is None:
            return await call_next(request)

        try:
            acquired, token = await barrier.try_acquire(cache_key)
        except Exception:
            logger.exception("Singleflight: acquire error, falling back")
            return await call_next(request)

        if acquired:
            try:
                return await call_next(request)
            finally:
                await barrier.release(cache_key, token)

        return await self._singleflight_wait(
            cache_key, token, backend, barrier, cache_status_header, request, call_next,
        )

    # ---- singleflight: цикл ожидания -----------------------------------

    async def _singleflight_wait(
        self,
        cache_key: str,
        initial_token: str,
        backend: Backend,
        barrier: SingleflightBarrier,
        cache_status_header: str,
        request: Request,
        call_next: Any,
    ) -> Response:
        """Ожидает появления данных в кэше или захватывает блокировку.

        Поллит кэш с заданным интервалом. Если данные появляются — возвращает
        их как ответ. Отслеживает смену владельца блокировки (новый токен) и
        сбрасывает таймер ожидания, чтобы не конкурировать с новым владельцем.

        При истечении таймаута для текущего владельца выполняет принудительный
        захват блокировки через CAS. Только один из ожидающих запросов
        успешно захватит блокировку; остальные продолжат ожидание нового
        владельца в пределах общего лимита (``MAX_WAIT_ROUNDS``).
        """
        watched_token = initial_token
        holder_elapsed = 0.0
        total_elapsed = 0.0
        max_total_wait = barrier.wait_timeout * MAX_WAIT_ROUNDS
        my_token: Optional[str] = None

        while total_elapsed < max_total_wait:
            await asyncio.sleep(barrier.poll_interval)
            holder_elapsed += barrier.poll_interval
            total_elapsed += barrier.poll_interval

            cached_response = await self._poll_cache(cache_key, backend, cache_status_header, request)
            if cached_response is not None:
                return cached_response

            current_token = await barrier.safe_read_token(cache_key)
            if current_token is REDIS_ERROR:
                break

            if current_token is None:
                cached_response = await self._poll_cache(cache_key, backend, cache_status_header, request)
                if cached_response is not None:
                    return cached_response
                my_token = await barrier.safe_acquire(cache_key)
                if my_token is not None:
                    break
                refreshed = await barrier.safe_read_token(cache_key)
                if refreshed not in (None, REDIS_ERROR) and refreshed != watched_token:
                    watched_token = refreshed
                    holder_elapsed = 0.0
                continue

            if current_token != watched_token:
                watched_token = current_token
                holder_elapsed = 0.0
                continue

            if holder_elapsed >= barrier.wait_timeout:
                my_token = await barrier.safe_force_acquire(cache_key, watched_token)
                if my_token is not None:
                    break
                refreshed = await barrier.safe_read_token(cache_key)
                if refreshed not in (None, REDIS_ERROR) and refreshed != watched_token:
                    watched_token = refreshed
                    holder_elapsed = 0.0
                    continue
                break

        if my_token is None:
            cached_response = await self._poll_cache(cache_key, backend, cache_status_header, request)
            if cached_response is not None:
                return cached_response

        if my_token is not None:
            try:
                return await call_next(request)
            finally:
                await barrier.release(cache_key, my_token)

        logger.debug("Singleflight: all attempts exhausted, proceeding without lock, key=%s", cache_key)
        return await call_next(request)

    async def _poll_cache(
        self,
        cache_key: str,
        backend: Backend,
        cache_status_header: str,
        request: Request,
    ) -> Optional[Response]:
        """Проверяет наличие данных в кэше и возвращает готовый ответ или ``None``."""
        try:
            ttl, cached = await backend.get_with_ttl(cache_key)
            if cached is not None:
                logger.debug("Singleflight: cache populated during wait, key=%s", cache_key)
                return _build_cached_response(cached, ttl, cache_status_header, request)
        except Exception:
            logger.exception("Singleflight: error checking cache, key=%s", cache_key)
        return None

    # ---- инициализация барьера -----------------------------------------

    def _get_barrier(self, backend: Backend) -> Optional[SingleflightBarrier]:
        """Возвращает экземпляр ``SingleflightBarrier`` или ``None``.

        Барьер создаётся лениво при первом вызове и только если:

        - Singleflight включён в настройках (``ENABLE_SINGLEFLIGHT``).
        - Бэкенд кэша — :class:`RedisBackend` (для ``InMemoryBackend``
          паттерн не имеет смысла, т.к. нет распределённого доступа).
        """
        if not settings.ENABLE_SINGLEFLIGHT:
            return None

        if self._barrier is not None:
            return self._barrier

        if self._barrier_checked:
            return None

        ReadOnlyCacheMiddleware._barrier_checked = True

        if not isinstance(backend, RedisBackend):
            return None

        ReadOnlyCacheMiddleware._barrier = SingleflightBarrier(
            redis=backend.redis,
            lock_ttl=settings.SINGLEFLIGHT_LOCK_TTL,
            wait_timeout=settings.SINGLEFLIGHT_WAIT_TIMEOUT,
            poll_interval=settings.SINGLEFLIGHT_POLL_INTERVAL,
        )
        return ReadOnlyCacheMiddleware._barrier

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


def _build_cached_response(
    cached: bytes,
    ttl: int,
    cache_status_header: str,
    request: Request,
) -> Response:
    """Формирует HTTP-ответ из закешированных данных."""
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
