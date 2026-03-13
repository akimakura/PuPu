import asyncio
import logging
from inspect import isawaitable
from typing import Any, Callable, Dict, Optional, cast

from fastapi import HTTPException, Request
from fastapi.routing import APIRoute
from py_common_lib.permissions import PermissionChecker
from py_common_lib.permissions.config import settings as permissions_settings
from py_common_lib.permissions.permissions_checker import get_current_user, http_bearer
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import JSONResponse, Response
from starlette.routing import Match

from src.cache import FastAPICache
from src.cache.backends.redis import RedisBackend
from src.cache.singleflight import REDIS_ERROR, SingleflightBarrier
from src.cache.types import Backend, CacheHeaderEnum, KeyBuilder
from src.config import settings
from src.utils.auth import api_key_auth

logger = logging.getLogger(__name__)


class ReadOnlyCacheMiddleware(BaseHTTPMiddleware):
    """Отдаёт кешированный ответ до выполнения DI, если это безопасно.

    При промахе кеша использует паттерн Singleflight: только один запрос
    идёт в обработчик и далее в источник данных, а остальные ожидают
    появления результата в кеше.
    """

    _barrier: Optional[SingleflightBarrier] = None
    _barrier_checked: bool = False

    # ---- dispatch (точка входа) ----------------------------------------

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
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
            return self._build_cached_response(cached, ttl, cache_status_header, request)

        if cache_control_header == CacheHeaderEnum.NO_CACHE:
            return await call_next(request)

        # --- Singleflight: только один запрос идёт в БД при cache miss ---

        barrier = self._get_barrier(backend)
        if barrier is None:
            return await call_next(request)

        cached_error_response = await self._poll_error_cache(cache_key, barrier, cache_status_header)
        if cached_error_response is not None:
            return cached_error_response

        try:
            acquired, token = await barrier.try_acquire(cache_key)
        except Exception:
            logger.exception("Singleflight: acquire error, falling back")
            return await call_next(request)

        if acquired:
            return await self._execute_owner_request(
                cache_key,
                token,
                barrier,
                request,
                call_next,
            )

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
        call_next: RequestResponseEndpoint,
    ) -> Response:
        """Ожидает появления данных в кеше или захватывает блокировку.

        Args:
            cache_key: Ключ кеша, общий для конкурирующих запросов.
            initial_token: Токен текущего владельца блокировки.
            backend: Бэкенд кеша для опроса сохранённого ответа.
            barrier: Singleflight-барьер на базе Redis.
            cache_status_header: Заголовок со статусом кеша.
            request: Входящий HTTP-запрос.
            call_next: Следующий обработчик в middleware-цепочке.

        Returns:
            Ответ из кеша либо результат вызова следующего обработчика.
        """
        watched_token = initial_token
        holder_elapsed = 0.0
        my_token: Optional[str] = None

        while True:
            await asyncio.sleep(barrier.poll_interval)
            holder_elapsed += barrier.poll_interval

            cached_response = await self._poll_cached_response(
                cache_key, backend, barrier, cache_status_header, request,
            )
            if cached_response is not None:
                return cached_response

            current_token = await barrier.safe_read_token(cache_key)
            if current_token is REDIS_ERROR:
                break

            if current_token is None:
                cached_response = await self._poll_cached_response(
                    cache_key, backend, barrier, cache_status_header, request,
                )
                if cached_response is not None:
                    return cached_response
                my_token = await barrier.safe_acquire(cache_key)
                if my_token is not None:
                    break
                token_updated, watched_token = await self._refresh_watched_token(cache_key, barrier, watched_token)
                if token_updated is None:
                    break
                if token_updated:
                    holder_elapsed = 0.0
                continue

            token_updated, watched_token = self._update_watched_token(watched_token, current_token)
            if token_updated:
                holder_elapsed = 0.0
                continue

            if holder_elapsed >= barrier.wait_timeout:
                my_token = await barrier.safe_force_acquire(cache_key, watched_token)
                if my_token is not None:
                    break
                token_updated, watched_token = await self._refresh_watched_token(cache_key, barrier, watched_token)
                if token_updated is None:
                    break
                if token_updated:
                    holder_elapsed = 0.0
                    continue
                holder_elapsed = 0.0
                continue

        if my_token is None:
            cached_response = await self._poll_cached_response(
                cache_key, backend, barrier, cache_status_header, request,
            )
            if cached_response is not None:
                return cached_response

        if my_token is not None:
            return await self._execute_owner_request(
                cache_key, my_token, barrier, request, call_next,
            )

        logger.debug("Singleflight: wait aborted, proceeding without lock, key=%s", cache_key)
        return await call_next(request)

    async def _poll_cached_response(
        self,
        cache_key: str,
        backend: Backend,
        barrier: SingleflightBarrier,
        cache_status_header: str,
        request: Request,
    ) -> Optional[Response]:
        """Проверяет success-cache, затем error-cache и возвращает готовый ответ или ``None``."""
        cached_response = await self._poll_cache(cache_key, backend, cache_status_header, request)
        if cached_response is not None:
            return cached_response
        return await self._poll_error_cache(cache_key, barrier, cache_status_header)

    async def _poll_cache(
        self,
        cache_key: str,
        backend: Backend,
        cache_status_header: str,
        request: Request,
    ) -> Optional[Response]:
        """Проверяет наличие успешного ответа в кеше и возвращает его или ``None``."""
        try:
            ttl, cached = await backend.get_with_ttl(cache_key)
            if cached is not None:
                logger.debug("Singleflight: cache populated during wait, key=%s", cache_key)
                return self._build_cached_response(cached, ttl, cache_status_header, request)
        except Exception:
            logger.exception("Singleflight: error checking cache, key=%s", cache_key)
        return None

    async def _poll_error_cache(
        self,
        cache_key: str,
        barrier: SingleflightBarrier,
        cache_status_header: str,
    ) -> Optional[Response]:
        """Проверяет наличие открытого error-state и возвращает готовый ответ или ``None``."""
        ttl = await barrier.safe_get_error_ttl(cache_key)
        if ttl is None:
            return None
        logger.debug("Singleflight: error-state open, key=%s", cache_key)
        return self._build_error_response(ttl, cache_status_header)

    async def _refresh_watched_token(
        self,
        cache_key: str,
        barrier: SingleflightBarrier,
        watched_token: str,
    ) -> tuple[Optional[bool], str]:
        """Повторно читает token владельца и сообщает, появился ли новый owner."""
        refreshed = await barrier.safe_read_token(cache_key)
        if refreshed is REDIS_ERROR:
            return None, watched_token
        return self._update_watched_token(watched_token, refreshed)

    @staticmethod
    def _update_watched_token(watched_token: str, candidate_token: Optional[str]) -> tuple[bool, str]:
        """Обновляет token наблюдаемого владельца и сообщает, произошла ли смена owner."""
        if candidate_token is None or candidate_token == watched_token:
            return False, watched_token
        return True, candidate_token

    async def _execute_owner_request(
        self,
        cache_key: str,
        token: str,
        barrier: SingleflightBarrier,
        request: Request,
        call_next: RequestResponseEndpoint,
    ) -> Response:
        """Выполняет запрос владельца lock и обновляет error-state по его результату."""
        try:
            response = await self._run_owner_request(cache_key, request, call_next)
            if 200 <= response.status_code < 400:
                await barrier.safe_clear_error_state(cache_key)
                return response

            if 500 <= response.status_code < 600:
                return await self._handle_owner_failure(cache_key, barrier, response)

            return response
        finally:
            await barrier.release(cache_key, token)

    async def _run_owner_request(
        self,
        cache_key: str,
        request: Request,
        call_next: RequestResponseEndpoint,
    ) -> Response:
        """Выполняет запрос владельца и нормализует необработанные исключения в 500 JSON."""
        try:
            return await call_next(request)
        except Exception:
            logger.exception("Singleflight: owner request crashed, key=%s", cache_key)
            return self._build_internal_error_response()

    async def _handle_owner_failure(
        self,
        cache_key: str,
        barrier: SingleflightBarrier,
        response: Response,
    ) -> Response:
        """Учитывает 5xx-ошибку владельца и при необходимости открывает error-state."""
        error_count = await barrier.safe_increment_owner_error_count(cache_key)
        if error_count is not None and error_count >= barrier.max_owner_errors:
            await barrier.safe_open_error_state(cache_key)
        return response

    # ---- инициализация барьера -----------------------------------------

    def _get_barrier(self, backend: Backend) -> Optional[SingleflightBarrier]:
        """Возвращает экземпляр ``SingleflightBarrier`` или ``None``.

        Барьер создаётся лениво при первом вызове и только если:

        - Singleflight включён в настройках (``ENABLE_SINGLEFLIGHT``).
        - Бэкенд кеша является :class:`RedisBackend`.
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
            max_owner_errors=settings.SINGLEFLIGHT_MAX_OWNER_ERRORS,
            error_ttl=settings.SINGLEFLIGHT_ERROR_TTL,
            error_counter_ttl=settings.SINGLEFLIGHT_ERROR_COUNTER_TTL,
        )
        return ReadOnlyCacheMiddleware._barrier

    def _match_route(self, request: Request) -> Optional[APIRoute]:
        """Находит подходящий ``APIRoute`` для текущего запроса."""
        scope = request.scope
        for route in request.app.router.routes:
            if not isinstance(route, APIRoute):
                continue
            match, _ = route.matches(scope)
            if match == Match.FULL:
                return route
        return None

    @staticmethod
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

    @staticmethod
    def _build_error_response(ttl: int, cache_status_header: str) -> JSONResponse:
        """Формирует стандартный ответ для открытого error-state."""
        ttl_header = max(ttl, 0)
        headers = {
            "Cache-Control": f"max-age={ttl_header}",
            cache_status_header: "ERROR",
        }
        return JSONResponse(status_code=503, content={"detail": {"msg": "Temporarily unavailable"}}, headers=headers)

    @staticmethod
    def _build_internal_error_response() -> JSONResponse:
        """Создаёт нормализованный JSON-ответ для необработанного исключения владельца."""
        return JSONResponse(status_code=500, content={"detail": {"msg": "Internal Server Error"}})
def _requires_api_key(route: APIRoute) -> bool:
    """Проверяет, подключена ли зависимость ``api_key_auth`` к маршруту."""
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
    """Возвращает ``PermissionChecker``, если он есть в зависимостях."""
    dependant = getattr(route, "dependant", None)
    if dependant is None:
        return None
    for dep in dependant.dependencies:
        dep_call = getattr(dep, "call", None)
        if isinstance(dep_call, PermissionChecker):
            return dep_call
    return None


async def _enforce_route_access(request: Request, route: APIRoute, endpoint: Any) -> None:
    """Проверяет hide/api-key/permissions перед выдачей кеша."""
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
