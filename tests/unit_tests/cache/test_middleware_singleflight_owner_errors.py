import json
from unittest.mock import AsyncMock, MagicMock

from fastapi import FastAPI
from starlette.requests import Request
from starlette.responses import Response

from src.cache import FastAPICache
from src.cache.middleware import ReadOnlyCacheMiddleware


def _make_request(app: FastAPI) -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/cache-test",
            "query_string": b"",
            "headers": [],
            "app": app,
        }
    )


class TestReadOnlyCacheMiddlewareOwnerFailures:

    async def test_success_response_clears_error_state(self) -> None:
        app = FastAPI()
        middleware = ReadOnlyCacheMiddleware(app)
        backend = MagicMock()

        barrier = MagicMock()
        barrier.safe_clear_error_state = AsyncMock()
        barrier.safe_increment_owner_error_count = AsyncMock()
        barrier.safe_open_error_state = AsyncMock()
        barrier.release = AsyncMock()
        barrier.max_owner_errors = 4

        response = Response(content=b"[]", media_type="application/json", status_code=200)
        call_next = AsyncMock(return_value=response)

        result = await middleware._execute_owner_request(
            "cache-key",
            "owner-token",
            barrier,
            _make_request(app),
            call_next,
        )

        assert result is response
        barrier.safe_clear_error_state.assert_awaited_once_with("cache-key")
        barrier.safe_increment_owner_error_count.assert_not_awaited()
        barrier.safe_open_error_state.assert_not_awaited()
        barrier.release.assert_awaited_once_with("cache-key", "owner-token")

    async def test_5xx_response_creates_error_cache_after_threshold(self) -> None:
        app = FastAPI()
        middleware = ReadOnlyCacheMiddleware(app)
        backend = MagicMock()
        backend.get_with_ttl = AsyncMock(return_value=(0, None))

        barrier = MagicMock()
        barrier.safe_clear_error_state = AsyncMock()
        barrier.safe_increment_owner_error_count = AsyncMock(return_value=4)
        barrier.safe_open_error_state = AsyncMock(return_value=True)
        barrier.release = AsyncMock()
        barrier.max_owner_errors = 4

        response = Response(
            content=b'{"detail":"upstream unavailable"}',
            media_type="application/json",
            status_code=503,
        )
        call_next = AsyncMock(return_value=response)

        result = await middleware._execute_owner_request(
            "cache-key",
            "owner-token",
            barrier,
            _make_request(app),
            call_next,
        )

        assert result.status_code == 503
        barrier.safe_increment_owner_error_count.assert_awaited_once_with("cache-key")
        barrier.safe_open_error_state.assert_awaited_once_with("cache-key")
        barrier.release.assert_awaited_once_with("cache-key", "owner-token")

    async def test_4xx_response_does_not_change_error_counter(self) -> None:
        app = FastAPI()
        middleware = ReadOnlyCacheMiddleware(app)
        backend = MagicMock()

        barrier = MagicMock()
        barrier.safe_clear_error_state = AsyncMock()
        barrier.safe_increment_owner_error_count = AsyncMock()
        barrier.safe_open_error_state = AsyncMock()
        barrier.release = AsyncMock()
        barrier.max_owner_errors = 4

        response = Response(content=b"{}", media_type="application/json", status_code=404)
        call_next = AsyncMock(return_value=response)

        result = await middleware._execute_owner_request(
            "cache-key",
            "owner-token",
            barrier,
            _make_request(app),
            call_next,
        )

        assert result is response
        barrier.safe_clear_error_state.assert_not_awaited()
        barrier.safe_increment_owner_error_count.assert_not_awaited()
        barrier.safe_open_error_state.assert_not_awaited()
        barrier.release.assert_awaited_once_with("cache-key", "owner-token")

    async def test_uncaught_exception_is_normalized_and_cached_after_threshold(self) -> None:
        app = FastAPI()
        middleware = ReadOnlyCacheMiddleware(app)
        backend = MagicMock()
        backend.get_with_ttl = AsyncMock(return_value=(0, None))

        barrier = MagicMock()
        barrier.safe_clear_error_state = AsyncMock()
        barrier.safe_increment_owner_error_count = AsyncMock(return_value=4)
        barrier.safe_open_error_state = AsyncMock(return_value=True)
        barrier.release = AsyncMock()
        barrier.max_owner_errors = 4

        call_next = AsyncMock(side_effect=RuntimeError("boom"))

        result = await middleware._execute_owner_request(
            "cache-key",
            "owner-token",
            barrier,
            _make_request(app),
            call_next,
        )

        assert result.status_code == 500
        assert json.loads(result.body) == {"detail": {"msg": "Internal Server Error"}}
        barrier.safe_increment_owner_error_count.assert_awaited_once_with("cache-key")
        barrier.safe_open_error_state.assert_awaited_once_with("cache-key")
        barrier.release.assert_awaited_once_with("cache-key", "owner-token")

    async def test_dispatch_returns_cached_error_before_lock_acquire(self, monkeypatch) -> None:  # type: ignore[no-untyped-def]
        app = FastAPI()
        middleware = ReadOnlyCacheMiddleware(app)
        endpoint = AsyncMock()
        endpoint.__fastapi_cache_config__ = {
            "namespace": "models",
            "key_builder": lambda *args, **kwargs: "cache-key",
        }
        route = MagicMock()
        route.endpoint = endpoint

        backend = MagicMock()
        backend.get_with_ttl = AsyncMock(return_value=(0, None))

        barrier = MagicMock()
        barrier.safe_get_error_ttl = AsyncMock(return_value=9)
        barrier.try_acquire = AsyncMock()

        monkeypatch.setattr("src.cache.middleware._enforce_route_access", AsyncMock(return_value=None))
        middleware._match_route = MagicMock(return_value=route)  # type: ignore[method-assign]
        middleware._get_barrier = MagicMock(return_value=barrier)  # type: ignore[method-assign]

        FastAPICache.reset()
        FastAPICache.init(backend=backend, prefix="api", enable=True)
        try:
            response = await middleware.dispatch(_make_request(app), AsyncMock(return_value=Response(status_code=200)))
        finally:
            FastAPICache.reset()

        assert response.status_code == 503
        assert json.loads(response.body) == {"detail": {"msg": "Temporarily unavailable"}}
        assert response.headers["X-FastAPI-Cache"] == "ERROR"
        assert response.headers["Cache-Control"] == "max-age=9"
        barrier.try_acquire.assert_not_awaited()
