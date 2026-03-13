import json
from unittest.mock import AsyncMock, MagicMock

from fastapi import FastAPI
from starlette.requests import Request
from starlette.responses import Response

from src.cache.middleware import ReadOnlyCacheMiddleware


def _make_request() -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/cache-test",
            "query_string": b"",
            "headers": [],
        }
    )


class TestReadOnlyCacheMiddlewareSingleflightWait:

    async def test_waiter_becomes_new_owner_after_timeout(self, monkeypatch) -> None:  # type: ignore[no-untyped-def]
        sleep_mock = AsyncMock(return_value=None)
        monkeypatch.setattr("src.cache.middleware.asyncio.sleep", sleep_mock)

        middleware = ReadOnlyCacheMiddleware(FastAPI())
        middleware._poll_cache = AsyncMock(return_value=None)  # type: ignore[method-assign]

        barrier = MagicMock()
        barrier.poll_interval = 1.0
        barrier.wait_timeout = 1.0
        barrier.safe_read_token = AsyncMock(return_value="token-1")
        barrier.safe_get_error_ttl = AsyncMock(return_value=None)
        barrier.safe_force_acquire = AsyncMock(return_value="token-2")
        barrier.safe_acquire = AsyncMock(return_value=None)
        barrier.safe_clear_error_state = AsyncMock()
        barrier.release = AsyncMock()

        downstream_response = Response(content=b"[]", media_type="application/json")
        call_next = AsyncMock(return_value=downstream_response)

        response = await middleware._singleflight_wait(
            "cache-key",
            "token-1",
            MagicMock(),
            barrier,
            "X-FastAPI-Cache",
            _make_request(),
            call_next,
        )

        assert response is downstream_response
        barrier.safe_force_acquire.assert_awaited_once_with("cache-key", "token-1")
        call_next.assert_awaited_once()
        barrier.release.assert_awaited_once_with("cache-key", "token-2")

    async def test_waiter_keeps_waiting_for_new_owner_and_returns_cached_response(
        self,
        monkeypatch,
    ) -> None:  # type: ignore[no-untyped-def]
        sleep_mock = AsyncMock(return_value=None)
        monkeypatch.setattr("src.cache.middleware.asyncio.sleep", sleep_mock)

        middleware = ReadOnlyCacheMiddleware(FastAPI())
        cached_response = Response(content=b"[]", media_type="application/json")
        middleware._poll_cache = AsyncMock(side_effect=[None, cached_response])  # type: ignore[method-assign]

        barrier = MagicMock()
        barrier.poll_interval = 1.0
        barrier.wait_timeout = 1.0
        barrier.safe_read_token = AsyncMock(side_effect=["token-1", "token-2"])
        barrier.safe_get_error_ttl = AsyncMock(return_value=None)
        barrier.safe_force_acquire = AsyncMock(return_value=None)
        barrier.safe_acquire = AsyncMock(return_value=None)
        barrier.release = AsyncMock()

        call_next = AsyncMock(return_value=Response(status_code=500))

        response = await middleware._singleflight_wait(
            "cache-key",
            "token-1",
            MagicMock(),
            barrier,
            "X-FastAPI-Cache",
            _make_request(),
            call_next,
        )

        assert response is cached_response
        barrier.safe_force_acquire.assert_awaited_once_with("cache-key", "token-1")
        call_next.assert_not_awaited()
        barrier.release.assert_not_awaited()

    async def test_waiter_returns_cached_error_when_error_cache_appears(
        self,
        monkeypatch,
    ) -> None:  # type: ignore[no-untyped-def]
        sleep_mock = AsyncMock(return_value=None)
        monkeypatch.setattr("src.cache.middleware.asyncio.sleep", sleep_mock)

        middleware = ReadOnlyCacheMiddleware(FastAPI())
        middleware._poll_cache = AsyncMock(return_value=None)  # type: ignore[method-assign]

        barrier = MagicMock()
        barrier.poll_interval = 1.0
        barrier.wait_timeout = 5.0
        barrier.safe_get_error_ttl = AsyncMock(side_effect=[None, 7])
        barrier.safe_read_token = AsyncMock(return_value="token-1")
        barrier.safe_force_acquire = AsyncMock(return_value=None)
        barrier.safe_acquire = AsyncMock(return_value=None)
        barrier.release = AsyncMock()

        call_next = AsyncMock(return_value=Response(status_code=500))

        response = await middleware._singleflight_wait(
            "cache-key",
            "token-1",
            MagicMock(),
            barrier,
            "X-FastAPI-Cache",
            _make_request(),
            call_next,
        )

        assert response.status_code == 503
        assert json.loads(response.body) == {"detail": {"msg": "Temporarily unavailable"}}
        assert response.headers["Cache-Control"] == "max-age=7"
        assert response.headers["X-FastAPI-Cache"] == "ERROR"
        call_next.assert_not_awaited()
        barrier.release.assert_not_awaited()
