from unittest.mock import AsyncMock, MagicMock

import pytest

from src.cache.singleflight import REDIS_ERROR, SingleflightBarrier


class TestSingleflightBarrier:

    async def test_try_acquire_returns_token_when_lock_acquired(self) -> None:
        redis = MagicMock()
        redis.set = AsyncMock(return_value=True)

        barrier = SingleflightBarrier(redis=redis, lock_ttl=15)

        acquired, token = await barrier.try_acquire("cache-key")

        assert acquired is True
        assert token
        redis.set.assert_awaited_once_with("singleflight:cache-key", token, nx=True, ex=15)

    async def test_try_acquire_returns_current_token_when_lock_is_held(self) -> None:
        redis = MagicMock()
        redis.set = AsyncMock(return_value=False)
        redis.get = AsyncMock(return_value=b"current-token")

        barrier = SingleflightBarrier(redis=redis)

        acquired, token = await barrier.try_acquire("cache-key")

        assert acquired is False
        assert token == "current-token"
        redis.get.assert_awaited_once_with("singleflight:cache-key")

    @pytest.mark.parametrize(
        ("raw_token", "expected_token"),
        [
            (None, None),
            (b"token-from-bytes", "token-from-bytes"),
            ("token-from-str", "token-from-str"),
        ],
    )
    async def test_get_lock_token_returns_normalized_value(self, raw_token: object, expected_token: object) -> None:
        redis = MagicMock()
        redis.get = AsyncMock(return_value=raw_token)

        barrier = SingleflightBarrier(redis=redis)

        token = await barrier.get_lock_token("cache-key")

        assert token == expected_token
        redis.get.assert_awaited_once_with("singleflight:cache-key")

    @pytest.mark.parametrize(("eval_result", "expected_acquired"), [(1, True), (0, False)])
    async def test_force_acquire_returns_expected_result(self, eval_result: int, expected_acquired: bool) -> None:
        redis = MagicMock()
        redis.eval = AsyncMock(return_value=eval_result)

        barrier = SingleflightBarrier(redis=redis, lock_ttl=12)

        acquired, token = await barrier.force_acquire("cache-key", "old-token")

        assert acquired is expected_acquired
        if expected_acquired:
            assert token
        else:
            assert token == ""

        redis.eval.assert_awaited_once()
        _, _, lock_key, expected_old_token, new_token, ttl = redis.eval.await_args.args
        assert lock_key == "singleflight:cache-key"
        assert expected_old_token == "old-token"
        assert ttl == "12"
        if expected_acquired:
            assert token == new_token

    async def test_release_swallows_redis_exception(self) -> None:
        redis = MagicMock()
        redis.eval = AsyncMock(side_effect=RuntimeError("redis error"))

        barrier = SingleflightBarrier(redis=redis)

        await barrier.release("cache-key", "token")

        redis.eval.assert_awaited_once()

    async def test_safe_read_token_returns_redis_error_on_exception(self) -> None:
        redis = MagicMock()
        barrier = SingleflightBarrier(redis=redis)
        barrier.get_lock_token = AsyncMock(side_effect=RuntimeError("redis error"))  # type: ignore[method-assign]

        result = await barrier.safe_read_token("cache-key")

        assert result is REDIS_ERROR

    async def test_safe_acquire_and_force_acquire_return_none_on_exception(self) -> None:
        redis = MagicMock()
        barrier = SingleflightBarrier(redis=redis)
        barrier.try_acquire = AsyncMock(side_effect=RuntimeError("redis error"))  # type: ignore[method-assign]
        barrier.force_acquire = AsyncMock(side_effect=RuntimeError("redis error"))  # type: ignore[method-assign]

        acquire_result = await barrier.safe_acquire("cache-key")
        force_acquire_result = await barrier.safe_force_acquire("cache-key", "old-token")

        assert acquire_result is None
        assert force_acquire_result is None
