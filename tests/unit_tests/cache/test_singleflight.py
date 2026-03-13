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
        await_args = redis.eval.await_args
        assert await_args is not None
        _, _, lock_key, expected_old_token, new_token, ttl = await_args.args
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

    async def test_get_error_ttl_returns_ttl_when_error_state_exists(self) -> None:
        redis = MagicMock()
        redis.get = AsyncMock(return_value=b"1")
        redis.ttl = AsyncMock(return_value=7)

        barrier = SingleflightBarrier(redis=redis)

        ttl = await barrier.get_error_ttl("cache-key")

        assert ttl == 7
        redis.get.assert_awaited_once_with("singleflight:error:cache-key")
        redis.ttl.assert_awaited_once_with("singleflight:error:cache-key")

    async def test_open_error_state_and_clear_error_state_use_expected_keys(self) -> None:
        redis = MagicMock()
        redis.set = AsyncMock()
        redis.delete = AsyncMock()

        barrier = SingleflightBarrier(redis=redis, error_ttl=10)

        await barrier.open_error_state("cache-key")
        await barrier.clear_error_state("cache-key")

        redis.set.assert_awaited_once_with("singleflight:error:cache-key", "1", ex=10)
        redis.delete.assert_awaited_once_with(
            "singleflight:error:cache-key",
            "singleflight:error-count:cache-key",
        )

    async def test_increment_owner_error_count_returns_new_value(self) -> None:
        redis = MagicMock()
        redis.eval = AsyncMock(return_value=4)

        barrier = SingleflightBarrier(redis=redis, error_counter_ttl=10)

        count = await barrier.increment_owner_error_count("cache-key")

        assert count == 4
        redis.eval.assert_awaited_once()
        await_args = redis.eval.await_args
        assert await_args is not None
        assert await_args.args[1:] == (
            1,
            "singleflight:error-count:cache-key",
            "10",
        )

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

    async def test_safe_error_helpers_return_fallbacks_on_exception(self) -> None:
        redis = MagicMock()
        barrier = SingleflightBarrier(redis=redis)
        barrier.get_error_ttl = AsyncMock(side_effect=RuntimeError("redis error"))  # type: ignore[method-assign]
        barrier.open_error_state = AsyncMock(side_effect=RuntimeError("redis error"))  # type: ignore[method-assign]
        barrier.increment_owner_error_count = AsyncMock(side_effect=RuntimeError("redis error"))  # type: ignore[method-assign]
        barrier.clear_error_state = AsyncMock(side_effect=RuntimeError("redis error"))  # type: ignore[method-assign]

        ttl = await barrier.safe_get_error_ttl("cache-key")
        cache_result = await barrier.safe_open_error_state("cache-key")
        error_count = await barrier.safe_increment_owner_error_count("cache-key")
        await barrier.safe_clear_error_state("cache-key")

        assert ttl is None
        assert cache_result is False
        assert error_count is None
