from unittest.mock import AsyncMock, call

from src.cache import FastAPICache


class TestFastAPICacheClear:

    async def test_clear_removes_singleflight_namespace_too(self) -> None:
        backend = AsyncMock()
        backend.clear = AsyncMock(side_effect=[2, 1, 1, 1])

        FastAPICache.reset()
        FastAPICache.init(backend=backend, prefix="api", enable=True)

        try:
            cleared = await FastAPICache.clear(namespace="models")
        finally:
            FastAPICache.reset()

        assert cleared == 5
        assert backend.clear.await_args_list == [
            call("api:models", None),
            call("singleflight:api:models", None),
            call("singleflight:error:api:models", None),
            call("singleflight:error-count:api:models", None),
        ]
