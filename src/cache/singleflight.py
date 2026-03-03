import asyncio
import logging
import uuid
from typing import Optional, Tuple, Union

from redis.asyncio import Redis, RedisCluster

logger = logging.getLogger(__name__)

_RELEASE_LOCK_SCRIPT = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
"""

_FORCE_ACQUIRE_SCRIPT = """
local current = redis.call("GET", KEYS[1])
if current == false then
    local ok = redis.call("SET", KEYS[1], ARGV[2], "NX", "EX", ARGV[3])
    if ok then
        return 1
    else
        return 0
    end
elseif current == ARGV[1] then
    redis.call("SET", KEYS[1], ARGV[2], "EX", ARGV[3])
    return 1
else
    return 0
end
"""


class SingleflightBarrier:
    """Распределённый барьер для паттерна Singleflight на основе Redis.

    Гарантирует, что при одновременных запросах к одному и тому же
    незакешированному ресурсу только один запрос выполняет реальное
    обращение к источнику данных, а остальные ожидают появления результата
    в кэше.

    Механизм работы:
    - Первый запрос захватывает блокировку (SET NX) и выполняет загрузку данных.
    - Остальные запросы обнаруживают блокировку и поллят кэш, пока данные
      не появятся или не истечёт таймаут.
    - При таймауте один из ожидающих запросов может принудительно перехватить
      блокировку через CAS (compare-and-swap), а остальные продолжают ждать
      нового владельца блокировки.
    """

    LOCK_KEY_PREFIX = "singleflight:"

    def __init__(
        self,
        redis: Union["Redis[bytes]", "RedisCluster[bytes]"],
        lock_ttl: int = 30,
        wait_timeout: float = 10.0,
        poll_interval: float = 0.1,
    ) -> None:
        self._redis = redis
        self._lock_ttl = lock_ttl
        self._wait_timeout = wait_timeout
        self._poll_interval = poll_interval

    def _lock_key(self, cache_key: str) -> str:
        """Формирует ключ блокировки на основе ключа кэша."""
        return f"{self.LOCK_KEY_PREFIX}{cache_key}"

    async def try_acquire(self, cache_key: str) -> Tuple[bool, str]:
        """Попытка захватить блокировку для данного ключа кэша.

        Возвращает кортеж ``(acquired, token)``:

        - ``acquired=True``, ``token`` — наш токен. Вызывающий должен выполнить
          запрос к источнику данных и затем вызвать :meth:`release`.
        - ``acquired=False``, ``token`` — токен текущего владельца блокировки.
          Вызывающий должен ожидать появления данных в кэше.
        """
        lock_key = self._lock_key(cache_key)
        token = uuid.uuid4().hex

        acquired = await self._redis.set(lock_key, token, nx=True, ex=self._lock_ttl)

        if acquired:
            logger.debug("Singleflight: lock acquired, key=%s", cache_key)
            return True, token

        current_raw = await self._redis.get(lock_key)
        current_token = current_raw.decode() if isinstance(current_raw, bytes) else (current_raw or "")
        logger.debug("Singleflight: lock busy, key=%s, holder=%s…", cache_key, current_token[:8])
        return False, current_token

    async def release(self, cache_key: str, token: str) -> None:
        """Освобождает блокировку, если мы всё ещё являемся её владельцем.

        Используется атомарный Lua-скрипт: блокировка удаляется только
        при совпадении текущего значения с переданным ``token``.
        """
        lock_key = self._lock_key(cache_key)
        try:
            result = await self._redis.eval(_RELEASE_LOCK_SCRIPT, 1, lock_key, token)
            if result:
                logger.debug("Singleflight: lock released, key=%s", cache_key)
            else:
                logger.debug("Singleflight: lock already expired/replaced, key=%s", cache_key)
        except Exception:
            logger.exception("Singleflight: error releasing lock, key=%s", cache_key)

    async def get_lock_token(self, cache_key: str) -> Optional[str]:
        """Возвращает текущий токен блокировки или ``None``, если блокировка отсутствует."""
        lock_key = self._lock_key(cache_key)
        raw = await self._redis.get(lock_key)
        if raw is None:
            return None
        return raw.decode() if isinstance(raw, bytes) else raw

    async def force_acquire(self, cache_key: str, expected_old_token: str) -> Tuple[bool, str]:
        """Принудительный захват блокировки при таймауте ожидания.

        Атомарно (через Lua-скрипт с CAS) заменяет блокировку новым
        токеном, только если текущее значение совпадает с
        ``expected_old_token`` либо блокировка уже истекла.

        Если блокировка была заменена другим запросом (другой токен),
        операция завершается неудачей — это предотвращает одновременный
        принудительный захват несколькими ожидающими запросами.
        """
        lock_key = self._lock_key(cache_key)
        new_token = uuid.uuid4().hex

        try:
            result = await self._redis.eval(
                _FORCE_ACQUIRE_SCRIPT,
                1,
                lock_key,
                expected_old_token,
                new_token,
                str(self._lock_ttl),
            )
        except Exception:
            logger.exception("Singleflight: error during force acquire, key=%s", cache_key)
            return False, ""

        if result:
            logger.debug("Singleflight: lock force-acquired, key=%s", cache_key)
            return True, new_token

        logger.debug("Singleflight: force acquire failed (lock replaced), key=%s", cache_key)
        return False, ""

    @property
    def wait_timeout(self) -> float:
        """Таймаут ожидания кэша на одного владельца блокировки (секунды)."""
        return self._wait_timeout

    @property
    def poll_interval(self) -> float:
        """Интервал опроса кэша (секунды)."""
        return self._poll_interval
