import logging
import uuid
from typing import Any, Optional, Tuple, Union

from redis.asyncio import Redis, RedisCluster

logger = logging.getLogger(__name__)

REDIS_ERROR = object()

_RELEASE_LOCK_SCRIPT = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
"""

_INCREMENT_ERROR_COUNT_SCRIPT = """
local count = redis.call("INCR", KEYS[1])
if count == 1 then
    redis.call("EXPIRE", KEYS[1], ARGV[1])
end
return count
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
    в кеше.

    Механизм работы:
    - Первый запрос захватывает блокировку через ``SET NX`` и выполняет загрузку данных.
    - Остальные запросы обнаруживают блокировку и опрашивают кеш,
      пока данные не появятся или не истечёт таймаут.
    - При таймауте один из ожидающих запросов может принудительно
      перехватить блокировку через CAS, а остальные продолжают ждать.
    """

    LOCK_KEY_PREFIX = "singleflight:"
    ERROR_KEY_PREFIX = "singleflight:error:"
    ERROR_COUNT_KEY_PREFIX = "singleflight:error-count:"

    def __init__(
        self,
        redis: Union["Redis[bytes]", "RedisCluster[bytes]"],
        lock_ttl: int = 30,
        wait_timeout: float = 10.0,
        poll_interval: float = 0.1,
        max_owner_errors: int = 4,
        error_ttl: int = 10,
        error_counter_ttl: int = 10,
    ) -> None:
        self._redis = redis
        self._lock_ttl = lock_ttl
        self._wait_timeout = wait_timeout
        self._poll_interval = poll_interval
        self._max_owner_errors = max_owner_errors
        self._error_ttl = error_ttl
        self._error_counter_ttl = error_counter_ttl

    def _lock_key(self, cache_key: str) -> str:
        """Формирует ключ блокировки на основе ключа кеша."""
        return f"{self.LOCK_KEY_PREFIX}{cache_key}"

    def _error_key(self, cache_key: str) -> str:
        """Формирует ключ закешированной ошибки."""
        return f"{self.ERROR_KEY_PREFIX}{cache_key}"

    def _error_count_key(self, cache_key: str) -> str:
        """Формирует ключ счётчика ошибок владельца блокировки."""
        return f"{self.ERROR_COUNT_KEY_PREFIX}{cache_key}"

    @staticmethod
    def _decode_redis_value(raw: Optional[Union[bytes, str]]) -> Optional[str]:
        """Нормализует значение Redis к строке."""
        if raw is None:
            return None
        return raw.decode() if isinstance(raw, bytes) else raw

    # ---- базовые операции (пробрасывают исключения) ---------------------

    async def try_acquire(self, cache_key: str) -> Tuple[bool, str]:
        """Пытается захватить блокировку для указанного ключа кеша.

        Возвращает кортеж ``(acquired, token)``:

        - ``acquired=True`` и ``token`` нашего владельца блокировки.
        - ``acquired=False`` и ``token`` текущего владельца блокировки.

        При ошибке Redis пробрасывает исключение.
        """
        lock_key = self._lock_key(cache_key)
        token = uuid.uuid4().hex

        acquired = await self._redis.set(lock_key, token, nx=True, ex=self._lock_ttl)  # type: ignore[union-attr]

        if acquired:
            return True, token

        current_raw = await self._redis.get(lock_key)  # type: ignore[union-attr]
        current_token = self._decode_redis_value(current_raw) or ""
        return False, current_token

    async def release(self, cache_key: str, token: str) -> None:
        """Освобождает блокировку, если мы всё ещё являемся её владельцем.

        Использует атомарный Lua-скрипт: блокировка удаляется только
        при совпадении текущего значения с переданным ``token``.
        """
        lock_key = self._lock_key(cache_key)
        try:
            await self._redis.eval(_RELEASE_LOCK_SCRIPT, 1, lock_key, token)  # type: ignore[union-attr]
        except Exception:
            logger.exception("Singleflight: error releasing lock, key=%s", cache_key)

    async def get_lock_token(self, cache_key: str) -> Optional[str]:
        """Возвращает текущий токен блокировки или ``None``, если блокировки нет.

        При ошибке Redis пробрасывает исключение.
        """
        lock_key = self._lock_key(cache_key)
        raw = await self._redis.get(lock_key)  # type: ignore[union-attr]
        return self._decode_redis_value(raw)

    async def get_error_ttl(self, cache_key: str) -> Optional[int]:
        """Возвращает TTL открытого error-state или ``None``, если его нет."""
        error_key = self._error_key(cache_key)
        raw = await self._redis.get(error_key)  # type: ignore[union-attr]
        if raw is None:
            return None

        ttl = await self._redis.ttl(error_key)  # type: ignore[union-attr]
        return max(int(ttl), 0)

    async def open_error_state(self, cache_key: str) -> None:
        """Открывает error-state для ключа на короткий TTL."""
        error_key = self._error_key(cache_key)
        await self._redis.set(error_key, "1", ex=self._error_ttl)  # type: ignore[union-attr]

    async def increment_owner_error_count(self, cache_key: str) -> int:
        """Увеличивает счётчик ошибок владельца и возвращает новое значение."""
        count_key = self._error_count_key(cache_key)
        result = await self._redis.eval(  # type: ignore[union-attr]
            _INCREMENT_ERROR_COUNT_SCRIPT,
            1,
            count_key,
            str(self._error_counter_ttl),
        )
        return int(result)

    async def clear_error_state(self, cache_key: str) -> None:
        """Удаляет закешированную ошибку и счётчик ошибок для ключа."""
        await self._redis.delete(  # type: ignore[union-attr]
            self._error_key(cache_key),
            self._error_count_key(cache_key),
        )

    async def force_acquire(self, cache_key: str, expected_old_token: str) -> Tuple[bool, str]:
        """Принудительно захватывает блокировку при таймауте ожидания.

        Атомарно заменяет владельца блокировки новым токеном, если текущее
        значение совпадает с ``expected_old_token`` либо блокировка уже истекла.

        При ошибке Redis пробрасывает исключение.
        """
        lock_key = self._lock_key(cache_key)
        new_token = uuid.uuid4().hex

        result = await self._redis.eval(  # type: ignore[union-attr]
            _FORCE_ACQUIRE_SCRIPT,
            1,
            lock_key,
            expected_old_token,
            new_token,
            str(self._lock_ttl),
        )

        if result:
            logger.debug("Singleflight: lock force-acquired, key=%s", cache_key)
            return True, new_token

        logger.debug("Singleflight: force acquire failed (lock replaced), key=%s", cache_key)
        return False, ""

    # ---- безопасные обёртки (для цикла ожидания) -----------------------

    async def safe_read_token(self, cache_key: str) -> Any:
        """Читает токен блокировки с подавлением ошибок Redis.

        Возвращает строковый токен, ``None`` либо :data:`REDIS_ERROR`.
        """
        try:
            return await self.get_lock_token(cache_key)
        except Exception:
            logger.exception("Singleflight: error reading lock token, key=%s", cache_key)
            return REDIS_ERROR

    async def safe_acquire(self, cache_key: str) -> Optional[str]:
        """Пытается захватить блокировку с подавлением ошибок Redis."""
        try:
            acquired, token = await self.try_acquire(cache_key)
            return token if acquired else None
        except Exception:
            logger.exception("Singleflight: error acquiring lock, key=%s", cache_key)
            return None

    async def safe_force_acquire(self, cache_key: str, expected_old_token: str) -> Optional[str]:
        """Принудительно захватывает блокировку с подавлением ошибок Redis."""
        try:
            acquired, token = await self.force_acquire(cache_key, expected_old_token)
            return token if acquired else None
        except Exception:
            logger.exception("Singleflight: force acquire error, key=%s", cache_key)
            return None

    async def safe_get_error_ttl(self, cache_key: str) -> Optional[int]:
        """Читает TTL открытого error-state с подавлением ошибок Redis."""
        try:
            return await self.get_error_ttl(cache_key)
        except Exception:
            logger.exception("Singleflight: error reading error-state, key=%s", cache_key)
            return None

    async def safe_open_error_state(self, cache_key: str) -> bool:
        """Открывает error-state с подавлением ошибок Redis."""
        try:
            await self.open_error_state(cache_key)
            return True
        except Exception:
            logger.exception("Singleflight: error opening owner error state, key=%s", cache_key)
            return False

    async def safe_increment_owner_error_count(self, cache_key: str) -> Optional[int]:
        """Увеличивает счётчик ошибок владельца с подавлением ошибок Redis."""
        try:
            return await self.increment_owner_error_count(cache_key)
        except Exception:
            logger.exception("Singleflight: error incrementing owner error count, key=%s", cache_key)
            return None

    async def safe_clear_error_state(self, cache_key: str) -> None:
        """Удаляет состояние ошибок с подавлением ошибок Redis."""
        try:
            await self.clear_error_state(cache_key)
        except Exception:
            logger.exception("Singleflight: error clearing owner error state, key=%s", cache_key)

    @property
    def wait_timeout(self) -> float:
        """Таймаут ожидания кеша для одного владельца блокировки в секундах."""
        return self._wait_timeout

    @property
    def poll_interval(self) -> float:
        """Интервал опроса кеша в секундах."""
        return self._poll_interval

    @property
    def max_owner_errors(self) -> int:
        """Порог подряд идущих ошибок владельца перед записью error-cache."""
        return self._max_owner_errors
