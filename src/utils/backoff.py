"""
Backoff метод для реализации обработки ошибок.
"""

import logging
import time
from functools import wraps
from typing import Any, Callable

from aioretry.retry import ParamBeforeRetry, ParamRetryPolicy, RetryInfo, TargetFunction, get_method, perform
from pydantic import ValidationError
from sqlalchemy.exc import NoResultFound

from src.utils.exceptions import CreatePVDException, PVDException

base_logger = logging.getLogger(__name__)

black_list_exceptions = (
    ValueError,
    NoResultFound,
    PVDException,
    ValidationError,
    CreatePVDException,
    KeyError,
    AttributeError,
    IndexError,
)


def retry(
    retry_policy: ParamRetryPolicy, before_retry: ParamBeforeRetry | None = None
) -> Callable[[TargetFunction], TargetFunction]:
    """Creates a decorator function

    Args:
        retry_policy (RetryPolicy, str): the retry policy
        before_retry (BeforeRetry, str, None): the function to be called after each failure of fn
                                               and before the corresponding retry.

    Returns:
        A wrapped function which accepts the same arguments as fn and returns an Awaitable

    Usage:
        @retry(retry_policy)
        async def coro_func():
            ...
    """

    def wrapper(fn: TargetFunction) -> TargetFunction:
        @wraps(fn)
        async def wrapped(*args: list[Any], **kwargs: dict[Any, Any]) -> Any:
            return await perform(
                fn,
                get_method(retry_policy, args, "retry_policy"),
                get_method(before_retry, args, "before_retry") if before_retry is not None else None,
                *args,
                **kwargs,
            )

        return wrapped

    return wrapper


def sync_retry(retries: int = 3, delay: int = 0) -> Callable:
    """Синхронная версия декоратора retry"""

    def decorator(func: Callable) -> Callable:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exception = None
            for attempt in range(1, retries + 1):
                try:
                    base_logger.info("Attempt %s to execute %s", attempt, func.__name__)
                    return func(*args, **kwargs)
                except Exception as e:
                    base_logger.exception("Error on attempt %s of %s", attempt, func.__name__)
                    last_exception = e
                    if attempt < retries:
                        time.sleep(delay)
            # Если все попытки не удались, пробросить последнее исключение
            if last_exception:
                raise last_exception

        return wrapper

    return decorator


class RetryConfig:
    def __init__(
        self,
        exceptions: (Exception,) = Exception,  # type: ignore
        tries: int = 3,
        delay: float = 0.01,
        max_delay: float = 0.5,
        multiplier: float = 3,
        logger: logging.Logger | None = base_logger,
        black_list_exceptions: tuple = black_list_exceptions,
    ) -> None:
        self.exceptions = exceptions
        self.tries = tries
        self.delay = delay
        self.logger = logger
        self.multiplier = multiplier
        self.max_delay = max_delay
        self.black_list_exceptions = black_list_exceptions

    def __call__(self, info: RetryInfo) -> tuple[bool, float]:
        exception_message = str(info.exception)
        if self.logger is not None:
            new_delay = self.delay * self.multiplier
            self.delay = new_delay if new_delay < self.max_delay else self.max_delay
            self.logger.exception(
                "Backoff retry [%s] in %s seconds... (exception: %s)",
                info.fails,
                self.delay,
                exception_message,
            )
        if (
            "(UNKNOWN_TABLE)" in exception_message
            or "(UNKNOWN_DATABASE)" in exception_message
            or "(ACCESS_DENIED)" in exception_message
            or "UndefinedTableError" in exception_message
            or "TABLE_ALREADY_EXISTS" in exception_message
        ):
            return True, 0
        for black_exception in self.black_list_exceptions:
            if isinstance(info.exception, black_exception):
                return True, 0
        return (
            isinstance(info.exception, self.exceptions) and info.fails > self.tries,
            self.delay,
        )
