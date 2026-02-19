from functools import wraps
from inspect import Parameter, Signature, isawaitable, iscoroutinefunction
from typing import Awaitable, Callable, List, Optional, ParamSpec, Type, TypeVar, Union, cast

from fastapi.concurrency import run_in_threadpool
from fastapi.dependencies.utils import get_typed_return_annotation, get_typed_signature
from py_common_lib.logger import EPMPYLogger, audit_types
from py_common_lib.logger.utils import RUNTIME
from redis.exceptions import ConnectionError
from starlette.requests import Request
from starlette.responses import Response
from starlette.status import HTTP_304_NOT_MODIFIED

from src.cache import FastAPICache
from src.cache.coder import Coder
from src.cache.types import CacheHeaderEnum, KeyBuilder

logger: EPMPYLogger = EPMPYLogger(__name__)
P = ParamSpec("P")
R = TypeVar("R")


def _augment_signature(signature: Signature, *extra: Parameter) -> Signature:
    if not extra:
        return signature

    parameters = list(signature.parameters.values())
    variadic_keyword_params: List[Parameter] = []
    while parameters and parameters[-1].kind is Parameter.VAR_KEYWORD:
        variadic_keyword_params.append(parameters.pop())

    return signature.replace(parameters=[*parameters, *extra, *variadic_keyword_params])


def _locate_param(sig: Signature, dep: Parameter, to_inject: List[Parameter]) -> Parameter:
    """Locate an existing parameter in the decorated endpoint

    If not found, returns the injectable parameter, and adds it to the to_inject list.

    """
    param = next((p for p in sig.parameters.values() if p.annotation is dep.annotation), None)
    if param is None:
        to_inject.append(dep)
        param = dep
    return param


def _uncacheable(request: Optional[Request]) -> bool:
    """Determine if this request should not be cached

    Returns true if:
    - Caching has been disabled globally
    - The request has a Cache-Control header with a value of "no-store"

    """
    if not FastAPICache.get_enable():
        return True
    if request is None:
        return False
    return request.headers.get("Cache-Control") == CacheHeaderEnum.NO_STORE


def cache(
    expire: Optional[int] = None,
    coder: Optional[Type[Coder]] = None,
    key_builder: Optional[KeyBuilder] = None,
    namespace: str = "",
    injected_dependency_namespace: str = "__fastapi_cache",
) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[Union[R, Response]]]]:
    """
    cache all function
    :param injected_dependency_namespace:
    :param namespace:
    :param expire:
    :param coder:
    :param key_builder:

    :return:
    """

    injected_request = Parameter(
        name=f"{injected_dependency_namespace}_request",
        annotation=Request,
        kind=Parameter.KEYWORD_ONLY,
    )
    injected_response = Parameter(
        name=f"{injected_dependency_namespace}_response",
        annotation=Response,
        kind=Parameter.KEYWORD_ONLY,
    )

    def wrapper(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[Union[R, Response]]]:
        # get_typed_signature ensures that any forward references are resolved first
        wrapped_signature = get_typed_signature(func)
        to_inject: List[Parameter] = []
        request_param = _locate_param(wrapped_signature, injected_request, to_inject)
        response_param = _locate_param(wrapped_signature, injected_response, to_inject)
        return_type = get_typed_return_annotation(func)

        @wraps(func)
        async def inner(*args: P.args, **kwargs: P.kwargs) -> Union[R, Response]:
            nonlocal coder
            nonlocal expire
            nonlocal key_builder

            async def ensure_async_func(*args: P.args, **kwargs: P.kwargs) -> R:
                """Run cached sync functions in thread pool just like FastAPI."""
                # if the wrapped function does NOT have request or response in
                # its function signature, make sure we don't pass them in as
                # keyword arguments
                kwargs.pop(injected_request.name, None)
                kwargs.pop(injected_response.name, None)

                if iscoroutinefunction(func):
                    # async, return as is.
                    # unintuitively, we have to await once here, so that caller
                    # does not have to await twice. See
                    # https://stackoverflow.com/a/59268198/532513
                    return await func(*args, **kwargs)
                else:
                    # sync, wrap in thread and return async
                    # see above why we have to await even although caller also awaits.
                    return await run_in_threadpool(func, *args, **kwargs)  # type: ignore[arg-type]

            copy_kwargs = kwargs.copy()
            request: Optional[Request] = copy_kwargs.pop(request_param.name, None)  # type: ignore[assignment]
            response: Optional[Response] = copy_kwargs.pop(response_param.name, None)  # type: ignore[assignment]

            if _uncacheable(request):
                return await ensure_async_func(*args, **kwargs)

            prefix = FastAPICache.get_prefix()
            coder = coder or FastAPICache.get_coder()
            expire = expire or FastAPICache.get_expire()
            key_builder = key_builder or FastAPICache.get_key_builder()
            backend = FastAPICache.get_backend()
            cache_status_header = FastAPICache.get_cache_status_header()
            cache_key = key_builder(
                func,
                f"{prefix}:{namespace}",
                request=request,
                response=response,
                args=args,
                kwargs=copy_kwargs,
            )
            if request is not None:
                object_id = request.url.path
            else:
                object_id = RUNTIME

            object_name = (
                return_type.__args__[0].__qualname__
                if getattr(return_type, "__args__", None)
                else return_type.__qualname__
            )
            names_body = copy_kwargs.get("names")
            if names_body is not None:
                object_id += f"?names={names_body}"
            if isawaitable(cache_key):
                cache_key = await cache_key
            assert isinstance(cache_key, str)  # noqa: S101  # assertion is a type guard

            try:
                ttl, cached = await backend.get_with_ttl(cache_key)
            except ConnectionError:
                logger.warning("Error retrieving cache key '%s' from backend: Connection Error", cache_key)
                ttl, cached = 0, None
            except Exception:  # noqa: PIE786  # unkown assertion
                logger.exception(
                    "Error retrieving cache key '%s' from backend:",
                    cache_key,
                )
                ttl, cached = 0, None
            if cached is None or (
                request is not None and request.headers.get("Cache-Control") == CacheHeaderEnum.NO_CACHE
            ):  # cache miss
                result = await ensure_async_func(*args, **kwargs)
                to_cache = coder.encode(result)

                try:
                    await backend.set(cache_key, to_cache, expire)
                    logger.debug("The result of the function %s has been cached.", func.__qualname__)
                except ConnectionError:
                    logger.exception("Error setting cache key '%s' in backend: Connection Error", cache_key)
                except Exception:  # noqa: PIE786  # unkown assertion
                    logger.exception("Error setting cache key '%s' in backend:", cache_key)

                if response:
                    response.headers.update(
                        {
                            "Cache-Control": f"max-age={expire}",
                            "ETag": f"W/{hash(to_cache)}",
                            cache_status_header: "MISS",
                        }
                    )

            else:  # cache hit
                if response:
                    etag = f"W/{hash(cached)}"
                    response.headers.update(
                        {
                            "Cache-Control": f"max-age={ttl}",
                            "ETag": etag,
                            cache_status_header: "HIT",
                        }
                    )

                    if_none_match = request.headers.get("if-none-match") if request else None
                    if if_none_match == etag:
                        response.status_code = HTTP_304_NOT_MODIFIED
                        logger.debug("The result of the function %s is taken from the cache.", func.__qualname__)
                        logger.audit(audit_types.C1, object_id=object_id, object_name=object_name)

                        return response

                result = cast(R, coder.decode_as_type(cached, type_=return_type))
                logger.audit(audit_types.C1, object_id=object_id, object_name=object_name)
                logger.debug("The result of the function %s is taken from the cache.", func.__qualname__)
            return result

        inner.__signature__ = _augment_signature(wrapped_signature, *to_inject)  # type: ignore[attr-defined]

        # Metadata for early-cache middleware to identify cached endpoints
        # and reuse the same key builder/coder configuration.
        inner.__fastapi_cache_config__ = {  # type: ignore[attr-defined]
            "namespace": namespace,
            "expire": expire,
            "coder": coder,
            "key_builder": key_builder,
        }

        return inner

    return wrapper
