import hashlib
from typing import Any, Callable, Dict, Optional, Tuple

from starlette.requests import Request
from starlette.responses import Response


async def default_key_builder(
    func: Callable[..., Any],
    namespace: str = "",
    *,
    request: Optional[Request] = None,
    response: Optional[Response] = None,
    args: Tuple[Any, ...],
    kwargs: Dict[str, Any],
) -> str:
    if kwargs.get("cache_header") is not None:
        kwargs.pop("cache_header")

    method = ""
    url_path = ""
    query = ""
    body_hash = ""
    if request:
        method = request.method.lower()
        url_path = request.url.path
        query = str(sorted(request.query_params.multi_items()))
        if request.method.upper() not in {"GET", "HEAD"}:
            body = await request.body()
            if body:
                body_hash = hashlib.md5(body).hexdigest()  # noqa: S324

    args_repr = "" if request else f"{args}:{kwargs}"
    cache_key = hashlib.md5(  # noqa: S324
        f"{func.__module__}:{func.__name__}:{method}:{url_path}:{query}:{body_hash}:{args_repr}".encode()
    ).hexdigest()
    return f"{namespace}:{method}:{url_path}:{query}:{body_hash}:{cache_key}"
