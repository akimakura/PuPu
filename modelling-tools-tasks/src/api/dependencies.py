from fastapi import Request
from py_common_lib.starlette_context_plugins import AuthorizationPlugin, ClientHostPlugin
from py_common_lib.utils import get_standard_headers
from starlette_context import context

from src.utils.context import CallContext


def get_call_context(request: Request) -> CallContext:
    context_obj = CallContext()
    headers = get_standard_headers()
    headers.update({
        AuthorizationPlugin.key: str(context.get(AuthorizationPlugin.key, "")),
        ClientHostPlugin.key: str(context.get(ClientHostPlugin.key, "")),
    })
    context_obj.set_context_from_request(request, headers)
    return context_obj
