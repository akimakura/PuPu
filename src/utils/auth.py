import jwt
from typing import Optional

from py_common_lib.logger.utils import BEARER, SYSTEM, RUNTIME
from fastapi import Depends, HTTPException
from fastapi.security import APIKeyHeader
from src.config import settings

X_API_KEY = APIKeyHeader(name=settings.APP_SECRET_HEADER, auto_error=False)


def get_user_login_by_token(auth_token: Optional[str]) -> str:
    """
    Получить пользователя из токена
    """
    if auth_token is None:
        return SYSTEM
    if BEARER in auth_token:
        auth_token = auth_token.replace(BEARER, "")
    try:
        decoded = jwt.decode(auth_token, options={"verify_signature": False})
    except jwt.DecodeError:
        return RUNTIME
    return decoded.get("sberpdi", decoded.get("sub", RUNTIME))


def api_key_auth(x_api_key: str = Depends(X_API_KEY)) -> None:
    """Проверяет API key из заголовка X-ACCESS-TOKEN."""
    if not settings.APP_SECRET_KEY:
        return None
    if x_api_key != settings.APP_SECRET_KEY.get_secret_value():
        raise HTTPException(
            status_code=401,
            detail=f"Invalid API Key. Check that you are passing a '{settings.APP_SECRET_HEADER}' on your header.",
        )
