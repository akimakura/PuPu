from typing import Any, Dict

from fastapi import HTTPException
from py_common_lib.logger import EPMPYLogger

logger = EPMPYLogger(__name__)


class HTTPExceptionWithAuditLogging(HTTPException):

    def __init__(
        self,
        audit_kwargs: Dict[str, Any],
        status_code: int,
        detail: Any = None,
        headers: Dict[str, str] | None = None,
    ) -> None:
        logger.audit(**audit_kwargs)
        super().__init__(status_code, detail, headers)


class PVDException(Exception):
    """Ошибка PVDictionaries."""


class CreatePVDException(Exception):
    """Ошибка создания справочника в PVDictionaries."""
