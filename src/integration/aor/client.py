"""Клиент для работы с PV Dictionaries"""

from typing import Optional
from urllib.parse import urljoin

from httpx import AsyncClient, HTTPStatusError, TransportError
from py_common_lib.consts import SERVER_ERRORS
from py_common_lib.logger import EPMPYLogger
from py_common_lib.logger.audit_types import F9
from py_common_lib.starlette_context_plugins import AuthorizationPlugin
from py_common_lib.utils.headers import get_standard_headers
from py_common_lib.utils.parsers import get_host_and_dns_name_by_url
from starlette_context import context

from src.config import settings
from src.integration.aor.model import CreateAorCommand

logger = EPMPYLogger(__name__)


class ClientAOR:
    """Клиент для работы с AOR client"""

    def __init__(self) -> None:
        self.headers = get_standard_headers()
        self.headers.update(
            {
                settings.AOR_AUTH_HEADER: context.get(AuthorizationPlugin.key),
                settings.AOR_ACCESS_HEADER: settings.AOR_SECRET_KEY.get_secret_value(),
            }
        )
        self.headers = {
            header: header_value for header, header_value in self.headers.items() if header_value is not None
        }
        if not settings.AOR_URL:
            raise ValueError("Переменная AOR_URL не найдена.")

    async def send_request(self, aor_command: Optional[CreateAorCommand] = None) -> None:
        """Создать DataStorage."""
        if not settings.ENABLE_AOR or not aor_command:
            return None
        try:
            async with AsyncClient(
                headers=self.headers,
                timeout=settings.AOR_HTTP_TIMEOUT,
            ) as client:
                url = urljoin(settings.AOR_URL, settings.AOR_PUSH_URL)
                logger.info("Sending AOR request to %s", url)
                body = aor_command.model_dump(mode="json", by_alias=True)
                logger.debug("AOR request body: %s", body)
                response = await client.post(url=url, json=body)
                response.raise_for_status()
                logger.info("AOR request has been sended to %s", url)
        except TransportError:
            logger.exception("Failed send to AOR: TransportError")
            ip_address, dns_name = get_host_and_dns_name_by_url(settings.PV_DICTIONARIES_URL)
            logger.audit(F9, host=ip_address if ip_address else "unknown", dns_name=dns_name)
        except HTTPStatusError as exc:
            logger.exception("Failed send to AOR: HTTPStatusError. %s", exc.response.text)
            if exc.response.status_code in SERVER_ERRORS:
                ip_address, dns_name = get_host_and_dns_name_by_url(settings.PV_DICTIONARIES_URL)
                logger.audit(F9, host=ip_address if ip_address else "unknown", dns_name=dns_name)
        except Exception:
            logger.exception("Failed send to AOR: Exception")
        return None
