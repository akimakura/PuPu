"""Клиент для работы с PV Dictionaries"""

from http import HTTPStatus
from urllib.parse import urljoin

from fastapi import HTTPException
from httpx import AsyncClient, HTTPStatusError, TransportError
from py_common_lib.consts import SERVER_ERRORS
from py_common_lib.logger import EPMPYLogger
from py_common_lib.logger.audit_types import F9
from py_common_lib.starlette_context_plugins import AuthorizationPlugin
from py_common_lib.utils.headers import get_standard_headers
from py_common_lib.utils.parsers import get_host_and_dns_name_by_url
from starlette_context import context

from src.config import settings
from src.utils.cert import get_cert_and_verify_for_httpx

logger = EPMPYLogger(__name__)


class ClientWorkerManager:
    """Клиент для работы с MT Worker Manager"""

    def __init__(self) -> None:
        self.cert, self.verify = get_cert_and_verify_for_httpx(
            settings.WORKER_PATH_TO_CA_CERT,
            settings.WORKER_PATH_TO_CLIENT_CERT,
            settings.WORKER_PATH_TO_CLIENT_CERT_KEY,
            settings.WORKER_CLIENT_CERT_PASSWORD,
        )
        self.headers = get_standard_headers()
        self.headers.update({settings.WORKER_AUTH_HEADER: context.get(AuthorizationPlugin.key)})
        self.headers = {
            header: header_value for header, header_value in self.headers.items() if header_value is not None
        }
        if not settings.WORKER_MANAGER_URL:
            raise ValueError("Переменная WORKER_MANAGER_URL не найдена.")

    async def create_data_storage(
        self,
        tenant_id: str,
        model_names: list[str],
        datastorage_names: list[str],
        if_not_exists: bool = False,
        delete_if_failder: bool = False,
    ) -> None:
        """Создать DataStorage."""
        if not settings.ENABLE_GENERATE_OBJECTS:
            return None
        try:
            async with AsyncClient(
                cert=self.cert,
                verify=self.verify if self.verify else False,
                headers=self.headers,
            ) as client:
                url = urljoin(settings.WORKER_MANAGER_URL, settings.WORKER_DATASTORAGE_URL)
                url = (
                    url.replace("{tenantName}", tenant_id)
                    + "?"
                    + "ifNotExists="
                    + str(if_not_exists).lower()
                    + "&"
                    + "deleteIfFailder="
                    + str(delete_if_failder).lower()
                )
                logger.info("Sending create datastorage request to %s", url)
                body = {
                    "modelNames": model_names,
                    "datastorages": datastorage_names,
                }
                response = await client.post(url=url, json=body)
                response.raise_for_status()
                logger.info("Create datastorage request has been sended to %s", url)
        except TransportError:
            logger.exception("Failed to create DataStorage: TransportError")
            ip_address, dns_name = get_host_and_dns_name_by_url(settings.PV_DICTIONARIES_URL)
            logger.audit(F9, host=ip_address if ip_address else "unknown", dns_name=dns_name)
            raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR)
        except HTTPStatusError as exc:
            logger.exception("Failed to create DataStorage: HTTPStatusError. %s", exc.response.text)
            if exc.response.status_code in SERVER_ERRORS:
                ip_address, dns_name = get_host_and_dns_name_by_url(settings.PV_DICTIONARIES_URL)
                logger.audit(F9, host=ip_address if ip_address else "unknown", dns_name=dns_name)
            raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=exc.response.text)
        return None

    async def update_data_storage(
        self,
        tenant_id: str,
        model_names: list[str],
        datastorage_names: list[str],
        enable_delete_column: bool = True,
        enable_delete_not_empty: bool = False,
    ) -> None:
        """Создать DataStorage."""
        if not settings.ENABLE_GENERATE_OBJECTS:
            return None
        try:
            async with AsyncClient(
                cert=self.cert,
                verify=self.verify if self.verify else False,
                headers=self.headers,
            ) as client:
                url = urljoin(settings.WORKER_MANAGER_URL, settings.WORKER_DATASTORAGE_URL)
                url = (
                    url.replace("{tenantName}", tenant_id)
                    + "?"
                    + "enableDeleteColumn="
                    + str(enable_delete_column).lower()
                    + "&"
                    + "enableDeleteNotEmpty="
                    + str(enable_delete_not_empty).lower()
                )
                logger.info("Sending update datastorage request to %s", url)
                body = {
                    "modelNames": model_names,
                    "datastorages": datastorage_names,
                }
                response = await client.patch(url=url, json=body)
                response.raise_for_status()
                logger.info("Update datastorage request has been sended to %s", url)
        except TransportError:
            logger.exception("Failed to create DataStorage: TransportError")
            ip_address, dns_name = get_host_and_dns_name_by_url(settings.PV_DICTIONARIES_URL)
            logger.audit(F9, host=ip_address if ip_address else "unknown", dns_name=dns_name)
            raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR)
        except HTTPStatusError as exc:
            logger.exception("Failed to create DataStorage: HTTPStatusError. %s", exc.response.text)
            if exc.response.status_code in SERVER_ERRORS:
                ip_address, dns_name = get_host_and_dns_name_by_url(settings.PV_DICTIONARIES_URL)
                logger.audit(F9, host=ip_address if ip_address else "unknown", dns_name=dns_name)
            raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=exc.response.text)
        return None

    async def create_dimension(
        self,
        tenant_id: str,
        model_names: list[str],
        dimension_names: list[str],
        if_not_exists: bool = False,
        delete_if_failder: bool = False,
    ) -> None:
        """Создать Dimension."""
        if not settings.ENABLE_GENERATE_OBJECTS:
            return None
        try:
            async with AsyncClient(
                cert=self.cert,
                verify=self.verify if self.verify else False,
                headers=self.headers,
            ) as client:
                url = urljoin(settings.WORKER_MANAGER_URL, settings.WORKER_DIMENSION_URL)
                url = (
                    url.replace("{tenantName}", tenant_id)
                    + "?"
                    + "ifNotExists="
                    + str(if_not_exists).lower()
                    + "&"
                    + "deleteIfFailder="
                    + str(delete_if_failder).lower()
                )
                body = {
                    "modelNames": model_names,
                    "dimensions": dimension_names,
                }
                logger.info("Sending create dimension request to %s body=%s", url, body)
                response = await client.post(url=url, json=body)
                response.raise_for_status()
                logger.info("Create dimension request has been sended to %s", url)
        except TransportError:
            logger.exception("Failed to create Dimension: TransportError")
            ip_address, dns_name = get_host_and_dns_name_by_url(settings.PV_DICTIONARIES_URL)
            logger.audit(F9, host=ip_address if ip_address else "unknown", dns_name=dns_name)
            raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR)
        except HTTPStatusError as exc:
            logger.exception("Failed to create Dimension: HTTPStatusError. %s", exc.response.text)
            if exc.response.status_code in SERVER_ERRORS:
                ip_address, dns_name = get_host_and_dns_name_by_url(settings.PV_DICTIONARIES_URL)
                logger.audit(F9, host=ip_address if ip_address else "unknown", dns_name=dns_name)
            raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=exc.response.text)
        return None

    async def update_dimension(
        self,
        tenant_id: str,
        model_names: list[str],
        dimension_names: list[str],
        enable_delete_column: bool = True,
        enable_delete_not_empty: bool = False,
    ) -> None:
        """Обновить Dimension.."""
        if not settings.ENABLE_GENERATE_OBJECTS:
            return None
        try:
            async with AsyncClient(
                cert=self.cert,
                verify=self.verify if self.verify else False,
                headers=self.headers,
            ) as client:
                url = urljoin(settings.WORKER_MANAGER_URL, settings.WORKER_DIMENSION_URL)
                url = (
                    url.replace("{tenantName}", tenant_id)
                    + "?"
                    + "enableDeleteColumn="
                    + str(enable_delete_column).lower()
                    + "&"
                    + "enableDeleteNotEmpty="
                    + str(enable_delete_not_empty).lower()
                )
                logger.info("Sending update dimension request to %s", url)
                body = {
                    "modelNames": model_names,
                    "dimensions": dimension_names,
                }
                response = await client.patch(url=url, json=body)
                response.raise_for_status()
                logger.info("Update dimension request has been sended to %s", url)
        except TransportError:
            logger.exception("Failed to create Dimension: TransportError")
            ip_address, dns_name = get_host_and_dns_name_by_url(settings.PV_DICTIONARIES_URL)
            logger.audit(F9, host=ip_address if ip_address else "unknown", dns_name=dns_name)
            raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR)
        except HTTPStatusError as exc:
            logger.exception("Failed to create Dimension: HTTPStatusError. %s", exc.response.text)
            if exc.response.status_code in SERVER_ERRORS:
                ip_address, dns_name = get_host_and_dns_name_by_url(settings.PV_DICTIONARIES_URL)
                logger.audit(F9, host=ip_address if ip_address else "unknown", dns_name=dns_name)
            raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=exc.response.text)
        return None

    async def create_composite(
        self,
        tenant_id: str,
        model_names: list[str],
        composites: list[str],
        replace: bool = False,
    ) -> None:
        """Создать Composite."""
        if not settings.ENABLE_GENERATE_OBJECTS:
            return None
        try:
            async with AsyncClient(
                cert=self.cert,
                verify=self.verify if self.verify else False,
                headers=self.headers,
            ) as client:
                url = urljoin(settings.WORKER_MANAGER_URL, settings.WORKER_COMPOSITE_URL)
                url = url.replace("{tenantName}", tenant_id) + "?" + "replace=" + str(replace).lower()
                logger.info("Sending create composite request to %s", url)
                body = {
                    "modelNames": model_names,
                    "composites": composites,
                }
                response = await client.post(url=url, json=body)
                response.raise_for_status()
                logger.info("Create composite request has been sended to %s", url)
        except TransportError:
            logger.exception("Failed to create Composite: TransportError")
            ip_address, dns_name = get_host_and_dns_name_by_url(settings.PV_DICTIONARIES_URL)
            logger.audit(F9, host=ip_address if ip_address else "unknown", dns_name=dns_name)
            raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR)
        except HTTPStatusError as exc:
            logger.exception("Failed to create Composite: HTTPStatusError. %s", exc.response.text)
            if exc.response.status_code in SERVER_ERRORS:
                ip_address, dns_name = get_host_and_dns_name_by_url(settings.PV_DICTIONARIES_URL)
                logger.audit(F9, host=ip_address if ip_address else "unknown", dns_name=dns_name)
            raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=exc.response.text)
        return None

    async def update_composite(
        self,
        tenant_id: str,
        model_names: list[str],
        composites: list[str],
    ) -> None:
        """Создать Composite."""
        if not settings.ENABLE_GENERATE_OBJECTS:
            return None
        try:
            async with AsyncClient(
                cert=self.cert,
                verify=self.verify if self.verify else False,
                headers=self.headers,
            ) as client:
                url = urljoin(settings.WORKER_MANAGER_URL, settings.WORKER_COMPOSITE_URL)
                url = url.replace("{tenantName}", tenant_id)
                logger.info("Sending update composite request to %s", url)
                body = {
                    "modelNames": model_names,
                    "composites": composites,
                }
                response = await client.patch(url=url, json=body)
                response.raise_for_status()
                logger.info("Update composite request has been sended to %s", url)
        except TransportError:
            logger.exception("Failed to create Composite: TransportError")
            ip_address, dns_name = get_host_and_dns_name_by_url(settings.PV_DICTIONARIES_URL)
            logger.audit(F9, host=ip_address if ip_address else "unknown", dns_name=dns_name)
            raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR)
        except HTTPStatusError as exc:
            logger.exception("Failed to create Composite: HTTPStatusError. %s", exc.response.text)
            if exc.response.status_code in SERVER_ERRORS:
                ip_address, dns_name = get_host_and_dns_name_by_url(settings.PV_DICTIONARIES_URL)
                logger.audit(F9, host=ip_address if ip_address else "unknown", dns_name=dns_name)
            raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=exc.response.text)
        return None
