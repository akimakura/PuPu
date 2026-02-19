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
from src.db.dimension import Dimension
from src.integration.pv_dictionaries.formatter import PVDictionaryFormatter
from src.integration.pv_dictionaries.models import PVDictionaryVersion, PVHierarchyPayload
from src.utils.cert import get_cert_and_verify_for_httpx

logger = EPMPYLogger(__name__)


class ClientPVDictionaries:
    """Клиент для работы с PV Dictionaries"""

    def __init__(self, ref_mapping: dict[str, Dimension] | None = None) -> None:
        self.cert, self.verify = get_cert_and_verify_for_httpx(
            settings.PV_DICTIONARIES_PATH_TO_CA_CERT,
            settings.PV_DICTIONARIES_PATH_TO_CLIENT_CERT,
            settings.PV_DICTIONARIES_PATH_TO_CLIENT_CERT_KEY,
            settings.PV_DICTIONARIES_CLIENT_CERT_PASSWORD,
        )
        if ref_mapping:
            self.ref_mapping = ref_mapping
        else:
            self.ref_mapping = {}
        self.headers = get_standard_headers()
        self.headers.update({settings.PV_DICTIONARIES_AUTH_HEADER: context.get(AuthorizationPlugin.key)})
        self.headers = {
            header: header_value for header, header_value in self.headers.items() if header_value is not None
        }
        if not settings.PV_DICTIONARIES_URL:
            raise ValueError("Переменная PV_DICTIONARIES_URL не найдена.")

    async def _create_dictionary(
        self,
        client: AsyncClient,
        dimension: Dimension,
        pv_dictionary: dict,
        with_attrs: bool = True,
        with_texts: bool = True,
    ) -> PVDictionaryVersion:
        """Создать dimension в PV Dictionaries с клиентом."""
        url = urljoin(
            settings.PV_DICTIONARIES_URL,
            settings.PV_DICTIONARIES_CREATE_DICTIONARY % settings.PV_DICTIONARIES_TENANT_NAME,
        )
        formatter = PVDictionaryFormatter(dimension, pv_dictionary, self.ref_mapping, with_attrs, with_texts)
        xml = formatter.get_xml_create_document()
        files = {"model": ("create_model_by_sem_layer.xml", xml, "text/xml")}
        logger.debug("XML for creating a dictionary: %s", files)
        response = await client.post(url, files=files)
        response.raise_for_status()
        dictionary = response.json()
        logger.debug("Response dictionary: %s", dictionary)
        return PVDictionaryVersion(
            object_id=dictionary[0].get("id"),
            object_name=pv_dictionary["object_name"],
            version_code=settings.PV_DICTIONARIES_VERSION_CODE,
        )

    async def _update_dictionary(
        self,
        client: AsyncClient,
        dimension: Dimension,
        pv_dictionary: dict,
        with_attrs: bool = True,
        with_texts: bool = True,
    ) -> PVDictionaryVersion:
        """Обновить dimension в PV Dictionaries с клиентом."""
        url = urljoin(
            settings.PV_DICTIONARIES_URL,
            settings.PV_DICTIONARIES_CREATE_DICTIONARY % settings.PV_DICTIONARIES_TENANT_NAME,
        )
        formatter = PVDictionaryFormatter(
            dimension,
            pv_dictionary,
            self.ref_mapping,
            with_attrs,
            with_texts,
        )
        xml = formatter.get_xml_create_document()
        files = {"model": ("update_model_by_sem_layer.xml", xml, "text/xml")}
        logger.debug("XML for updating a dictionary: %s", files)
        response = await client.put(url, files=files)
        response.raise_for_status()
        dictionary = response.json()
        logger.debug("Response dictionary: %s", dictionary)
        return PVDictionaryVersion(
            object_id=dictionary[0].get("id"),
            object_name=dictionary[0].get("name"),
            description=settings.PV_DICTIONARIES_VERSION_DESCRIPTION_PATTERN % dimension.timestamp,
        )

    async def create_dictionary(
        self,
        dimension: Dimension,
        pv_dictionary: dict,
        with_attrs: bool = True,
        with_texts: bool = True,
    ) -> PVDictionaryVersion:
        """Создать dimension в PV Dictionaries."""
        try:
            async with AsyncClient(
                cert=self.cert, verify=self.verify if self.verify else False, headers=self.headers
            ) as client:
                result = await self._create_dictionary(client, dimension, pv_dictionary, with_attrs, with_texts)
        except TransportError:
            logger.exception("Failed to create dictionary: TransportError")
            ip_address, dns_name = get_host_and_dns_name_by_url(settings.PV_DICTIONARIES_URL)
            logger.audit(F9, host=ip_address if ip_address else "unknown", dns_name=dns_name)
            raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR)
        except HTTPStatusError as exc:
            logger.exception("Failed to create dictionary: HTTPStatusError. %s", exc.response.text)
            if exc.response.status_code in SERVER_ERRORS:
                ip_address, dns_name = get_host_and_dns_name_by_url(settings.PV_DICTIONARIES_URL)
                logger.audit(F9, host=ip_address if ip_address else "unknown", dns_name=dns_name)
            raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=exc.response.text)
        return result

    async def update_dictionary(
        self,
        dimension: Dimension,
        pv_dictionary: dict,
        with_attrs: bool = True,
        with_texts: bool = True,
    ) -> PVDictionaryVersion:
        """Создать dimension в PV Dictionaries."""
        try:
            async with AsyncClient(
                cert=self.cert, verify=self.verify if self.verify else False, headers=self.headers
            ) as client:
                result = await self._update_dictionary(client, dimension, pv_dictionary, with_attrs, with_texts)
        except TransportError:
            logger.exception("Failed to create dictionary: TransportError")
            ip_address, dns_name = get_host_and_dns_name_by_url(settings.PV_DICTIONARIES_URL)
            logger.audit(F9, host=ip_address if ip_address else "unknown", dns_name=dns_name)
            raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR)
        except HTTPStatusError as exc:
            logger.exception("Failed to create dictionary: HTTPStatusError. %s", exc.response.text)
            if exc.response.status_code in SERVER_ERRORS:
                ip_address, dns_name = get_host_and_dns_name_by_url(settings.PV_DICTIONARIES_URL)
                logger.audit(F9, host=ip_address if ip_address else "unknown", dns_name=dns_name)
            raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=exc.response.text)
        return result

    async def _create_version_dictionary(self, client: AsyncClient, version: PVDictionaryVersion) -> int:
        """Создать version в PV Dictionaries."""
        url = urljoin(
            settings.PV_DICTIONARIES_URL,
            settings.PV_DICTIONARIES_CREATE_VERSION % (settings.PV_DICTIONARIES_TENANT_NAME, version.object_name),
        )
        send_version = version.model_dump(by_alias=True, exclude_none=True)
        logger.debug("Version for creating a dictionary: %s", send_version)
        response = await client.post(url, json=send_version)
        response.raise_for_status()
        saved_version_code = response.json()
        logger.debug("Response from url %s: %s", url, saved_version_code)
        return saved_version_code["versionCode"]

    async def create_version_dictionary(self, version: PVDictionaryVersion) -> int:
        """Создать version в PV Dictionaries."""
        try:
            async with AsyncClient(
                cert=self.cert, verify=self.verify if self.verify else False, headers=self.headers
            ) as client:
                saved_version_code = await self._create_version_dictionary(client, version)
        except TransportError:
            logger.exception("Failed to create version: TransportError")
            ip_address, dns_name = get_host_and_dns_name_by_url(settings.PV_DICTIONARIES_URL)
            logger.audit(F9, host=ip_address if ip_address else "unknown", dns_name=dns_name)
            raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR)
        except HTTPStatusError as exc:
            logger.exception("Failed to create version: HTTPStatusError. %s", exc.response.text)
            if exc.response.status_code in SERVER_ERRORS:
                ip_address, dns_name = get_host_and_dns_name_by_url(settings.PV_DICTIONARIES_URL)
                logger.audit(F9, host=ip_address if ip_address else "unknown", dns_name=dns_name)
            raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=exc.response.text)
        return saved_version_code

    async def _activate_version_dictionary(self, client: AsyncClient, version: int, dictionary_name: str) -> None:
        """Активировать версию dictionary."""
        url = urljoin(
            settings.PV_DICTIONARIES_URL,
            settings.PV_DICTIONARIES_ACTIVATE_VERSION
            % (settings.PV_DICTIONARIES_TENANT_NAME, dictionary_name, version),
        )
        response = await client.post(url, json={"state": "ACTIVE"})
        logger.debug("Response: %s", response.json())
        response.raise_for_status()
        return None

    async def create_hierarchy(self, payload: PVHierarchyPayload) -> dict:
        """Создать иерархию в PVD."""
        try:
            async with AsyncClient(
                cert=self.cert, verify=self.verify if self.verify else False, headers=self.headers
            ) as client:
                url = urljoin(settings.PV_DICTIONARIES_URL, settings.PV_HIERARCHIES_CREATE_URL)
                body = payload.model_dump(by_alias=True, exclude_none=True)
                logger.debug("PVD hierarchy create request: url=%s, body=%s", url, body)
                response = await client.post(url, json=body)
                response.raise_for_status()
                return response.json() if response.text else {}
        except TransportError:
            logger.exception("Failed to create hierarchy in PVD: TransportError")
            ip_address, dns_name = get_host_and_dns_name_by_url(settings.PV_DICTIONARIES_URL)
            logger.audit(F9, host=ip_address if ip_address else "unknown", dns_name=dns_name)
            raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR)
        except HTTPStatusError as exc:
            logger.exception("Failed to create hierarchy in PVD: HTTPStatusError. %s", exc.response.text)
            if exc.response.status_code in SERVER_ERRORS:
                ip_address, dns_name = get_host_and_dns_name_by_url(settings.PV_DICTIONARIES_URL)
                logger.audit(F9, host=ip_address if ip_address else "unknown", dns_name=dns_name)
            raise HTTPException(status_code=exc.response.status_code, detail=exc.response.text)

    async def update_hierarchy(self, hierarchy_name: str, payload: PVHierarchyPayload) -> dict:
        """Обновить иерархию в PVD."""
        try:
            async with AsyncClient(
                cert=self.cert, verify=self.verify if self.verify else False, headers=self.headers
            ) as client:
                url = urljoin(
                    settings.PV_DICTIONARIES_URL,
                    settings.PV_HIERARCHIES_UPDATE_URL % hierarchy_name,
                )
                body = payload.model_dump(by_alias=True, exclude_none=True)
                logger.debug("PVD hierarchy update request: url=%s, body=%s", url, body)
                response = await client.put(url, json=body)
                response.raise_for_status()
                return response.json() if response.text else {}
        except TransportError:
            logger.exception("Failed to update hierarchy in PVD: TransportError")
            ip_address, dns_name = get_host_and_dns_name_by_url(settings.PV_DICTIONARIES_URL)
            logger.audit(F9, host=ip_address if ip_address else "unknown", dns_name=dns_name)
            raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR)
        except HTTPStatusError as exc:
            logger.exception("Failed to update hierarchy in PVD: HTTPStatusError. %s", exc.response.text)
            if exc.response.status_code in SERVER_ERRORS:
                ip_address, dns_name = get_host_and_dns_name_by_url(settings.PV_DICTIONARIES_URL)
                logger.audit(F9, host=ip_address if ip_address else "unknown", dns_name=dns_name)
            raise HTTPException(status_code=exc.response.status_code, detail=exc.response.text)

    async def delete_hierarchy(self, hierarchy_name: str) -> None:
        """Удалить иерархию из PVD."""
        try:
            async with AsyncClient(
                cert=self.cert, verify=self.verify if self.verify else False, headers=self.headers
            ) as client:
                url = urljoin(
                    settings.PV_DICTIONARIES_URL,
                    settings.PV_HIERARCHIES_DELETE_URL % hierarchy_name,
                )
                logger.debug("Deleting hierarchy from PVD: %s", hierarchy_name)
                response = await client.delete(url)
                response.raise_for_status()
        except TransportError:
            logger.exception("Failed to delete hierarchy from PVD: TransportError")
            ip_address, dns_name = get_host_and_dns_name_by_url(settings.PV_DICTIONARIES_URL)
            logger.audit(F9, host=ip_address if ip_address else "unknown", dns_name=dns_name)
            raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR)
        except HTTPStatusError as exc:
            if exc.response.status_code == HTTPStatus.NOT_FOUND:
                logger.warning(
                    "Hierarchy '%s' not found in PVD, skipping deletion: %s",
                    hierarchy_name,
                    exc.response.text,
                )
                return
            logger.exception("Failed to delete hierarchy from PVD: HTTPStatusError. %s", exc.response.text)
            if exc.response.status_code in SERVER_ERRORS:
                ip_address, dns_name = get_host_and_dns_name_by_url(settings.PV_DICTIONARIES_URL)
                logger.audit(F9, host=ip_address if ip_address else "unknown", dns_name=dns_name)
            raise HTTPException(status_code=exc.response.status_code, detail=exc.response.text)

    async def activate_version_dictionary(self, version: int, dictionary_name: str) -> None:
        """Активировать версию dictionary."""
        try:
            async with AsyncClient(
                cert=self.cert, verify=self.verify if self.verify else False, headers=self.headers
            ) as client:
                await self._activate_version_dictionary(client, version, dictionary_name)
        except TransportError:
            logger.exception("Failed to activate version: TransportError")
            ip_address, dns_name = get_host_and_dns_name_by_url(settings.PV_DICTIONARIES_URL)
            logger.audit(F9, host=ip_address if ip_address else "unknown", dns_name=dns_name)
            raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR)
        except HTTPStatusError as exc:
            logger.exception("Failed to activate version: HTTPStatusError. %s", exc.response.text)
            if exc.response.status_code in SERVER_ERRORS:
                ip_address, dns_name = get_host_and_dns_name_by_url(settings.PV_DICTIONARIES_URL)
                logger.audit(F9, host=ip_address if ip_address else "unknown", dns_name=dns_name)
            raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=exc.response.text)
        return None

    async def _delete_dictionary(
        self,
        client: AsyncClient,
        pv_name: str,
    ) -> None:
        """Удалить dimension в PV Dictionaries с клиентом."""
        url = urljoin(
            settings.PV_DICTIONARIES_URL,
            settings.PV_DICTIONARIES_DELETE_DICTIONARY % settings.PV_DICTIONARIES_TENANT_NAME,
        )
        response = await client.delete(url + pv_name)

        if response.text and isinstance(response.json(), dict) and response.json().get("error") == "DICT_NOT_FOUND":
            return None
        response.raise_for_status()
        logger.info("Deleted dictionary: %s", pv_name)
        return None

    async def delete_dictionary(
        self,
        pv_name: str,
    ) -> None:
        """Удалить dimension в PV Dictionaries."""
        try:
            async with AsyncClient(
                cert=self.cert, verify=self.verify if self.verify else False, headers=self.headers
            ) as client:
                await self._delete_dictionary(client, pv_name)
        except TransportError:
            logger.exception("Failed to delete dictionary: TransportError")
            ip_address, dns_name = get_host_and_dns_name_by_url(settings.PV_DICTIONARIES_URL)
            logger.audit(F9, host=ip_address if ip_address else "unknown", dns_name=dns_name)
            raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR)
        except HTTPStatusError as exc:
            logger.exception("Failed to delete dictionary: HTTPStatusError. %s", exc.response.text)
            if exc.response.status_code in SERVER_ERRORS:
                ip_address, dns_name = get_host_and_dns_name_by_url(settings.PV_DICTIONARIES_URL)
                logger.audit(F9, host=ip_address if ip_address else "unknown", dns_name=dns_name)
            raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=exc.response.text)

    async def _get_dictionary_or_none_by_client(
        self, client: AsyncClient, domain_name: str, object_name: str
    ) -> PVDictionaryVersion | None:
        """Получить dictionary из PV Dictionaries."""
        url = f"{urljoin(settings.PV_DICTIONARIES_URL, settings.PV_DICTIONARIES_GET_DICTIONARY % settings.PV_DICTIONARIES_TENANT_NAME)}?domain.name={domain_name}&name={object_name}"
        response = await client.get(url)
        if response.status_code == HTTPStatus.OK:
            response_json: dict = response.json()
            if response_json.get("count", 0) > 0 and response_json.get("dictionaries", []):
                dictionary: dict = response_json.get("dictionaries", [])[0]
                return PVDictionaryVersion(
                    object_id=dictionary["id"],
                    object_name=dictionary["name"],
                    version_code=settings.PV_DICTIONARIES_VERSION_CODE,
                )
            logger.debug("Dictionary not found: %s", url)
            return None
        elif response.status_code == HTTPStatus.NOT_FOUND:
            logger.debug("Dictionary not found: %s", url)
            return None
        response.raise_for_status()
        logger.debug("Dictionary not found: %s", url)
        return None

    async def get_dictionary_or_none(self, domain_name: str, object_name: str) -> PVDictionaryVersion | None:
        """Получить dictionary в PV Dictionaries."""
        try:
            async with AsyncClient(
                cert=self.cert, verify=self.verify if self.verify else False, headers=self.headers
            ) as client:
                result = await self._get_dictionary_or_none_by_client(client, domain_name, object_name)
        except TransportError:
            logger.exception("Failed to get dictionary: TransportError")
            ip_address, dns_name = get_host_and_dns_name_by_url(settings.PV_DICTIONARIES_URL)
            logger.audit(F9, host=ip_address if ip_address else "unknown", dns_name=dns_name)
            raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR)
        except HTTPStatusError as exc:
            logger.exception("Failed to get dictionary: HTTPStatusError. %s", exc.response.text)
            if exc.response.status_code in SERVER_ERRORS:
                ip_address, dns_name = get_host_and_dns_name_by_url(settings.PV_DICTIONARIES_URL)
                logger.audit(F9, host=ip_address if ip_address else "unknown", dns_name=dns_name)
            raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=exc.response.text)
        return result
