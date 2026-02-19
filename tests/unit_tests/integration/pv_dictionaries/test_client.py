from contextlib import nullcontext as does_not_raise
from http import HTTPStatus
from typing import Any, Optional, Type

import pytest
from fastapi import HTTPException
from httpx import HTTPStatusError, Request, Response, TransportError
from py_common_lib.logger import formatter
from py_common_lib.starlette_context_plugins import ClientHostPlugin

from src.db.dimension import Dimension
from src.integration.pv_dictionaries import client
from src.integration.pv_dictionaries.client import ClientPVDictionaries
from src.integration.pv_dictionaries.models import PVDictionaryVersion
from tests.unit_tests.fixtures.pv_dictionaries import pv_dictionary_attribute


class MockContext(dict):
    """Мок starlette_context"""

    data = {ClientHostPlugin.key: "127.0.0.1"}


def mock_get_standard_headers() -> dict[str, str]:
    return {"mocked": "mocked"}


class MockResponse:

    def __init__(self, url: str) -> None:
        self.url = url

    def raise_for_status(self) -> None:
        return None

    def json(self) -> list[dict] | dict:
        """
        Мок функции json оригинального Response из httpx.

        Returns:
            dict: versionCode для тестирования функции _create_version_dictionary.
            list[dict]: список id для тесторовния функции _create_dictionary.
        """
        if "version" in self.url:
            return {"versionCode": 1}
        return [{"id": 1}]


class MockAsyncClient:

    def __init__(self, cert: Any = None, verify: Any = None, headers: Any = None) -> None:
        self.cert = cert
        self.verify = verify

    async def __aenter__(self, cert: Any = None, verify: Any = None, headers: Any = None) -> "MockAsyncClient":
        return self

    async def __aexit__(self, cert: Any = None, verify: Any = None, headers: Any = None) -> None:
        return None

    async def post(self, url: Any = None, files: Any = None, json: Any = None) -> MockResponse:
        return MockResponse(url)


class TestClientPVDictionaries:

    def test_init(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_context = MockContext()
        monkeypatch.setattr(client, "get_standard_headers", mock_get_standard_headers)
        monkeypatch.setattr(client, "context", mock_context)
        monkeypatch.setattr(client.settings, "PV_DICTIONARIES_URL", "")
        with pytest.raises(ValueError, match="Переменная PV_DICTIONARIES_URL не найдена."):
            _ = ClientPVDictionaries()

    async def test_create_dictionary_with_client(
        self, monkeypatch: pytest.MonkeyPatch, pv_dimensions: list[Dimension]
    ) -> None:
        mock_context = MockContext()
        monkeypatch.setattr(client, "get_standard_headers", mock_get_standard_headers)
        monkeypatch.setattr(client, "context", mock_context)
        monkeypatch.setattr(client, "AsyncClient", MockAsyncClient)
        client_pvd = ClientPVDictionaries()
        mock_client = MockAsyncClient()
        assert PVDictionaryVersion(
            dictionary_id=1, version_code="1.1", dictionary_name="testPv"
        ) == await client_pvd._create_dictionary(
            mock_client, pv_dimensions[0], pv_dictionary_attribute  # type: ignore
        )

    async def test_activate_version_dictionary_with_client(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_context = MockContext()
        monkeypatch.setattr(client, "get_standard_headers", mock_get_standard_headers)
        monkeypatch.setattr(client, "context", mock_context)
        monkeypatch.setattr(client, "AsyncClient", MockAsyncClient)
        client_pvd = ClientPVDictionaries()
        mock_client = MockAsyncClient()
        await client_pvd._activate_version_dictionary(mock_client, 1, "testPv")  # type: ignore

    async def test_create_version_dictionary_with_client(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_context = MockContext()
        monkeypatch.setattr(client, "get_standard_headers", mock_get_standard_headers)
        monkeypatch.setattr(client, "context", mock_context)
        monkeypatch.setattr(client, "AsyncClient", MockAsyncClient)
        version = PVDictionaryVersion(dictionary_id=1, version_code="1.1", dictionary_name="testPv")
        client_pvd = ClientPVDictionaries()
        mock_client = MockAsyncClient()
        await client_pvd._create_version_dictionary(mock_client, version)  # type: ignore

    @pytest.mark.parametrize(
        ("exception", "status", "expected_exception"),
        [
            (None, None, does_not_raise()),
            (TransportError, None, pytest.raises(HTTPException)),
            (HTTPStatusError, HTTPStatus.NOT_FOUND, pytest.raises(HTTPException)),
        ],
    )
    async def test_create_dictionary(
        self,
        monkeypatch: pytest.MonkeyPatch,
        pv_dimensions: list[Dimension],
        exception: Optional[Type[TransportError] | Type[HTTPStatusError]],
        expected_exception: pytest.RaisesExc,
        status: Optional[HTTPStatus],
    ) -> None:
        async def mock_create_dictionary(
            self: Any, client: Any, dimension: Any, pv_dictionary: Any
        ) -> PVDictionaryVersion:
            if exception is None:
                return PVDictionaryVersion(dictionary_id=1, version_code="1.1", dictionary_name="testPv")
            elif exception == HTTPStatusError:
                if status:
                    status_code = int(HTTPStatus.NOT_FOUND)
                else:
                    status_code = 500
                raise HTTPStatusError(
                    "test", request=Request("POST", "http://test.ru/"), response=Response(status_code)
                )
            elif exception == TransportError:
                raise TransportError("test")
            raise Exception("msg")

        mock_context = MockContext()
        monkeypatch.setattr(client, "get_standard_headers", mock_get_standard_headers)
        monkeypatch.setattr(client, "context", mock_context)
        monkeypatch.setattr(formatter, "context", mock_context)
        monkeypatch.setattr(client, "AsyncClient", MockAsyncClient)
        monkeypatch.setattr(ClientPVDictionaries, "_create_dictionary", mock_create_dictionary)
        client_pvd = ClientPVDictionaries()
        with expected_exception:
            await client_pvd.create_dictionary(pv_dimensions[0], pv_dictionary_attribute)

    @pytest.mark.parametrize(
        ("exception", "status", "expected_exception"),
        [
            (None, None, does_not_raise()),
            (TransportError, None, pytest.raises(HTTPException)),
            (HTTPStatusError, HTTPStatus.NOT_FOUND, pytest.raises(HTTPException)),
        ],
    )
    async def test_create_version_dictionary(
        self,
        monkeypatch: pytest.MonkeyPatch,
        exception: Optional[Type[TransportError] | Type[HTTPStatusError]],
        expected_exception: pytest.RaisesExc,
        status: Optional[HTTPStatus],
    ) -> None:
        async def mock_create_version_dictionary(self: Any, client: Any, version: Any) -> None:
            if exception is None:
                return None
            elif exception == HTTPStatusError:
                if status:
                    status_code = int(HTTPStatus.NOT_FOUND)
                else:
                    status_code = 500
                raise HTTPStatusError(
                    "test", request=Request("POST", "http://test.ru/"), response=Response(status_code)
                )
            elif exception == TransportError:
                raise TransportError("test")
            raise Exception("msg")

        mock_context = MockContext()
        monkeypatch.setattr(client, "get_standard_headers", mock_get_standard_headers)
        monkeypatch.setattr(client, "context", mock_context)
        monkeypatch.setattr(formatter, "context", mock_context)
        monkeypatch.setattr(client, "AsyncClient", MockAsyncClient)
        monkeypatch.setattr(ClientPVDictionaries, "_create_version_dictionary", mock_create_version_dictionary)
        client_pvd = ClientPVDictionaries()
        with expected_exception:
            await client_pvd.create_version_dictionary(
                PVDictionaryVersion(dictionary_id=1, version_code="1.1", dictionary_name="testPv")
            )

    @pytest.mark.parametrize(
        ("exception", "status", "expected_exception"),
        [
            (None, None, does_not_raise()),
            (TransportError, None, pytest.raises(HTTPException)),
            (HTTPStatusError, HTTPStatus.NOT_FOUND, pytest.raises(HTTPException)),
        ],
    )
    async def test_activate_version_dictionary(
        self,
        monkeypatch: pytest.MonkeyPatch,
        exception: Optional[Type[TransportError] | Type[HTTPStatusError]],
        expected_exception: pytest.RaisesExc,
        status: Optional[HTTPStatus],
    ) -> None:
        async def mock_activate_version_dictionary(self: Any, client: Any, version: Any, dictionary_name: Any) -> None:
            if exception is None:
                return None
            elif exception == HTTPStatusError:
                if status:
                    status_code = int(HTTPStatus.NOT_FOUND)
                else:
                    status_code = 500
                raise HTTPStatusError(
                    "test", request=Request("POST", "http://test.ru/"), response=Response(status_code)
                )
            elif exception == TransportError:
                raise TransportError("test")
            raise Exception("msg")

        mock_context = MockContext()
        monkeypatch.setattr(client, "get_standard_headers", mock_get_standard_headers)
        monkeypatch.setattr(client, "context", mock_context)
        monkeypatch.setattr(formatter, "context", mock_context)
        monkeypatch.setattr(client, "AsyncClient", MockAsyncClient)
        monkeypatch.setattr(ClientPVDictionaries, "_activate_version_dictionary", mock_activate_version_dictionary)
        client_pvd = ClientPVDictionaries()
        with expected_exception:
            await client_pvd.activate_version_dictionary(1, "testPv")
