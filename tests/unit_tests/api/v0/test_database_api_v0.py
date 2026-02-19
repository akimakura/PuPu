from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.v0.const import DATABASE_URL, TENANT_NAME_URL, V0_PREFIX_URL
from src.db.database import Database
from src.main import API_PREFIX_URL
from src.models.database import (
    Database as DatabaseModel,
    DatabaseCreateRequest as DatabaseCreateRequestModel,
    DatabaseEditRequest as DatabaseEditRequestModel,
)
from src.service.database import DatabaseService
from tests.unit_tests.conftest import mock_method_by_http_exception, mock_session_maker
from tests.unit_tests.fixtures.database import (
    database_model_create_list,
    database_model_list,
    database_model_update_list,
)
from tests.unit_tests.utils import clear_uncompared_fields

PREFIX_URL = API_PREFIX_URL + V0_PREFIX_URL + TENANT_NAME_URL + DATABASE_URL


class TestDatabaseApiV0:

    @pytest.mark.parametrize(
        ("tenant_id", "expected", "db_models"),
        [
            ("tenant1", [], []),
            (
                "tenant1",
                database_model_list,
                "databases",
            ),
            (None, [], []),
        ],
    )
    async def test_get_database_list(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        tenant_id: str,
        expected: list[DatabaseModel],
        db_models: list[Database],
        request: pytest.FixtureRequest,
    ) -> None:
        if isinstance(db_models, str):
            db_models = request.getfixturevalue(db_models)
        mocked_session.add_all(db_models)
        await mocked_session.commit()
        mock_session_maker(monkeypatch, mocked_session)
        if not tenant_id:
            mock_method_by_http_exception(
                monkeypatch, DatabaseService, "get_database_list", HTTPStatus.INTERNAL_SERVER_ERROR
            )
        expected_list = [
            clear_uncompared_fields(DatabaseModel.model_dump(i, mode="json", by_alias=True)) for i in expected
        ]
        expected_list.sort(key=lambda database: database["name"])
        url = PREFIX_URL + "/"
        if tenant_id:
            url = url.replace("{tenantName}", tenant_id)
        else:
            url = url.replace("{tenantName}", "test")
        no_cached_database_res = await async_client.get(url=url)
        if tenant_id:
            assert no_cached_database_res.status_code == HTTPStatus.OK
            assert expected_list == clear_uncompared_fields(no_cached_database_res.json())
            cached_database_res = await async_client.get(url=url)
            assert clear_uncompared_fields(cached_database_res.json()) == clear_uncompared_fields(
                no_cached_database_res.json()
            )
        else:
            assert no_cached_database_res.status_code == HTTPStatus.INTERNAL_SERVER_ERROR

    @pytest.mark.parametrize(
        ("tenant_id", "database_name", "expected", "expected_status"),
        [
            ("tenant1", "test_database1222", None, HTTPStatus.NOT_FOUND),
            ("tenant1", "test_database2", database_model_list[1], HTTPStatus.OK),
            (None, "test_database2", database_model_list[1], HTTPStatus.INTERNAL_SERVER_ERROR),
        ],
    )
    async def test_get_database_by_name(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        databases: list[Database],
        tenant_id: str,
        database_name: str,
        expected: DatabaseModel,
        expected_status: HTTPStatus,
    ) -> None:
        mocked_session.add_all(databases)
        await mocked_session.commit()
        mock_session_maker(monkeypatch, mocked_session)
        if not tenant_id:
            mock_method_by_http_exception(
                monkeypatch, DatabaseService, "get_database_by_name", HTTPStatus.INTERNAL_SERVER_ERROR
            )
        expected_dict = None
        if expected is not None:
            expected_dict = clear_uncompared_fields(
                DatabaseModel.model_dump(
                    expected,
                    mode="json",
                    by_alias=True,
                )
            )
        url = PREFIX_URL + f"/{database_name}"
        if tenant_id:
            url = url.replace("{tenantName}", tenant_id)
        else:
            url = url.replace("{tenantName}", "test")
        no_cached_datastorage_res = await async_client.get(url=url)
        assert no_cached_datastorage_res.status_code == expected_status
        if tenant_id and expected_status == HTTPStatus.OK:
            assert expected_dict == clear_uncompared_fields(no_cached_datastorage_res.json())
            cached_datastorage_res = await async_client.get(url=url)
            assert clear_uncompared_fields(cached_datastorage_res.json()) == clear_uncompared_fields(
                no_cached_datastorage_res.json()
            )

    @pytest.mark.parametrize(
        ("tenant_id", "model_name", "database_name", "expected_status"),
        [
            ("tenant1", "test_model1", "test_database123", HTTPStatus.NOT_FOUND),
            ("tenant1", "test_model1", "test_database1", HTTPStatus.NO_CONTENT),
            (None, "test_model1", "test_database1", HTTPStatus.INTERNAL_SERVER_ERROR),
        ],
    )
    async def test_delete_database_by_name(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        databases: list[Database],
        tenant_id: str,
        model_name: str,
        database_name: str,
        expected_status: HTTPStatus,
    ) -> None:
        mocked_session.add_all(databases)
        await mocked_session.commit()
        mock_session_maker(monkeypatch, mocked_session)
        if not tenant_id:
            mock_method_by_http_exception(
                monkeypatch, DatabaseService, "delete_database_by_name", HTTPStatus.INTERNAL_SERVER_ERROR
            )
        url = PREFIX_URL + f"/{database_name}"
        if tenant_id:
            url = url.replace("{tenantName}", tenant_id)
        else:
            url = url.replace("{tenantName}", "test")
        url = url.replace("{modelName}", model_name)
        response = await async_client.delete(url=url)
        assert response.status_code == expected_status
        if tenant_id and response.status_code == HTTPStatus.NO_CONTENT:
            response = await async_client.get(url=url)
            assert response.status_code == HTTPStatus.NOT_FOUND

    @pytest.mark.parametrize(
        ("tenant_id", "database", "expected"),
        [
            ("tenant1", database_model_create_list[0], database_model_list[0]),
            ("tenant1", database_model_create_list[1], database_model_list[1]),
            (None, database_model_create_list[1], database_model_list[1]),
        ],
    )
    async def test_create_database(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        tenant_id: str,
        database: DatabaseCreateRequestModel,
        expected: DatabaseModel,
    ) -> None:
        mock_session_maker(monkeypatch, mocked_session)
        if not tenant_id:
            mock_method_by_http_exception(
                monkeypatch, DatabaseService, "create_database_by_schema", HTTPStatus.INTERNAL_SERVER_ERROR
            )
        url = PREFIX_URL + "/"
        if tenant_id:
            url = url.replace("{tenantName}", tenant_id)
        else:
            url = url.replace("{tenantName}", "test")
        database_dict = database.model_dump(exclude_unset=True, mode="json", by_alias=True)
        response = await async_client.post(url=url, json=database_dict)
        if tenant_id:
            assert response.status_code == HTTPStatus.CREATED
            assert clear_uncompared_fields(response.json()) == clear_uncompared_fields(
                expected.model_dump(mode="json", by_alias=True)
            )
            response = await async_client.get(url=url + database_dict["name"])
            assert response.status_code == HTTPStatus.OK
        else:
            assert response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR

    @pytest.mark.parametrize(
        ("tenant_id", "database_name", "update_database"),
        [
            ("tenant1", "test_database1", database_model_update_list[0]),
            ("tenant1", "test_database2", database_model_update_list[1]),
            (None, "test_database2", database_model_update_list[1]),
            ("tenant1", "unknown", database_model_update_list[1]),
        ],
    )
    async def test_update_database_by_name(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        tenant_id: str,
        database_name: str,
        update_database: DatabaseEditRequestModel,
        databases: list[Database],
    ) -> None:
        mocked_session.add_all(databases)
        await mocked_session.commit()
        mock_session_maker(monkeypatch, mocked_session)
        if not tenant_id:
            mock_method_by_http_exception(
                monkeypatch, DatabaseService, "get_updated_fields", HTTPStatus.INTERNAL_SERVER_ERROR
            )
        update_database_dict = clear_uncompared_fields(
            update_database.model_dump(exclude_unset=True, mode="json", by_alias=True)
        )
        url = PREFIX_URL + f"/{database_name}"
        if tenant_id:
            url = url.replace("{tenantName}", tenant_id)
        else:
            url = url.replace("{tenantName}", "test")
        original_model_dict = clear_uncompared_fields((await async_client.get(url=url)).json())
        response = await async_client.patch(url=url, json=update_database_dict)
        if database_name == "unknown":
            assert response.status_code == HTTPStatus.NOT_FOUND
            return
        if tenant_id:
            assert response.status_code == HTTPStatus.OK
            response_dict = clear_uncompared_fields(response.json())
            for field, value in update_database_dict.items():
                assert response_dict[field] == value
                original_model_dict.pop(field)
            for field, value in original_model_dict.items():
                assert response_dict[field] == value
        else:
            assert response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR
