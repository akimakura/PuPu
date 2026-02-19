from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.v0.const import MODEL_URL, TENANT_NAME_URL, V0_PREFIX_URL
from src.db.database import Database
from src.db.model import Model
from src.main import API_PREFIX_URL
from src.models.model import Model as ModelModel, ModelEditRequest as ModelEditRequestModel
from src.service.model import ModelService
from tests.unit_tests.conftest import mock_method_by_http_exception, mock_session_maker
from tests.unit_tests.fixtures.model import model_model_list, model_model_update_list
from tests.unit_tests.utils import clear_uncompared_fields

PREFIX_URL = API_PREFIX_URL + V0_PREFIX_URL + TENANT_NAME_URL + MODEL_URL


class TestModelApiV0:

    @pytest.mark.parametrize(
        ("tenant_id", "expected", "db_models"),
        [
            ("tenant1", [], []),
            (
                "tenant1",
                model_model_list,
                "models",
            ),
            (None, [], []),
        ],
    )
    async def test_get_model_list(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        tenant_id: str,
        expected: list[ModelModel],
        db_models: list[Model],
        request: pytest.FixtureRequest,
    ) -> None:
        if isinstance(db_models, str):
            db_models = request.getfixturevalue(db_models)
        mocked_session.add_all(db_models)
        await mocked_session.commit()
        mock_session_maker(monkeypatch, mocked_session)
        if not tenant_id:
            mock_method_by_http_exception(monkeypatch, ModelService, "get_model_list", HTTPStatus.INTERNAL_SERVER_ERROR)
        expected_list = [ModelModel.model_dump(i, mode="json", by_alias=True) for i in expected]
        expected_list.sort(key=lambda model: model["name"])
        url = PREFIX_URL + "/"
        if tenant_id:
            url = url.replace("{tenantName}", tenant_id)
        else:
            url = url.replace("{tenantName}", "test")
        no_cached_model_res = await async_client.get(url=url)
        if tenant_id:
            assert no_cached_model_res.status_code == HTTPStatus.OK
            assert clear_uncompared_fields(expected_list) == clear_uncompared_fields(no_cached_model_res.json())
            cached_model_res = await async_client.get(url=url)
            assert clear_uncompared_fields(cached_model_res.json()) == clear_uncompared_fields(
                no_cached_model_res.json()
            )
        else:
            assert no_cached_model_res.status_code == HTTPStatus.INTERNAL_SERVER_ERROR

    @pytest.mark.parametrize(
        ("tenant_id", "model_name", "expected", "expected_status"),
        [
            ("tenant1", "test_model1222", None, HTTPStatus.NOT_FOUND),
            ("tenant1", "test_model1", model_model_list[0], HTTPStatus.OK),
            (None, "test_model1", model_model_list[0], HTTPStatus.INTERNAL_SERVER_ERROR),
        ],
    )
    async def test_get_model_by_name(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        models: list[Model],
        tenant_id: str,
        model_name: str,
        expected: ModelModel,
        expected_status: HTTPStatus,
    ) -> None:
        mocked_session.add_all(models)
        await mocked_session.commit()
        mock_session_maker(monkeypatch, mocked_session)
        if not tenant_id:
            mock_method_by_http_exception(
                monkeypatch, ModelService, "get_model_by_name", HTTPStatus.INTERNAL_SERVER_ERROR
            )
        expected_dict = None
        if expected is not None:
            expected_dict = ModelModel.model_dump(
                expected,
                mode="json",
                by_alias=True,
            )
        url = PREFIX_URL + f"/{model_name}"
        if tenant_id:
            url = url.replace("{tenantName}", tenant_id)
        else:
            url = url.replace("{tenantName}", "test")
        no_cached_model_res = await async_client.get(url=url)
        assert no_cached_model_res.status_code == expected_status
        if tenant_id and expected_status == HTTPStatus.OK:
            assert clear_uncompared_fields(expected_dict) == clear_uncompared_fields(no_cached_model_res.json())
            cached_model_res = await async_client.get(url=url)
            assert clear_uncompared_fields(cached_model_res.json()) == clear_uncompared_fields(
                no_cached_model_res.json()
            )

    @pytest.mark.parametrize(
        ("tenant_id", "model", "expected_status"),
        [
            ("tenant1", "test_model1123", HTTPStatus.NOT_FOUND),
            ("tenant1", "test_model1", HTTPStatus.NO_CONTENT),
            (None, "test_model1", HTTPStatus.INTERNAL_SERVER_ERROR),
        ],
    )
    async def test_delete_model_by_name(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        models: list[Model],
        tenant_id: str,
        model: str,
        expected_status: HTTPStatus,
    ) -> None:
        mocked_session.add_all(models)
        await mocked_session.commit()
        mock_session_maker(monkeypatch, mocked_session)
        if not tenant_id:
            mock_method_by_http_exception(
                monkeypatch, ModelService, "delete_model_by_name", HTTPStatus.INTERNAL_SERVER_ERROR
            )
        url = PREFIX_URL + f"/{model}"
        if tenant_id:
            url = url.replace("{tenantName}", tenant_id)
        else:
            url = url.replace("{tenantName}", "test")
        response = await async_client.delete(url=url)
        assert response.status_code == expected_status
        if tenant_id and response.status_code == HTTPStatus.NO_CONTENT:
            response = await async_client.get(url=url)
            assert response.status_code == HTTPStatus.NOT_FOUND

    @pytest.mark.parametrize(
        ("tenant_id", "model"),
        [
            ("tenant1", model_model_list[0]),
            ("tenant1", model_model_list[1]),
            (None, model_model_list[1]),
        ],
    )
    async def test_create_model(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        tenant_id: str,
        model: ModelModel,
        databases: list[Database],
    ) -> None:
        mocked_session.add_all(databases)
        await mocked_session.commit()
        mock_session_maker(monkeypatch, mocked_session)
        if not tenant_id:
            mock_method_by_http_exception(
                monkeypatch, ModelService, "create_model_by_schema", HTTPStatus.INTERNAL_SERVER_ERROR
            )
        url = PREFIX_URL + "/"
        if tenant_id:
            url = url.replace("{tenantName}", tenant_id)
        else:
            url = url.replace("{tenantName}", "test")
        model_dict = model.model_dump(mode="json", by_alias=True)
        response = await async_client.post(url=url, json=model_dict)
        if tenant_id:
            assert response.status_code == HTTPStatus.CREATED
            assert clear_uncompared_fields(response.json()) == clear_uncompared_fields(model_dict)
            response = await async_client.get(url=url + model_dict["name"])
            assert response.status_code == HTTPStatus.OK
        else:
            assert response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR

    @pytest.mark.parametrize(
        ("tenant_id", "model_name", "update_model"),
        [
            ("tenant1", "test_model1", model_model_update_list[0]),
            ("tenant1", "test_model2", model_model_update_list[1]),
            (None, "test_model2", model_model_update_list[1]),
            ("tenant1", "unknown", model_model_update_list[1]),
        ],
    )
    async def test_update_model_by_name(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        tenant_id: str,
        model_name: str,
        update_model: ModelEditRequestModel,
        models: list[Model],
    ) -> None:
        mocked_session.add_all(models)
        await mocked_session.commit()
        mock_session_maker(monkeypatch, mocked_session)
        if not tenant_id:
            mock_method_by_http_exception(
                monkeypatch, ModelService, "get_updated_fields", HTTPStatus.INTERNAL_SERVER_ERROR
            )
        update_model_dict = update_model.model_dump(exclude_unset=True, mode="json", by_alias=True)
        url = PREFIX_URL + f"/{model_name}"
        if tenant_id:
            url = url.replace("{tenantName}", tenant_id)
        else:
            url = url.replace("{tenantName}", "test")
        original_model_dict = clear_uncompared_fields((await async_client.get(url=url)).json())
        response = await async_client.patch(url=url, json=update_model_dict)
        if model_name == "unknown":
            assert response.status_code == HTTPStatus.NOT_FOUND
            return
        if tenant_id:
            assert response.status_code == HTTPStatus.OK
            response_dict = clear_uncompared_fields(response.json())
            for field, value in update_model_dict.items():
                assert response_dict[field] == value
                original_model_dict.pop(field)
            for field, value in original_model_dict.items():
                assert response_dict[field] == value
        else:
            assert response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR
