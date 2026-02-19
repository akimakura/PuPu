from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.v0.const import MEASURE_URL, TENANT_NAME_URL, V0_PREFIX_URL
from src.db.dimension import Dimension
from src.db.measure import Measure
from src.db.model import Model
from src.main import API_PREFIX_URL
from src.models.measure import (
    Measure as MeasureModel,
    MeasureCreateRequest as MeasureCreateRequestModel,
    MeasureEditRequest as MeasureEditRequestModel,
    MeasureV0,
)
from src.service.measure import MeasureService
from tests.unit_tests.conftest import mock_method_by_http_exception, mock_session_maker
from tests.unit_tests.fixtures.measure import measure_model_create_list, measure_model_list, measure_model_update_list
from tests.unit_tests.utils import clear_uncompared_fields

PREFIX_URL = API_PREFIX_URL + V0_PREFIX_URL + TENANT_NAME_URL + MEASURE_URL


class TestMeasureApiV0:

    @pytest.mark.parametrize(
        ("tenant_id", "model_name", "expected", "db_models"),
        [
            ("tenant1", "test_model2", [], []),
            (
                "tenant1",
                "test_model1",
                measure_model_list[0:2],
                "measures",
            ),
            (None, "test_model2", [], []),
        ],
    )
    async def test_get_measure_list_by_model_name(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        tenant_id: str,
        model_name: str,
        expected: list[MeasureModel],
        db_models: list[Measure],
        request: pytest.FixtureRequest,
    ) -> None:
        if isinstance(db_models, str):
            db_models = request.getfixturevalue(db_models)
        mocked_session.add_all(db_models)
        await mocked_session.commit()
        mock_session_maker(monkeypatch, mocked_session)
        if not tenant_id:
            mock_method_by_http_exception(
                monkeypatch, MeasureService, "get_measure_list_by_model_name", HTTPStatus.INTERNAL_SERVER_ERROR
            )
        expected_list = [
            MeasureModel.model_dump(MeasureV0.model_validate(i), mode="json", by_alias=True) for i in expected
        ]
        expected_list.sort(key=lambda measure: measure["name"])
        url = PREFIX_URL + "/"
        url = url.replace("{modelName}", model_name)
        if tenant_id:
            url = url.replace("{tenantName}", tenant_id)
        else:
            url = url.replace("{tenantName}", "test")
        no_cached_measure_res = await async_client.get(url=url)
        if tenant_id:
            assert no_cached_measure_res.status_code == HTTPStatus.OK
            assert clear_uncompared_fields(expected_list) == clear_uncompared_fields(no_cached_measure_res.json())
            cached_measure_res = await async_client.get(url=url)
            assert clear_uncompared_fields(cached_measure_res.json()) == clear_uncompared_fields(
                no_cached_measure_res.json()
            )
        else:
            assert no_cached_measure_res.status_code == HTTPStatus.INTERNAL_SERVER_ERROR

    @pytest.mark.parametrize(
        ("tenant_id", "model_name", "names", "expected", "db_models"),
        [
            ("tenant1", "test_model2", [], [], []),
            (
                "tenant1",
                "test_model1",
                ["test1", "test2"],
                measure_model_list[0:2],
                "measures",
            ),
            (None, "test_model2", [], [], []),
        ],
    )
    async def test_get_measure_list_by_names(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        tenant_id: str,
        model_name: str,
        names: list[str],
        expected: list[MeasureModel],
        db_models: list[Measure],
        request: pytest.FixtureRequest,
    ) -> None:
        if isinstance(db_models, str):
            db_models = request.getfixturevalue(db_models)
        mocked_session.add_all(db_models)
        await mocked_session.commit()
        mock_session_maker(monkeypatch, mocked_session)
        if not tenant_id:
            mock_method_by_http_exception(
                monkeypatch, MeasureService, "get_measure_list_by_names", HTTPStatus.INTERNAL_SERVER_ERROR
            )
        expected_list = [
            MeasureModel.model_dump(MeasureV0.model_validate(i), mode="json", by_alias=True) for i in expected
        ]
        expected_list.sort(key=lambda measure: measure["name"])
        url = PREFIX_URL + "/"
        url = url.replace("{modelName}", model_name)
        if tenant_id:
            url = url.replace("{tenantName}", tenant_id)
        else:
            url = url.replace("{tenantName}", "test")
        no_cached_measure_res = await async_client.post(url=url, json=names)
        if tenant_id:
            assert no_cached_measure_res.status_code == HTTPStatus.OK
            assert clear_uncompared_fields(expected_list) == clear_uncompared_fields(no_cached_measure_res.json())
            cached_measure_res = await async_client.post(url=url, json=names)
            assert clear_uncompared_fields(cached_measure_res.json()) == clear_uncompared_fields(
                no_cached_measure_res.json()
            )
        else:
            assert no_cached_measure_res.status_code == HTTPStatus.INTERNAL_SERVER_ERROR

    @pytest.mark.parametrize(
        ("tenant_id", "model_name", "measure_name", "expected", "expected_status"),
        [
            ("tenant1", "test_model1", "test222", None, HTTPStatus.NOT_FOUND),
            ("tenant1", "test_model1", "test1", measure_model_list[0], HTTPStatus.OK),
            (None, "test_model1", "test1", measure_model_list[0], HTTPStatus.INTERNAL_SERVER_ERROR),
        ],
    )
    async def test_get_measure_by_measure_name(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        measures: list[Measure],
        tenant_id: str,
        model_name: str,
        measure_name: str,
        expected: MeasureModel,
        expected_status: pytest.RaisesExc,
    ) -> None:
        mocked_session.add_all(measures)
        await mocked_session.commit()
        mock_session_maker(monkeypatch, mocked_session)
        if not tenant_id:
            mock_method_by_http_exception(
                monkeypatch, MeasureService, "get_measure_by_measure_name", HTTPStatus.INTERNAL_SERVER_ERROR
            )
        expected_dict = None
        if expected is not None:
            expected_dict = MeasureModel.model_dump(
                MeasureV0.model_validate(expected),
                mode="json",
                by_alias=True,
            )
        url = PREFIX_URL + f"/{measure_name}"
        url = url.replace("{modelName}", model_name)
        if tenant_id:
            url = url.replace("{tenantName}", tenant_id)
        else:
            url = url.replace("{tenantName}", "test")
        no_cached_measure_res = await async_client.get(url=url)
        assert no_cached_measure_res.status_code == expected_status
        if tenant_id and expected_status == HTTPStatus.OK:
            assert clear_uncompared_fields(expected_dict) == clear_uncompared_fields(no_cached_measure_res.json())
            cached_measure_res = await async_client.get(url=url)
            assert clear_uncompared_fields(cached_measure_res.json()) == clear_uncompared_fields(
                no_cached_measure_res.json()
            )

    @pytest.mark.parametrize(
        ("tenant_id", "model_name", "measure_name", "expected_status"),
        [
            ("tenant1", "test_model1", "test1222", HTTPStatus.NOT_FOUND),
            ("tenant1", "test_model1", "test1", HTTPStatus.NO_CONTENT),
            (None, "test_model1", "test1", HTTPStatus.INTERNAL_SERVER_ERROR),
        ],
    )
    async def test_delete_measure_by_name(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        measures: list[Measure],
        tenant_id: str,
        model_name: str,
        measure_name: str,
        expected_status: HTTPStatus,
    ) -> None:
        mocked_session.add_all(measures)
        await mocked_session.commit()
        mock_session_maker(monkeypatch, mocked_session)
        if not tenant_id:
            mock_method_by_http_exception(
                monkeypatch, MeasureService, "delete_measure_by_name", HTTPStatus.INTERNAL_SERVER_ERROR
            )
        url = PREFIX_URL + f"/{measure_name}"
        url = url.replace("{modelName}", model_name)
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
        ("tenant_id", "model_name", "measure", "expected"),
        [
            ("tenant1", "test_model1", measure_model_create_list[0], measure_model_list[3]),
            ("tenant1", "test_model1", measure_model_create_list[1], measure_model_list[4]),
            (None, "test_model1", measure_model_create_list[1], measure_model_list[4]),
        ],
    )
    async def test_create_measure(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        dimensions: list[Dimension],
        models: list[Model],
        tenant_id: str,
        model_name: str,
        measure: MeasureCreateRequestModel,
        expected: MeasureModel,
    ) -> None:
        mocked_session.add_all(models)
        await mocked_session.commit()
        mocked_session.add_all(dimensions)
        await mocked_session.commit()
        mock_session_maker(monkeypatch, mocked_session)
        if not tenant_id:
            mock_method_by_http_exception(
                monkeypatch, MeasureService, "create_measure_by_schema", HTTPStatus.INTERNAL_SERVER_ERROR
            )
        url = PREFIX_URL + "/create"
        url = url.replace("{modelName}", model_name)
        if tenant_id:
            url = url.replace("{tenantName}", tenant_id)
        else:
            url = url.replace("{tenantName}", "test")
        measure_dict = measure.model_dump(exclude_unset=True, mode="json", by_alias=True)
        response = await async_client.post(url=url, json=measure_dict)
        if tenant_id:
            assert response.status_code == HTTPStatus.CREATED
            assert clear_uncompared_fields(response.json()) == clear_uncompared_fields(
                MeasureV0.model_validate(expected).model_dump(mode="json", by_alias=True)
            )
            response = await async_client.get(url=url.replace("create", "") + measure_dict["name"])
            assert response.status_code == HTTPStatus.OK
        else:
            assert response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR

    @pytest.mark.parametrize(
        ("tenant_id", "model_name", "measure_name", "update_measure"),
        [
            ("tenant1", "test_model1", "test2", measure_model_update_list[0]),
            ("tenant1", "test_model1", "test1", measure_model_update_list[1]),
            (None, "test_model1", "test1", measure_model_update_list[1]),
            ("tenant1", "test_model1", "unknown", measure_model_update_list[1]),
        ],
    )
    async def test_update_measure_by_name(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        measures: list[Measure],
        dimensions: list[Dimension],
        models: list[Model],
        tenant_id: str,
        model_name: str,
        measure_name: str,
        update_measure: MeasureEditRequestModel,
    ) -> None:
        mocked_session.add_all(models)
        await mocked_session.commit()
        mocked_session.add_all(dimensions)
        await mocked_session.commit()
        mocked_session.add_all(measures)
        await mocked_session.commit()
        mock_session_maker(monkeypatch, mocked_session)
        if not tenant_id:
            mock_method_by_http_exception(
                monkeypatch, MeasureService, "get_updated_fields", HTTPStatus.INTERNAL_SERVER_ERROR
            )
        update_measure_dict = update_measure.model_dump(exclude_unset=True, mode="json", by_alias=True)
        url = PREFIX_URL + f"/{measure_name}"
        url = url.replace("{modelName}", model_name)
        if tenant_id:
            url = url.replace("{tenantName}", tenant_id)
        else:
            url = url.replace("{tenantName}", "test")
        original_model_dict = clear_uncompared_fields((await async_client.get(url=url)).json())
        response = await async_client.patch(url=url, json=update_measure_dict)
        if measure_name == "unknown":
            assert response.status_code == HTTPStatus.NOT_FOUND
            return
        if tenant_id:
            assert response.status_code == HTTPStatus.OK
            response_dict = clear_uncompared_fields(response.json())
            for field, value in update_measure_dict.items():
                assert response_dict[field] == value
                original_model_dict.pop(field)
            for field, value in original_model_dict.items():
                assert response_dict[field] == value
        else:
            assert response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR
