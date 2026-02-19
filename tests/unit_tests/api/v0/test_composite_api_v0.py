from http import HTTPStatus
from typing import Optional

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.v0.const import COMPOSITE_URL, TENANT_NAME_URL, V0_PREFIX_URL
from src.db.composite import Composite
from src.db.data_storage import DataStorage
from src.db.dimension import Dimension
from src.db.measure import Measure
from src.db.model import Model
from src.main import API_PREFIX_URL
from src.models.composite import (
    Composite as CompositeModel,
    CompositeCreateRequest as CompositeCreateRequestModel,
    CompositeEditRequest as CompositeEditRequestModel,
    CompositeV0,
)
from src.service.composite import CompositeService
from tests.unit_tests.conftest import mock_method_by_http_exception, mock_session_maker
from tests.unit_tests.fixtures.composite import (
    composite_model_create_list,
    composite_model_list,
    composite_model_update_list,
    sql_expressions,
)
from tests.unit_tests.utils import clear_uncompared_fields

PREFIX_URL = API_PREFIX_URL + V0_PREFIX_URL + TENANT_NAME_URL + COMPOSITE_URL


class TestCompositeApiV0:

    @pytest.mark.parametrize(
        ("tenant_id", "model_name", "expected", "db_models"),
        [
            ("tenant1", "test_model1", [], []),
            (
                "tenant1",
                "test_model1",
                composite_model_list[:2],
                "composites",
            ),
            (None, "test_model1", [], []),
        ],
    )
    async def test_get_composite_list_by_model_name(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        measures: list[Measure],
        dimensions: list[Dimension],
        data_storages: list[DataStorage],
        models: list[Model],
        tenant_id: Optional[str],
        model_name: str,
        expected: list[CompositeModel],
        db_models: list[Composite],
        request: pytest.FixtureRequest,
    ) -> None:
        mocked_session.add_all(models)
        await mocked_session.commit()
        mocked_session.add_all(dimensions)
        await mocked_session.commit()
        mocked_session.add_all(measures)
        await mocked_session.commit()
        mocked_session.add_all(data_storages)
        await mocked_session.commit()
        if isinstance(db_models, str):
            db_models = request.getfixturevalue(db_models)
        expected_list = [
            CompositeV0.model_dump(CompositeV0.model_validate(i), mode="json", by_alias=True) for i in expected
        ]
        expected_list.sort(key=lambda composite: composite["name"])
        mocked_session.add_all(db_models)
        await mocked_session.commit()
        mock_session_maker(monkeypatch, mocked_session)
        if not tenant_id:
            mock_method_by_http_exception(
                monkeypatch, CompositeService, "get_composite_list_by_model_name", HTTPStatus.INTERNAL_SERVER_ERROR
            )
        url = PREFIX_URL + "/"
        url = url.replace("{modelName}", model_name)
        if tenant_id:
            url = url.replace("{tenantName}", tenant_id)
        else:
            url = url.replace("{tenantName}", "test")
        no_cached_composites_res = await async_client.get(url=url)
        if tenant_id:
            assert no_cached_composites_res.status_code == HTTPStatus.OK
            assert clear_uncompared_fields(expected_list) == clear_uncompared_fields(no_cached_composites_res.json())
            cached_composites_res = await async_client.get(url=url)
            assert clear_uncompared_fields(cached_composites_res.json()) == clear_uncompared_fields(
                no_cached_composites_res.json()
            )
        else:
            assert no_cached_composites_res.status_code == HTTPStatus.INTERNAL_SERVER_ERROR

    @pytest.mark.parametrize(
        ("tenant_id", "model_name", "composite_name", "expected", "expected_status"),
        [
            ("tenant1", "test_model1", "test222", None, HTTPStatus.NOT_FOUND),
            ("tenant1", "test_model1", "test_composite1", composite_model_list[0], HTTPStatus.OK),
            (None, "test_model1", "test_composite1", composite_model_list[0], HTTPStatus.INTERNAL_SERVER_ERROR),
        ],
    )
    async def test_get_composite_by_composite_name(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        composites: list[Composite],
        measures: list[Measure],
        dimensions: list[Dimension],
        data_storages: list[DataStorage],
        models: list[Model],
        tenant_id: Optional[str],
        model_name: str,
        composite_name: str,
        expected: CompositeModel,
        expected_status: HTTPStatus,
    ) -> None:
        mocked_session.add_all(models)
        await mocked_session.commit()
        mocked_session.add_all(dimensions)
        await mocked_session.commit()
        mocked_session.add_all(measures)
        await mocked_session.commit()
        mocked_session.add_all(data_storages)
        await mocked_session.commit()
        mocked_session.add_all(composites)
        await mocked_session.commit()
        expected_dict = None
        if expected is not None:
            expected_dict = CompositeV0.model_dump(
                CompositeV0.model_validate(expected),
                mode="json",
                by_alias=True,
            )
        mock_session_maker(monkeypatch, mocked_session)
        if not tenant_id:
            mock_method_by_http_exception(
                monkeypatch, CompositeService, "get_composite_by_name", HTTPStatus.INTERNAL_SERVER_ERROR
            )
        url = PREFIX_URL + f"/{composite_name}"
        url = url.replace("{modelName}", model_name)
        if tenant_id:
            url = url.replace("{tenantName}", tenant_id)
        else:
            url = url.replace("{tenantName}", "test")
        no_cached_composites_res = await async_client.get(url=url)
        assert no_cached_composites_res.status_code == expected_status
        if tenant_id and expected_status == HTTPStatus.OK:
            assert clear_uncompared_fields(expected_dict) == clear_uncompared_fields(no_cached_composites_res.json())
            cached_composite_res = await async_client.get(url=url)
            assert clear_uncompared_fields(cached_composite_res.json()) == clear_uncompared_fields(
                no_cached_composites_res.json()
            )

    @pytest.mark.parametrize(
        ("tenant_id", "model_name", "composite_name", "expected_status"),
        [
            ("tenant1", "test_model1", "test_composite123", HTTPStatus.NOT_FOUND),
            ("tenant1", "test_model1", "test_composite1", HTTPStatus.NO_CONTENT),
            (None, "test_model1", "test_composite1", HTTPStatus.INTERNAL_SERVER_ERROR),
        ],
    )
    async def test_delete_composite_by_name(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        measures: list[Measure],
        dimensions: list[Dimension],
        data_storages: list[DataStorage],
        models: list[Model],
        composites: list[Composite],
        tenant_id: str,
        model_name: str,
        composite_name: str,
        expected_status: HTTPStatus,
    ) -> None:
        mocked_session.add_all(models)
        await mocked_session.commit()
        mocked_session.add_all(dimensions)
        await mocked_session.commit()
        mocked_session.add_all(measures)
        await mocked_session.commit()
        mocked_session.add_all(data_storages)
        await mocked_session.commit()
        mocked_session.add_all(composites)
        await mocked_session.commit()
        mock_session_maker(monkeypatch, mocked_session)
        if not tenant_id:
            mock_method_by_http_exception(
                monkeypatch, CompositeService, "delete_composite_by_name", HTTPStatus.INTERNAL_SERVER_ERROR
            )
        url = PREFIX_URL + f"/{composite_name}"
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
        ("tenant_id", "model_name", "composite", "expected", "expected_expression"),
        [
            ("tenant1", "test_model1", composite_model_create_list[0], composite_model_list[0], sql_expressions[0]),
            (None, "test_model1", composite_model_create_list[0], composite_model_list[1], sql_expressions[0]),
        ],
    )
    async def test_create_composite(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        measures: list[Measure],
        dimensions: list[Dimension],
        data_storages: list[DataStorage],
        models: list[Model],
        tenant_id: Optional[str],
        model_name: str,
        composite: CompositeCreateRequestModel,
        expected: CompositeModel,
        expected_expression: str,
    ) -> None:
        mocked_session.add_all(models)
        await mocked_session.commit()
        mocked_session.add_all(dimensions)
        await mocked_session.commit()
        mocked_session.add_all(measures)
        await mocked_session.commit()
        mocked_session.add_all(data_storages)
        await mocked_session.commit()
        mock_session_maker(monkeypatch, mocked_session)
        if not tenant_id:
            mock_method_by_http_exception(
                monkeypatch, CompositeService, "create_composite_by_schema", HTTPStatus.INTERNAL_SERVER_ERROR
            )
        url = PREFIX_URL + "/"
        url = url.replace("{modelName}", model_name)
        if tenant_id:
            url = url.replace("{tenantName}", tenant_id)
        else:
            url = url.replace("{tenantName}", "test")
        composite_dict = composite.model_dump(exclude_unset=True, mode="json", by_alias=True)
        response = await async_client.post(url=url, json=composite_dict)
        if tenant_id:
            assert response.status_code == HTTPStatus.CREATED
            assert clear_uncompared_fields(response.json()) == clear_uncompared_fields(
                CompositeV0.model_validate(expected).model_dump(mode="json", by_alias=True)
            )
            response = await async_client.get(url=url + composite_dict["name"])
            assert response.status_code == HTTPStatus.OK
            composite_orm = (
                (
                    await mocked_session.execute(
                        select(Composite).where(
                            Composite.name == composite_dict["name"],
                            Composite.models.any(Model.name == model_name),
                            Composite.tenant_id == tenant_id,
                        )
                    )
                )
                .unique()
                .scalars()
                .one_or_none()
            )
            assert composite_orm is not None
        else:
            assert response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR

    @pytest.mark.parametrize(
        ("tenant_id", "model_name", "composite_name", "update_composite", "exprected_expression"),
        [
            ("tenant1", "test_model1", "test_composite1", composite_model_update_list[0], sql_expressions[1]),
            ("tenant1", "test_model1", "test_composite2", composite_model_update_list[1], sql_expressions[2]),
            (None, "test_model1", "test_composite2", composite_model_update_list[1], sql_expressions[2]),
            ("tenant1", "test_model1", "unknown", composite_model_update_list[1], sql_expressions[2]),
        ],
    )
    async def test_update_composite_by_name(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        measures: list[Measure],
        dimensions: list[Dimension],
        composites: list[Composite],
        data_storages: list[DataStorage],
        models: list[Model],
        tenant_id: Optional[str],
        model_name: str,
        composite_name: str,
        update_composite: CompositeEditRequestModel,
        exprected_expression: str,
    ) -> None:
        mocked_session.add_all(models)
        await mocked_session.commit()
        mocked_session.add_all(dimensions)
        await mocked_session.commit()
        mocked_session.add_all(measures)
        await mocked_session.commit()
        mocked_session.add_all(composites)
        await mocked_session.commit()
        mocked_session.add_all(data_storages)
        await mocked_session.commit()
        mock_session_maker(monkeypatch, mocked_session)
        if not tenant_id:
            mock_method_by_http_exception(
                monkeypatch, CompositeService, "get_updated_fields", HTTPStatus.INTERNAL_SERVER_ERROR
            )
        update_composite_dict = update_composite.model_dump(exclude_unset=True, mode="json", by_alias=True)
        url = PREFIX_URL + f"/{composite_name}"
        url = url.replace("{modelName}", model_name)
        if tenant_id:
            url = url.replace("{tenantName}", tenant_id)
        else:
            url = url.replace("{tenantName}", "test")
        original_model_dict = clear_uncompared_fields((await async_client.get(url=url)).json())
        response = await async_client.patch(url=url, json=update_composite_dict)
        if composite_name == "unknown":
            assert response.status_code == HTTPStatus.NOT_FOUND
            return
        if tenant_id:
            assert response.status_code == HTTPStatus.OK
            response_dict = clear_uncompared_fields(response.json())
            for composite_field in response_dict.get("fields"):
                composite_field.pop("sqlColumnType", None)
            for composite_field in original_model_dict.get("fields"):
                composite_field.pop("sqlColumnType", None)
            for field, value in update_composite_dict.items():
                assert response_dict[field] == value
                original_model_dict.pop(field)
            for field, value in original_model_dict.items():
                assert response_dict[field] == value
            composite_orm = (
                (
                    await mocked_session.execute(
                        select(Composite).where(
                            Composite.name == composite_name,
                            Composite.models.any(Model.name == model_name),
                            Composite.tenant_id == tenant_id,
                        )
                    )
                )
                .unique()
                .scalars()
                .one_or_none()
            )
            assert composite_orm is not None
        else:
            assert response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR
