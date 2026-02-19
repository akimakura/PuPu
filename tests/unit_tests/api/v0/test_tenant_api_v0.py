from http import HTTPStatus
from typing import Optional

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Composite

from src.api.v0.const import TENANT_URL, V0_PREFIX_URL
from src.db import DataStorage, Dimension, Measure, Model
from src.db.tenant import Tenant
from src.main import API_PREFIX_URL
from src.models.tenant import (
    Tenant as TenantModel,
    TenantCreateRequest as TenantCreateRequestModel,
    TenantEditRequest as TenantEditRequestModel,
)
from src.service.tenant import TenantService
from tests.unit_tests.conftest import mock_method_by_http_exception, mock_session_maker
from tests.unit_tests.fixtures.tenant import tenant_model_create_list, tenant_model_list, tenant_model_update_list
from tests.unit_tests.utils import clear_uncompared_fields

PREFIX_URL = API_PREFIX_URL + V0_PREFIX_URL + TENANT_URL


class TestTenantApiV0:

    @pytest.mark.parametrize(
        ("expected", "db_models"),
        [
            ([], []),
            (
                tenant_model_list[:2],
                "tenants",
            ),
            (None, []),
        ],
    )
    async def test_get_tenant_list(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        expected: Optional[list[TenantModel]],
        db_models: list[Tenant],
        request: pytest.FixtureRequest,
    ) -> None:
        if isinstance(db_models, str):
            db_models = request.getfixturevalue(db_models)
        mocked_session.add_all(db_models)
        await mocked_session.commit()
        mock_session_maker(monkeypatch, mocked_session)
        if expected is None:
            mock_method_by_http_exception(
                monkeypatch, TenantService, "get_tenant_list", HTTPStatus.INTERNAL_SERVER_ERROR
            )
        url = PREFIX_URL + "/"
        no_cached_tenant_res = await async_client.get(url=url)
        if expected is not None:
            assert no_cached_tenant_res.status_code == HTTPStatus.OK
            expected_list = [TenantModel.model_dump(i, mode="json", by_alias=True) for i in expected]
            expected_list.sort(key=lambda tenant: tenant["name"])
            assert clear_uncompared_fields(expected_list) == clear_uncompared_fields(no_cached_tenant_res.json())
            cached_tenant_res = await async_client.get(url=url)
            assert clear_uncompared_fields(cached_tenant_res.json()) == clear_uncompared_fields(
                no_cached_tenant_res.json()
            )
        else:
            assert no_cached_tenant_res.status_code == HTTPStatus.INTERNAL_SERVER_ERROR

    @pytest.mark.parametrize(
        ("tenant_id", "expected", "expected_status"),
        [
            ("sdfsdf", None, HTTPStatus.NOT_FOUND),
            ("tenant1", tenant_model_list[0], HTTPStatus.OK),
            (None, tenant_model_list[0], HTTPStatus.INTERNAL_SERVER_ERROR),
        ],
    )
    async def test_get_tenant_by_name(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        tenants: list[Tenant],
        tenant_id: Optional[str],
        expected: Optional[TenantModel],
        expected_status: HTTPStatus,
    ) -> None:
        mocked_session.add_all(tenants)
        await mocked_session.commit()
        mock_session_maker(monkeypatch, mocked_session)
        if not tenant_id:
            mock_method_by_http_exception(
                monkeypatch, TenantService, "get_tenant_by_name", HTTPStatus.INTERNAL_SERVER_ERROR
            )
        expected_dict = None
        if expected is not None:
            expected_dict = TenantModel.model_dump(
                expected,
                mode="json",
                by_alias=True,
            )
        url = PREFIX_URL + f"/{tenant_id}"
        no_cached_datastorage_res = await async_client.get(url=url)
        assert no_cached_datastorage_res.status_code == expected_status
        if tenant_id and expected_status != HTTPStatus.NOT_FOUND:
            assert clear_uncompared_fields(expected_dict) == clear_uncompared_fields(no_cached_datastorage_res.json())
            cached_datastorage_res = await async_client.get(url=url)
            assert clear_uncompared_fields(cached_datastorage_res.json()) == clear_uncompared_fields(
                no_cached_datastorage_res.json()
            )

    @pytest.mark.parametrize(
        ("tenant_id", "expected_status"),
        [
            ("tenantgg1", HTTPStatus.NOT_FOUND),
            ("tenant1", HTTPStatus.NO_CONTENT),
            (None, HTTPStatus.INTERNAL_SERVER_ERROR),
        ],
    )
    async def test_delete_tenant_by_name(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        tenants: list[Tenant],
        tenant_id: str,
        expected_status: HTTPStatus,
    ) -> None:
        mocked_session.add_all(tenants)
        await mocked_session.commit()
        mock_session_maker(monkeypatch, mocked_session)
        if not tenant_id:
            mock_method_by_http_exception(
                monkeypatch, TenantService, "delete_tenant_by_name", HTTPStatus.INTERNAL_SERVER_ERROR
            )
        url = PREFIX_URL + f"/{tenant_id}"
        response = await async_client.delete(url=url)
        assert response.status_code == expected_status
        if tenant_id and response.status_code == HTTPStatus.NO_CONTENT:
            response = await async_client.get(url=url)
            assert response.status_code == HTTPStatus.NOT_FOUND

    @pytest.mark.parametrize(
        ("tenant", "expected"),
        [
            (tenant_model_create_list[0], tenant_model_list[2]),
            (tenant_model_create_list[1], tenant_model_list[3]),
            (tenant_model_create_list[1], None),
        ],
    )
    async def test_create_tenant(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        tenant: TenantCreateRequestModel,
        expected: Optional[TenantModel],
    ) -> None:
        mock_session_maker(monkeypatch, mocked_session)
        if expected is None:
            mock_method_by_http_exception(
                monkeypatch, TenantService, "create_tenant_by_schema", HTTPStatus.INTERNAL_SERVER_ERROR
            )
        url = PREFIX_URL + "/"
        tenant_dict = tenant.model_dump(exclude_unset=True, mode="json", by_alias=True)
        response = await async_client.post(url=url, json=tenant_dict)
        if expected is not None:
            assert response.status_code == HTTPStatus.CREATED
            assert clear_uncompared_fields(response.json()) == clear_uncompared_fields(
                expected.model_dump(mode="json", by_alias=True)
            )
            response = await async_client.get(url=url + tenant_dict["name"])
            assert response.status_code == HTTPStatus.OK
        else:
            assert response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR

    @pytest.mark.parametrize(
        ("tenant_id", "update_tenant"),
        [
            ("tenant1", tenant_model_update_list[0]),
            ("tenant1", tenant_model_update_list[1]),
            (None, tenant_model_update_list[1]),
            ("unknown", tenant_model_update_list[1]),
        ],
    )
    async def test_update_tenant_by_name(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        tenant_id: str,
        update_tenant: TenantEditRequestModel,
        tenants: list[Tenant],
    ) -> None:
        mocked_session.add_all(tenants)
        await mocked_session.commit()
        mock_session_maker(monkeypatch, mocked_session)
        if not tenant_id:
            mock_method_by_http_exception(
                monkeypatch, TenantService, "get_updated_fields", HTTPStatus.INTERNAL_SERVER_ERROR
            )
        update_tenant_dict = update_tenant.model_dump(exclude_unset=True, mode="json", by_alias=True)
        url = PREFIX_URL + f"/{tenant_id}"
        original_model_dict = clear_uncompared_fields((await async_client.get(url=url)).json())
        response = await async_client.patch(url=url, json=update_tenant_dict)
        if tenant_id == "unknown":
            assert response.status_code == HTTPStatus.NOT_FOUND
            return
        if tenant_id:
            assert response.status_code == HTTPStatus.OK
            response_dict = clear_uncompared_fields(response.json())
            for field, value in update_tenant_dict.items():
                assert response_dict[field] == value
                original_model_dict.pop(field)
            for field, value in original_model_dict.items():
                assert response_dict[field] == value
        else:
            assert response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR

    @pytest.mark.asyncio
    async def test_search_elements(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        tenants: list[Tenant],
        measures: list[Measure],
        dimensions: list[Dimension],
        data_storages: list[DataStorage],
        models: list[Model],
        composites: list[Composite],
        tenant_id: Optional[str] = "tenant1",
    ) -> None:
        mocked_session.add_all(measures)
        mocked_session.add_all(dimensions)
        mocked_session.add_all(data_storages)
        mocked_session.add_all(models)
        mocked_session.add_all(composites)
        mocked_session.add_all(tenants)
        await mocked_session.commit()
        mock_session_maker(monkeypatch, mocked_session)

        url_one = PREFIX_URL + (
            f"/{tenant_id}/search/objects" f"?query=test_dim7_values&modelName=test_model1&type=DATA_STORAGE"
        )
        url_two = PREFIX_URL + (
            f"/{tenant_id}/search/objects" f"?query=test_dim7_values&modelName=test_model1&type=DIMENSION"
        )

        result_one = (await async_client.get(url=url_one)).json()
        result_two = (await async_client.get(url=url_two)).json()

        expected_one = {
            "results": {
                "test_model1": {
                    "composites": [],
                    "data_storages": ["test_dim7_values"],
                    "dimensions": [],
                    "measures": [],
                }
            }
        }

        assert result_one == expected_one
        assert result_two == {"results": {}}

    @pytest.mark.parametrize(
        ("object_name", "object_type", "expected"),
        [
            (
                "test1",
                "MEASURE",
                {
                    "results": {
                        "test_model1": {
                            "composites": [
                                "test_composite1",
                                "test_composite2",
                            ],
                            "data_storages": [
                                "test_dim5_attributes",
                                "test_dim5_texts",
                                "test_dim5_values",
                                "test_dim7_values",
                                "test_dim8_values",
                                "test_dim9_values",
                                "test_dim1_values",
                                "test_dim2_values",
                                "test_dim3_values",
                                "test_dim4_values",
                                "test_dso1",
                                "test_dso2",
                            ],
                            "dimensions": [],
                            "measures": [],
                        }
                    }
                },
            ),
            (
                "test_dim5",
                "DIMENSION",
                {
                    "results": {
                        "test_model1": {
                            "composites": ["test_composite1"],
                            "dimensions": [],
                            "measures": [],
                            "data_storages": ["test_dso1"],
                        }
                    }
                },
            ),
            (
                "test_composite1",
                "COMPOSITE",
                {
                    "results": {
                        "test_model1": {
                            "composites": [],
                            "data_storages": [],
                            "dimensions": [],
                            "measures": [],
                        }
                    }
                },
            ),
            (
                "test_dso1",
                "DATA_STORAGE",
                {
                    "results": {
                        "test_model1": {
                            "composites": [
                                "test_composite1",
                                "test_composite2",
                            ],
                            "data_storages": [],
                            "dimensions": [],
                            "measures": [],
                        }
                    }
                },
            ),
        ],
    )
    async def test_find_where_used(
        self,
        object_name: str,
        object_type: str,
        expected: dict,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        tenants: list[Tenant],
        measures: list[Measure],
        dimensions: list[Dimension],
        data_storages: list[DataStorage],
        models: list[Model],
        composites: list[Composite],
        tenant_id: Optional[str] = "tenant1",
    ) -> None:
        mocked_session.add_all(measures)
        mocked_session.add_all(dimensions)
        mocked_session.add_all(data_storages)
        mocked_session.add_all(models)
        mocked_session.add_all(composites)
        mocked_session.add_all(tenants)
        await mocked_session.commit()
        mock_session_maker(monkeypatch, mocked_session)

        url = PREFIX_URL + f"/{tenant_id}/models/{models[0].name}/objectLinks/{object_name}?type={object_type}"

        result = (await async_client.get(url=url)).json()

        assert result == expected
