from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.v0.const import DATASTORAGE_URL, DIMENSION_URL, MODEL_NAME_URL, TENANT_NAME_URL, V0_PREFIX_URL
from src.config import settings
from src.db.dimension import Dimension
from src.db.measure import Measure
from src.db.model import Model
from src.main import API_PREFIX_URL
from src.models.dimension import (
    Dimension as DimensionModel,
    DimensionCreateRequest as DimensionCreateRequestModel,
    DimensionEditRequest as DimensionEditRequestModel,
    DimensionV0,
)
from src.models.model_import import ImportFromFileResponse
from src.service.dimension import DimensionService
from tests.unit_tests.conftest import mock_method_by_http_exception, mock_session_maker
from tests.unit_tests.fixtures.dimension import (
    dimension_model_create_list,
    dimension_model_list,
    dimension_model_update_list,
    test_create_dimension_model_models,
)
from tests.unit_tests.utils import clear_uncompared_fields

PREFIX_URL = API_PREFIX_URL + V0_PREFIX_URL + TENANT_NAME_URL + MODEL_NAME_URL
DATA_STORAGE_PREFIX = API_PREFIX_URL + V0_PREFIX_URL + TENANT_NAME_URL + DATASTORAGE_URL


class TestDimensionApiV0:

    @pytest.mark.parametrize(
        ("tenant_id", "model_name", "expected", "db_models"),
        [
            ("tenant1", "test_model1", [], []),
            (
                "tenant1",
                "test_model1",
                dimension_model_list[:8],
                "dimensions",
            ),
            (None, "test_model1", [], []),
        ],
    )
    async def test_get_dimension_list_by_model_name(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        model_name: str,
        tenant_id: str,
        expected: list[DimensionModel],
        db_models: list[Dimension],
        request: pytest.FixtureRequest,
    ) -> None:
        if isinstance(db_models, str):
            db_models = request.getfixturevalue(db_models)
        mocked_session.add_all(db_models)
        await mocked_session.commit()
        mock_session_maker(monkeypatch, mocked_session)
        if not tenant_id:
            mock_method_by_http_exception(
                monkeypatch, DimensionService, "get_dimension_list_by_model_name", HTTPStatus.INTERNAL_SERVER_ERROR
            )
        expected_list = [
            clear_uncompared_fields(
                DimensionModel.model_dump(DimensionV0.model_validate(i), mode="json", by_alias=True)
            )
            for i in expected
        ]
        expected_list.sort(key=lambda dimension: dimension["name"])
        url = PREFIX_URL + DIMENSION_URL + "/"
        url = url.replace("{modelName}", model_name)
        if tenant_id:
            url = url.replace("{tenantName}", tenant_id)
        else:
            url = url.replace("{tenantName}", "test")
        no_cached_dimension_res = await async_client.get(url=url)
        if tenant_id:
            assert no_cached_dimension_res.status_code == HTTPStatus.OK
            assert expected_list == clear_uncompared_fields(no_cached_dimension_res.json())
            cached_dimension_res = await async_client.get(url=url)
            assert clear_uncompared_fields(cached_dimension_res.json()) == clear_uncompared_fields(
                no_cached_dimension_res.json()
            )
        else:
            assert no_cached_dimension_res.status_code == HTTPStatus.INTERNAL_SERVER_ERROR

    @pytest.mark.parametrize(
        ("tenant_id", "model_name", "names", "expected", "db_models"),
        [
            (
                "tenant1",
                "test_model1",
                [
                    "test_dim5",
                ],
                [dimension_model_list[0]],
                "dimensions",
            ),
            (
                "tenant1",
                "test_model1",
                [],
                [],
                "dimensions",
            ),
            (
                None,
                "test_model1",
                [],
                [],
                "dimensions",
            ),
        ],
    )
    async def test_get_dimension_by_names(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        tenant_id: str,
        model_name: str,
        names: list[str],
        expected: list[DimensionModel],
        db_models: list[Dimension],
        request: pytest.FixtureRequest,
    ) -> None:
        if isinstance(db_models, str):
            db_models = request.getfixturevalue(db_models)
        mocked_session.add_all(db_models)
        await mocked_session.commit()
        mock_session_maker(monkeypatch, mocked_session)
        if not tenant_id:
            mock_method_by_http_exception(
                monkeypatch, DimensionService, "get_dimension_list_by_names", HTTPStatus.INTERNAL_SERVER_ERROR
            )
        expected_list = [
            clear_uncompared_fields(
                DimensionModel.model_dump(DimensionV0.model_validate(i), mode="json", by_alias=True)
            )
            for i in expected
        ]
        expected_list.sort(key=lambda dimension: dimension["name"])
        url = PREFIX_URL + DIMENSION_URL + "/"
        url = url.replace("{modelName}", model_name)
        if tenant_id:
            url = url.replace("{tenantName}", tenant_id)
        else:
            url = url.replace("{tenantName}", "test")
        no_cached_dimension_res = await async_client.post(url=url, json=names)
        if tenant_id:
            assert no_cached_dimension_res.status_code == HTTPStatus.OK
            assert expected_list == clear_uncompared_fields(no_cached_dimension_res.json())
            cached_dimension_res = await async_client.post(url=url, json=names)
            assert clear_uncompared_fields(cached_dimension_res.json()) == clear_uncompared_fields(
                no_cached_dimension_res.json()
            )
        else:
            assert no_cached_dimension_res.status_code == HTTPStatus.INTERNAL_SERVER_ERROR

    @pytest.mark.parametrize(
        ("tenant_id", "model_name", "name", "expected", "expected_status"),
        [
            ("tenant1", "test_model1", "test_dim5555", None, HTTPStatus.NOT_FOUND),
            ("tenant1", "test_model1", "test_dim5", dimension_model_list[0], HTTPStatus.OK),
            (None, "test_model1", "test_dim5", dimension_model_list[0], HTTPStatus.INTERNAL_SERVER_ERROR),
        ],
    )
    async def test_get_dimension_by_dimension_name(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        dimensions: list[Dimension],
        tenant_id: str,
        model_name: str,
        name: str,
        expected: DimensionModel,
        expected_status: HTTPStatus,
    ) -> None:
        mocked_session.add_all(dimensions)
        await mocked_session.commit()
        mock_session_maker(monkeypatch, mocked_session)
        if not tenant_id:
            mock_method_by_http_exception(
                monkeypatch, DimensionService, "get_dimension_by_dimension_name", HTTPStatus.INTERNAL_SERVER_ERROR
            )
        expected_dict = None
        if expected is not None:
            expected_dict = clear_uncompared_fields(
                DimensionModel.model_dump(
                    DimensionV0.model_validate(expected),
                    mode="json",
                    by_alias=True,
                )
            )
        url = PREFIX_URL + DIMENSION_URL + f"/{name}"
        url = url.replace("{modelName}", model_name)
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
        ("tenant_id", "model_name", "dimension_name", "expected_status"),
        [
            ("tenant1", "test_model1", "fsdf", HTTPStatus.NOT_FOUND),
            ("tenant1", "test_model1", "test_dim8", HTTPStatus.NO_CONTENT),
            (None, "test_model1", "test_dim8", HTTPStatus.INTERNAL_SERVER_ERROR),
        ],
    )
    async def test_delete_dimension_by_name(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        dimensions: list[Dimension],
        tenant_id: str,
        model_name: str,
        dimension_name: str,
        expected_status: HTTPStatus,
    ) -> None:
        mocked_session.add_all(dimensions)
        await mocked_session.commit()
        mock_session_maker(monkeypatch, mocked_session)
        if not tenant_id:
            mock_method_by_http_exception(
                monkeypatch, DimensionService, "delete_dimension_by_name", HTTPStatus.INTERNAL_SERVER_ERROR
            )
        url = PREFIX_URL + DIMENSION_URL + f"/{dimension_name}"
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
        ("tenant_id", "model_name", "dimension", "expected", "expected_text_status", "expected_attribute_status"),
        [
            (
                "tenant1",
                "test_model1",
                dimension_model_create_list[0],
                dimension_model_list[8],
                HTTPStatus.OK,
                HTTPStatus.OK,
            ),
            (
                "tenant1",
                "test_model1",
                dimension_model_create_list[1],
                dimension_model_list[9],
                HTTPStatus.NOT_FOUND,
                HTTPStatus.OK,
            ),
            (
                None,
                "test_model1",
                dimension_model_create_list[1],
                dimension_model_list[9],
                HTTPStatus.NOT_FOUND,
                HTTPStatus.NOT_FOUND,
            ),
        ],
    )
    async def test_create_dimension(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        measures: list[Measure],
        dimensions: list[Dimension],
        models: list[Model],
        tenant_id: str,
        model_name: str,
        dimension: DimensionCreateRequestModel,
        expected: DimensionModel,
        expected_text_status: HTTPStatus,
        expected_attribute_status: HTTPStatus,
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
                monkeypatch, DimensionService, "create_dimension_by_schema", HTTPStatus.INTERNAL_SERVER_ERROR
            )
        create_url = PREFIX_URL + DIMENSION_URL + "/create"
        create_url = create_url.replace("{modelName}", model_name)
        if tenant_id:
            create_url = create_url.replace("{tenantName}", tenant_id)
        else:
            create_url = create_url.replace("{tenantName}", "test")
        dimension_dict = clear_uncompared_fields(dimension.model_dump(exclude_unset=True, mode="json", by_alias=True))
        response = await async_client.post(url=create_url, json=dimension_dict)
        if tenant_id:
            assert response.status_code == HTTPStatus.CREATED
            response_dimension_dict = clear_uncompared_fields(response.json())
            assert clear_uncompared_fields(response.json()) == clear_uncompared_fields(
                DimensionV0.model_validate(expected).model_dump(mode="json", by_alias=True)
            )
            get_url = PREFIX_URL + DIMENSION_URL + "/" + response_dimension_dict["name"]
            get_url = get_url.replace("{modelName}", model_name)
            if tenant_id:
                get_url = get_url.replace("{tenantName}", tenant_id)
            else:
                get_url = get_url.replace("{tenantName}", "test")
            response = await async_client.get(url=get_url)
            assert response.status_code == HTTPStatus.OK
            get_data_storage_url = DATA_STORAGE_PREFIX + "/"
            get_data_storage_url = get_data_storage_url.replace("{modelName}", model_name)
            get_data_storage_url = get_data_storage_url.replace("{tenantName}", tenant_id)
            if tenant_id:
                get_data_storage_url = get_data_storage_url.replace("{tenantName}", tenant_id)
            else:
                get_data_storage_url = get_data_storage_url.replace("{tenantName}", "test")
            response = await async_client.get(url=get_data_storage_url + str(response_dimension_dict.get("textTable")))
            assert response.status_code == expected_text_status
            response = await async_client.get(
                url=get_data_storage_url + str(response_dimension_dict.get("attributesTable"))
            )
            assert response.status_code == expected_attribute_status
        else:
            assert response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR

    @pytest.mark.parametrize(
        ("tenant_id", "model_name", "dimension_name", "update_dimension"),
        [
            ("tenant1", "test_model1", "test_dim5", dimension_model_update_list[0]),
            ("tenant1", "test_model1", "test_dim5", dimension_model_update_list[1]),
            ("tenant1", "test_model1", "test_dim5", dimension_model_update_list[2]),
            ("tenant1", "test_model1", "test_dim3", dimension_model_update_list[3]),
            (None, "test_model1", "test_dim5", dimension_model_update_list[2]),
            ("tenant1", "test_model1", "unknown", dimension_model_update_list[3]),
        ],
    )
    async def test_update_dimension_by_name(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        measures: list[Measure],
        dimensions: list[Dimension],
        models: list[Model],
        tenant_id: str,
        model_name: str,
        dimension_name: str,
        update_dimension: DimensionEditRequestModel,
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
                monkeypatch, DimensionService, "get_updated_fields", HTTPStatus.INTERNAL_SERVER_ERROR
            )
        update_dimension_dict = update_dimension.model_dump(exclude_unset=True, mode="json", by_alias=True)
        url = PREFIX_URL + DIMENSION_URL + f"/{dimension_name}"
        url = url.replace("{modelName}", model_name)
        if tenant_id:
            url = url.replace("{tenantName}", tenant_id)
        else:
            url = url.replace("{tenantName}", "test")
        original_model_dict = clear_uncompared_fields((await async_client.get(url=url)).json())
        response = await async_client.patch(url=url, json=update_dimension_dict)
        if dimension_name == "unknown":
            assert response.status_code == HTTPStatus.NOT_FOUND
            return
        if tenant_id:
            assert response.status_code == HTTPStatus.OK
            response_dict = clear_uncompared_fields(response.json())
            get_data_storage_url = DATA_STORAGE_PREFIX + "/"
            get_data_storage_url = get_data_storage_url.replace("{modelName}", model_name)
            if tenant_id:
                get_data_storage_url = get_data_storage_url.replace("{tenantName}", tenant_id)
            else:
                get_data_storage_url = get_data_storage_url.replace("{tenantName}", "test")
            if len(response_dict["valueTexts"]) == 0 or response_dict["dimensionRef"] is not None:
                assert response_dict["textTable"] is None
            else:
                assert response_dict["textTable"] == settings.TEXT_DATASTORAGE_PATTERN % dimension_name
                data_storage_resp = await async_client.get(url=get_data_storage_url + str(response_dict["textTable"]))
                assert data_storage_resp.status_code == HTTPStatus.OK

            if len(response_dict["valueAttributes"]) == 0 or response_dict["dimensionRef"] is not None:
                assert response_dict["attributesTable"] is None
            else:
                assert response_dict["attributesTable"] == settings.ATTRIBUTE_DATASTORAGE_PATTERN % dimension_name
                data_storage_resp = await async_client.get(
                    url=get_data_storage_url + str(response_dict["attributesTable"])
                )
                assert data_storage_resp.status_code == HTTPStatus.OK
            if response_dict["dimensionRef"] is not None:
                assert response_dict["valuesTable"] is None
            for field, value in update_dimension_dict.items():
                assert response_dict[field] == value
                original_model_dict.pop(field)
            for field, value in original_model_dict.items():
                if field != "textTable" and field != "attributesTable" and field != "valuesTable":
                    assert response_dict[field] == value
        else:
            assert response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR

    @pytest.mark.parametrize(
        ("tenant_id", "model_name", "dimensions_file_path", "attributes_file_path", "expected", "expected_list"),
        [
            (
                "tenant1",
                "test_model1",
                "tests/unit_tests/fixtures/data_xlsx/test_import_dimension.xlsx",
                "tests/unit_tests/fixtures/data_xlsx/test_import_dimension_attrs.xlsx",
                ImportFromFileResponse(
                    created=["unit_test1", "unit_test2", "unit_test3", "unit_test4"],
                    updated=["unit_test1", "unit_test2", "unit_test3"],
                ),
                test_create_dimension_model_models,
            ),
        ],
    )
    async def test_create_model_dimension(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        models: list[Model],
        tenant_id: str,
        model_name: str,
        dimensions_file_path: str,
        attributes_file_path: str,
        expected: ImportFromFileResponse,
        expected_list: list,
    ) -> None:
        mocked_session.add_all(models)
        await mocked_session.commit()
        mock_session_maker(monkeypatch, mocked_session)
        if not tenant_id:
            mock_method_by_http_exception(
                monkeypatch, DimensionService, "create_dimension_by_schema", HTTPStatus.INTERNAL_SERVER_ERROR
            )
        url = PREFIX_URL + DIMENSION_URL + "/create/model"
        url = url.replace("{tenantName}", tenant_id)
        url = url.replace("{modelName}", model_name)
        with open(dimensions_file_path, "rb") as file:
            dimensions = file.read()
        with open(attributes_file_path, "rb") as file:
            attrs = file.read()
        files = {
            "dimensions": (
                "test_import_dimension.xlsx",
                dimensions,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ),
            "attributes": (
                "test_import_dimension_attrs.xlsx",
                attrs,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ),
        }
        response = await async_client.post(url=url, files=files)
        resp_body = response.json()
        resp_body["created"].sort()
        resp_body["updated"].sort()
        assert expected.model_dump(mode="json", by_alias=True) == resp_body
        url = PREFIX_URL + DIMENSION_URL + "/"
        url = url.replace("{modelName}", model_name)
        if tenant_id:
            url = url.replace("{tenantName}", tenant_id)
        else:
            url = url.replace("{tenantName}", "test")
        result_created = []
        for created in resp_body["created"]:
            response = await async_client.get(url=url + created)
            result_created.append(response.json())
        expected_list = [
            DimensionV0.model_dump(DimensionV0.model_validate(i), mode="json", by_alias=True) for i in expected_list
        ]
        assert clear_uncompared_fields(expected_list) == clear_uncompared_fields(result_created)
