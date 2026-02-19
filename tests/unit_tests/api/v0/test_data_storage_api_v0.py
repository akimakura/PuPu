import copy
from http import HTTPStatus
from typing import Optional

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.v0.const import DATASTORAGE_URL, TENANT_NAME_URL, V0_PREFIX_URL
from src.db.data_storage import DataStorage
from src.db.dimension import Dimension
from src.db.measure import Measure
from src.db.model import Model
from src.main import API_PREFIX_URL
from src.models.data_storage import (
    DataStorage as DataStorageModel,
    DataStorageCreateRequest as DataStorageCreateRequestModel,
    DataStorageEditRequest as DataStorageEditRequestModel,
    DataStorageV0,
)
from src.models.model_import import ImportFromFileResponse
from src.service.data_storage import DataStorageService
from tests.unit_tests.conftest import mock_method_by_http_exception, mock_session_maker
from tests.unit_tests.fixtures.data_storage import (
    data_storage_model_create_list,
    data_storage_model_list,
    data_storage_model_update_list,
)
from tests.unit_tests.utils import clear_uncompared_fields

PREFIX_URL = API_PREFIX_URL + V0_PREFIX_URL + TENANT_NAME_URL + DATASTORAGE_URL


class TestDataStorageApiV0:

    @pytest.mark.parametrize(
        ("tenant_id", "model_name", "expected", "db_models"),
        [
            ("tenant1", "test_model1", [], []),
            (
                "tenant1",
                "test_model1",
                data_storage_model_list[:12],
                "data_storages",
            ),
            (None, "test_model1", [], []),
        ],
    )
    async def test_get_data_storage_list_by_model_name(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        tenant_id: Optional[str],
        model_name: str,
        expected: list[DataStorageModel],
        db_models: list[DataStorage],
        request: pytest.FixtureRequest,
        models: list[Model],
    ) -> None:
        if isinstance(db_models, str):
            db_models = request.getfixturevalue(db_models)
        mocked_session.add_all(models)
        await mocked_session.commit()
        mocked_session.add_all(db_models)
        await mocked_session.commit()
        mock_session_maker(monkeypatch, mocked_session)
        if not tenant_id:
            mock_method_by_http_exception(
                monkeypatch, DataStorageService, "get_data_storage_list_by_model_name", HTTPStatus.INTERNAL_SERVER_ERROR
            )
        expected = copy.deepcopy(expected)
        expected_list = [
            clear_uncompared_fields(
                DataStorageModel.model_dump(DataStorageV0.model_validate(i), mode="json", by_alias=True)
            )
            for i in expected
        ]
        expected_list.sort(key=lambda data_storage: data_storage["name"])
        url = PREFIX_URL + "/"
        url = url.replace("{modelName}", model_name)
        if tenant_id:
            url = url.replace("{tenantName}", tenant_id)
        else:
            url = url.replace("{tenantName}", "test")
        no_cached_data_storages_res = await async_client.get(url=url)
        if tenant_id:
            assert no_cached_data_storages_res.status_code == HTTPStatus.OK
            assert expected_list == clear_uncompared_fields(no_cached_data_storages_res.json())
            cached_data_storages_res = await async_client.get(url=url)
            assert clear_uncompared_fields(cached_data_storages_res.json()) == clear_uncompared_fields(
                no_cached_data_storages_res.json()
            )
        else:
            assert no_cached_data_storages_res.status_code == HTTPStatus.INTERNAL_SERVER_ERROR

    @pytest.mark.parametrize(
        ("tenant_id", "model_name", "dso_name", "expected", "expected_status"),
        [
            ("tenant1", "test_model1", "test222", None, HTTPStatus.NOT_FOUND),
            ("tenant1", "test_model1", "test_dso1", data_storage_model_list[0], HTTPStatus.OK),
            (None, "test_model1", "test_dso1", data_storage_model_list[0], HTTPStatus.INTERNAL_SERVER_ERROR),
        ],
    )
    async def test_get_data_storage_by_ds_name(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        data_storages: list[DataStorage],
        tenant_id: Optional[str],
        model_name: str,
        dso_name: str,
        expected: DataStorageModel,
        expected_status: HTTPStatus,
    ) -> None:
        mocked_session.add_all(data_storages)
        await mocked_session.commit()
        mock_session_maker(monkeypatch, mocked_session)
        if not tenant_id:
            mock_method_by_http_exception(
                monkeypatch, DataStorageService, "get_data_storage_by_name", HTTPStatus.INTERNAL_SERVER_ERROR
            )
        expected_dict = None
        if expected is not None:
            expected = copy.deepcopy(expected)
            expected_dict = clear_uncompared_fields(
                DataStorageModel.model_dump(
                    DataStorageV0.model_validate(expected),
                    mode="json",
                    by_alias=True,
                )
            )
        url = PREFIX_URL + f"/{dso_name}"
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
        ("tenant_id", "model_name", "data_storage_name", "expected_status"),
        [
            ("tenant1", "test_model1", "test_dso1123", HTTPStatus.NOT_FOUND),
            ("tenant1", "test_model1", "test_dso1", HTTPStatus.NO_CONTENT),
            (None, "test_model1", "test_dso1", HTTPStatus.INTERNAL_SERVER_ERROR),
        ],
    )
    async def test_delete_data_storage_by_name(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        data_storages: list[DataStorage],
        tenant_id: str,
        model_name: str,
        data_storage_name: str,
        expected_status: HTTPStatus,
    ) -> None:
        mocked_session.add_all(data_storages)
        await mocked_session.commit()
        mock_session_maker(monkeypatch, mocked_session)
        if not tenant_id:
            mock_method_by_http_exception(
                monkeypatch, DataStorageService, "delete_data_storage_by_name", HTTPStatus.INTERNAL_SERVER_ERROR
            )
        url = PREFIX_URL + f"/{data_storage_name}"
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
        ("tenant_id", "model_name", "data_storage", "expected"),
        [
            ("tenant1", "test_model1", data_storage_model_create_list[0], data_storage_model_list[0]),
            ("tenant1", "test_model1", data_storage_model_create_list[1], data_storage_model_list[11]),
            (None, "test_model1", data_storage_model_create_list[1], data_storage_model_list[11]),
        ],
    )
    async def test_create_data_storage(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        measures: list[Measure],
        dimensions: list[Dimension],
        models: list[Model],
        tenant_id: str,
        model_name: str,
        data_storage: DataStorageCreateRequestModel,
        expected: DataStorageModel,
    ) -> None:
        mocked_session.add_all(models)
        await mocked_session.commit()
        mocked_session.add_all(dimensions)
        await mocked_session.commit()
        mocked_session.add_all(measures)
        await mocked_session.commit()
        mock_session_maker(monkeypatch, mocked_session)
        async def mock_collect_views_for_model(*_: object, **__: object) -> list[int]:
            return []

        monkeypatch.setattr(DataStorageService, "collect_views_for_model", mock_collect_views_for_model)
        if not tenant_id:
            mock_method_by_http_exception(
                monkeypatch, DataStorageService, "create_data_storage_by_schema", HTTPStatus.INTERNAL_SERVER_ERROR
            )
        url = PREFIX_URL + "/"
        url = url.replace("{modelName}", model_name)
        if tenant_id:
            url = url.replace("{tenantName}", tenant_id)
        else:
            url = url.replace("{tenantName}", "test")
        data_storage_dict = data_storage.model_dump(exclude_unset=True, mode="json", by_alias=True)
        response = await async_client.post(url=url, json=data_storage_dict)
        if tenant_id:
            assert response.status_code == HTTPStatus.CREATED
            assert clear_uncompared_fields(response.json()) == clear_uncompared_fields(
                DataStorageV0.model_validate(expected).model_dump(mode="json", by_alias=True)
            )
            response = await async_client.get(url=url + data_storage_dict["name"])
            assert response.status_code == HTTPStatus.OK
        else:
            assert response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR

    @pytest.mark.parametrize(
        ("tenant_id", "model_name", "data_storage_name", "update_data_storage"),
        [
            ("tenant1", "test_model1", "test_dso1", data_storage_model_update_list[0]),
            ("tenant1", "test_model1", "test_dso2", data_storage_model_update_list[1]),
            (None, "test_model1", "test_dso2", data_storage_model_update_list[1]),
            ("tenant1", "test_model1", "unknown", data_storage_model_update_list[1]),
        ],
    )
    async def test_update_data_storage_by_name(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        measures: list[Measure],
        dimensions: list[Dimension],
        data_storages: list[DataStorage],
        models: list[Model],
        tenant_id: str,
        model_name: str,
        data_storage_name: str,
        update_data_storage: DataStorageEditRequestModel,
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
        async def mock_collect_views_for_model(*_: object, **__: object) -> list[int]:
            return []

        monkeypatch.setattr(DataStorageService, "collect_views_for_model", mock_collect_views_for_model)
        if not tenant_id:
            mock_method_by_http_exception(
                monkeypatch,
                DataStorageService,
                "get_updated_fields",
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )
        update_data_storage_dict = update_data_storage.model_dump(exclude_unset=True, mode="json", by_alias=True)
        url = PREFIX_URL + f"/{data_storage_name}"
        url = url.replace("{modelName}", model_name)
        if tenant_id:
            url = url.replace("{tenantName}", tenant_id)
        else:
            url = url.replace("{tenantName}", "test")
        original_model_dict = clear_uncompared_fields((await async_client.get(url=url)).json())
        response = await async_client.patch(url=url, json=update_data_storage_dict)
        if data_storage_name == "unknown":
            assert response.status_code == HTTPStatus.NOT_FOUND
            return
        if tenant_id:
            assert response.status_code == HTTPStatus.OK
            response_dict = clear_uncompared_fields(response.json())
            for composite_field in response_dict.get("fields"):
                composite_field.pop("sqlColumnType", None)
            for composite_field in original_model_dict.get("fields"):
                composite_field.pop("sqlColumnType", None)
            for field, value in update_data_storage_dict.items():
                assert response_dict[field] == value
                original_model_dict.pop(field)
            original_model_dict.pop("logDataStorageName", None)
            original_model_dict.pop("planningEnabled", None)
            log_data_storage_new = response_dict.pop("logDataStorageName", None)
            planning_new = response_dict.pop("planningEnabled", None)
            for field, value in original_model_dict.items():
                assert response_dict[field] == value
            if planning_new:
                assert log_data_storage_new == data_storage_name + "_logs"
        else:
            assert response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR

    @pytest.mark.parametrize(
        (
            "tenant_id",
            "model_name",
            "data_storage_file_path",
            "fields_file_path",
            "expected",
        ),
        [
            (
                "tenant1",
                "test_model1",
                "tests/unit_tests/fixtures/data_xlsx/test_import_data_storage1.xlsx",
                "tests/unit_tests/fixtures/data_xlsx/test_import_data_storage_fields1.xlsx",
                ImportFromFileResponse(
                    created=["test_dso_compination1", "test_dso_compination2"],
                ),
            ),
            (
                "tenant1",
                "test_model1",
                None,
                "tests/unit_tests/fixtures/data_xlsx/test_import_data_storage_fields2.xlsx",
                ImportFromFileResponse(
                    updated=["test_dso1", "test_dso2"],
                ),
            ),
        ],
    )
    async def test_create_model_data_storage(
        self,
        monkeypatch: pytest.MonkeyPatch,
        measures: list[Measure],
        dimensions: list[Dimension],
        models: list[Model],
        data_storages: list[DataStorage],
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        tenant_id: str,
        model_name: str,
        data_storage_file_path: Optional[str],
        fields_file_path: Optional[str],
        expected: ImportFromFileResponse,
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
        async def mock_collect_views_for_model(*_: object, **__: object) -> list[int]:
            return []

        monkeypatch.setattr(DataStorageService, "collect_views_for_model", mock_collect_views_for_model)
        url = PREFIX_URL + "/create/model"
        url = url.replace("{tenantName}", tenant_id)
        url = url.replace("{modelName}", model_name)
        datastorage = None
        files = {}
        if data_storage_file_path:
            with open(data_storage_file_path, "rb") as file:
                datastorage_binary = file.read()
            datastorage = (
                data_storage_file_path,
                datastorage_binary,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            files["dataStorages"] = datastorage
        if fields_file_path:
            with open(fields_file_path, "rb") as file:
                fields_binary = file.read()
            fields = (
                fields_file_path,
                fields_binary,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            files["fields"] = fields
        response = await async_client.post(url=url, files=files)
        resp_body = response.json()
        assert expected.model_dump(mode="json", by_alias=True) == resp_body
