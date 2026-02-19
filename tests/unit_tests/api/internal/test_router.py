from typing import Any

import pytest
from fastapi import FastAPI
from httpx import AsyncClient

from src.api.internal.router import PermissionEnum, get_permissions_service, http_bearer
from tests.unit_tests.conftest import MOCK_TOKEN


class MockAuth:
    credentials = MOCK_TOKEN
    scheme = "Bearer"


class MockService:
    async def get_permissions(self, token: str) -> list[PermissionEnum]:
        return [PermissionEnum.COMPOSITE_CREATE]


def override_bearer() -> Any:
    return MockAuth()


def mock_get_service() -> Any:
    return MockService()


class TestInternal:

    async def test_get_semantic_permissions(
        self, monkeypatch: pytest.MonkeyPatch, async_client: AsyncClient, fastapi_app: FastAPI
    ) -> None:
        fastapi_app.dependency_overrides[http_bearer] = override_bearer
        fastapi_app.dependency_overrides[get_permissions_service] = mock_get_service
        monkeypatch.setattr("src.api.internal.router.http_bearer", override_bearer)
        monkeypatch.setattr("src.api.internal.router.get_permissions_service", mock_get_service)
        url = "/api/internal/semanticPermissions"
        response = await async_client.get(url=url, headers={"Authorization": f"Bearer {MOCK_TOKEN}"})
        assert response.json() == [PermissionEnum.COMPOSITE_CREATE]
