from typing import AsyncGenerator

import pytest
from httpx import ASGITransport, AsyncClient

from src.main import create_app


@pytest.fixture
def test_app():
    """Фикстура для тестового приложения"""
    app = create_app()
    yield app


@pytest.fixture
async def async_client(test_app) -> AsyncGenerator[AsyncClient, None]:
    """
    Создание асинхронного HTTP-клиента для тестирования API.

    Returns:
        Генератор асинхронного HTTP-клиента.
    """
    async with AsyncClient(
        transport=ASGITransport(app=test_app),
        base_url="https://localhost:8000",
    ) as ac:
        setattr(ac, "app", test_app)
        yield ac
