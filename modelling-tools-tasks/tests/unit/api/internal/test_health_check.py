from http import HTTPStatus

from httpx import AsyncClient


async def test_health_check_success(async_client: AsyncClient, mock_metrics):
    response = await async_client.get("http://localhost:8000/api/internal/health_check")

    assert response.status_code == HTTPStatus.OK
    assert response.json() == "ok"
    mock_metrics.inc.assert_called_once()
