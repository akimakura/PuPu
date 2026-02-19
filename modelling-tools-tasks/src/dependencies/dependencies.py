from contextlib import asynccontextmanager

from typing_extensions import AsyncGenerator

from src.config import settings
from src.integrations.modelling_tools_api.codegen import ApiClient as MTApiClient


async def get_mt_api_client() -> AsyncGenerator[MTApiClient, None]:
    async with MTApiClient(settings.MT_API_CONFIGURATION) as api_client:
        try:
            yield api_client
        finally:
            await api_client.close()


mt_api_client_context = asynccontextmanager(get_mt_api_client)
