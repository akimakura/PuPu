"""
Тут реализована вся логика работы сервиса.
Работа с БД инкапсулирована через DataRepository.
"""

from src.integrations.modelling_tools_api.codegen import (
    Database,
    V1Api,
)


class DatabaseService:
    """
    Сервис для работы с базами данных через API.

    Args:
        mt_api_v1_client (V1Api): Клиент для взаимодействия с MT API версии 1.
    """

    def __init__(self, mt_api_v1_client: V1Api):
        self.mt_api_v1_client = mt_api_v1_client

    async def get_database(self, tenant_id: str, model_name: str) -> Database:
        """
        Получение базы данных по имени модели.

        Args:
            tenant_id (str): Идентификатор арендатора (tenant).
            model_name (str): Имя модели, связанной с базой данных.

        Returns:
            Database: Объект, представляющий полученную базу данных.
        """
        model_response = await self.mt_api_v1_client.get_model_by_name(model_name, tenant_id)
        database_response = await self.mt_api_v1_client.get_database_by_name(model_response.database_name, tenant_id)
        return database_response
