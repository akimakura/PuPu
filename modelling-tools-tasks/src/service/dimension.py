"""
Тут реализована вся логика работы сервиса.
Работа с БД инкапсулирована через DataRepository.
"""

from py_common_lib.logger import EPMPYLogger

from src.config import settings
from src.integrations.modelling_tools_api.codegen import (
    Dimension,
    InternalApi,
    ObjectStatus,
    ObjectType,
    RespObjectStatus,
    Result,
    V1Api,
)

logger = EPMPYLogger(__name__)


class DimensionService:
    """
    Сервис управления измерениями (dimensions), позволяющий получать данные
    хранилищ и изменять статус измерений на основе статуса хранилища данных.

    Args:
        mt_api_v1_client (V1Api): Клиент API версии v1 для взаимодействия с моделью.
        mt_internal_api_client (InternalApi): Внутренний клиент API для работы с объектами модели.
        _model_name (str | None): Имя текущей модели (может быть установлено позже).
        _tenant_id (str | None): Идентификатор тенанта (также может быть установлен позже).
    """

    def __init__(
        self,
        mt_api_v1_client: V1Api,
        mt_internal_api_client: InternalApi,
        model_name: str | None = None,
        tenant_id: str | None = None,
    ) -> None:
        self.mt_api_v1_client = mt_api_v1_client
        self.mt_internal_api_client = mt_internal_api_client
        self._model_name = model_name
        self._tenant_id = tenant_id

    @property
    def model_name(self) -> str:
        if self._model_name is None:
            raise ValueError("model_name is not set")
        return self._model_name

    @model_name.setter
    def model_name(self, name: str) -> None:
        self._model_name = name

    @property
    def tenant_id(self) -> str:
        if self._tenant_id is None:
            raise ValueError("tenant_id is not set")
        return self._tenant_id

    @tenant_id.setter
    def tenant_id(self, tenant: str) -> None:
        self._tenant_id = tenant

    async def get_all_related_datastorage_names_by_dimension_name(self, dimension_name: str) -> list[str]:
        """
        Возвращает список связанных хранилищ данных для измерения по имени.

        Args:
            dimension_name (str): Название измерения.

        Returns:
            List[str]: Список названий хранилищ данных, относящихся к данному измерению.
        """
        dimension = await self.get_dimension(dimension_name)
        if not dimension.is_virtual and not dimension.dimension_ref:
            return [
                name
                for name in [dimension.values_table, dimension.attributes_table, dimension.text_table]
                if name is not None
            ]
        return []

    def get_all_related_datastorage_names_by_dimension(self, dimension: Dimension) -> list[str]:
        """
        Возвращает список связанных хранилищ данных для конкретного объекта измерения.

        Args:
            dimension (Dimension): Объект измерения.

        Returns:
            List[str]: Список названий хранилищ данных, относящихся к данному измерению.
        """
        if not dimension.is_virtual and not dimension.dimension_ref:
            return [
                name
                for name in [dimension.values_table, dimension.attributes_table, dimension.text_table]
                if name is not None
            ]
        return []

    async def get_dimension(self, name: str) -> Dimension:
        """
        Получает описание измерения по указанному имени.

        Args:
            name (str): Название измерения.

        Returns:
            Dimension: Объект измерения.
        """
        dimension = await self.mt_api_v1_client.get_dimension_by_model_name_and_dimension_name(
            dimension_name=name,
            model_name=self.model_name,
            tenant_name=self.tenant_id,
            _request_timeout=settings.MT_API_TIMEOUT,
        )
        return dimension

    async def get_dimensions(self) -> list[Dimension]:
        """
        Получает список всех измерений текущей модели.

        Returns:
            List[Dimension]: Список объектов измерений.
        """
        dimensions = await self.mt_api_v1_client.get_dimensions_by_model_name(
            model_name=self.model_name,
            tenant_name=self.tenant_id,
            _request_timeout=settings.MT_API_TIMEOUT,
        )
        return dimensions

    def get_dimension_status_by_datastorage_status(
        self, dimension_name: str, datastorage_statuses: list[ObjectStatus]
    ) -> list[ObjectStatus]:
        """
        Формирует статусы измерения на основании состояния хранилищ данных.

        Args:
            dimension_name (str): Название измерения.
            datastorage_statuses (List[ObjectStatus]): Статусы хранилищ данных.

        Returns:
            List[ObjectStatus]: Обновлённый список статусов с учётом нового статуса измерения.
        """
        statuses: list[ObjectStatus] = []
        errors = []
        for datastorage_status in datastorage_statuses:
            if (
                datastorage_status.object_type == ObjectType.DATA_STORAGE
                and datastorage_status.status != Result.SUCCESS
            ):
                errors.append(str(datastorage_status.msg))
        statuses.extend(datastorage_statuses)
        statuses.append(
            ObjectStatus.model_validate({
                "schemaName": None,
                "objectName": dimension_name,
                "modelName": self.model_name,
                "objectType": ObjectType.DIMENSION,
                "status": Result.SUCCESS if not errors else Result.FAILURE,
                "msg": None if not errors else "; ".join(errors),
            })
        )
        return statuses

    async def change_dimension_status_by_datastorage_status(
        self, dimension_name: str, datastorage_statuses: list[ObjectStatus]
    ) -> list[RespObjectStatus]:
        """
        Изменяет статус измерения исходя из состояния хранилищ данных.

        Args:
            dimension_name (str): Название измерения.
            datastorage_statuses (List[ObjectStatus]): Текущие статусы хранилищ данных.

        Returns:
            List[RespObjectStatus]: Результат изменений статусов измерений.
        """
        statuses = self.get_dimension_status_by_datastorage_status(dimension_name, datastorage_statuses)
        logger.info("Dimension change statuses: %s", [status.model_dump(mode="json") for status in statuses])
        result = await self.mt_internal_api_client.change_object_status_in_model(
            tenant_name=self.tenant_id, object_status=statuses, _request_timeout=settings.MT_API_TIMEOUT
        )
        return result
