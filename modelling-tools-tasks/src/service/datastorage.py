"""
Тут реализована вся логика работы сервиса.
Работа с БД инкапсулирована через DataRepository.
"""

from py_common_lib.logger import EPMPYLogger

from src.config import settings
from src.integrations.modelling_tools_api.codegen import (
    DataStorage,
    DataStorageField,
    DbObject,
    InternalApi,
    ObjectStatus,
    ObjectType,
    RespObjectStatus,
    Result,
    V1Api,
)
from src.models.database import DatabaseTypeEnum
from src.models.database_object import DatabaseObjectGenerationResult
from src.repository.table import TableRepository
from src.service.utils import change_model_status, get_change_object_status

logger = EPMPYLogger(__name__)


class DataStorageService:
    """
    Сервис управления хранилищами данных (datastorages), позволяющий получать данные
    хранилищ, создавать, обновлять и изменять статус.

    Args:
        mt_api_v1_client (V1Api): Клиент API версии v1 для взаимодействия с моделью.
        mt_internal_api_client (InternalApi): Внутренний клиент API для работы с объектами модели.
        _table_repository (TableRepository | None): Репозиторий для работы с таблицами (может быть установлено позже).
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
        self._table_repository: TableRepository | None = None
        self._model_name: str | None = model_name
        self._tenant_id: str | None = tenant_id

    @property
    def table_repository(self) -> TableRepository:
        if self._table_repository is None:
            raise ValueError("table_repository is not set")
        return self._table_repository

    @table_repository.setter
    def table_repository(self, repository: TableRepository) -> None:
        self._table_repository = repository

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

    async def get_datastorage(self, name: str) -> DataStorage:
        """
        Асинхронный метод получения хранилища данных (data storage) по имени модели и имени хранилища.

        Args:
            name (str): Имя хранилища данных, которое нужно получить.

        Returns:
            DataStorage: Объект хранилища данных.
        """
        datastorage = await self.mt_api_v1_client.get_data_storage_by_model_name_and_data_storage_name(
            name, self.model_name, self.tenant_id, _request_timeout=settings.MT_API_TIMEOUT
        )
        return datastorage

    async def get_datastorages(self) -> list[DataStorage]:
        """
        Асинхронный метод получения списка всех хранилищ данных для текущей модели.

        Returns:
            List[DataStorage]: Список объектов хранилищ данных.
        """
        datastorages = await self.mt_api_v1_client.get_data_storages_by_model_name(
            self.model_name, self.tenant_id, _request_timeout=settings.MT_API_TIMEOUT
        )
        return datastorages

    def get_change_datastorage_status(
        self, datastorage_name: str, change_result: list[DatabaseObjectGenerationResult]
    ) -> list[ObjectStatus]:
        """
        Метод возвращает список статусов изменения объекта хранилища данных после выполнения операции.

        Args:
            datastorage_name (str): Название хранилища данных.
            change_result (List[DatabaseObjectGenerationResult]): Результат изменения объекта базы данных.

        Returns:
            List[ObjectStatus]: Статусы изменений объектов хранилища данных.
        """
        return get_change_object_status(self.model_name, ObjectType.DATA_STORAGE, datastorage_name, change_result)

    async def create_datastorage_in_db(
        self,
        datastorage: DataStorage,
        if_not_exists: bool = False,
        delete_if_failder: bool = False,
    ) -> list[ObjectStatus]:
        """
        Создаёт хранилище данных (таблицы базы данных), указанные в параметре `datastorage`.

        Args:
            datastorage (DataStorage): Объект типа DataStorage, содержащий описание структуры таблиц БД.
            if_not_exists (bool, optional): Если установлено значение True,
                                        таблицы будут созданы только в случае отсутствия существующих таблиц.
                                        По умолчанию False.
            delete_if_failder (bool, optional): Если задано True, то при неуспешном создании таблицы будут удалены

        Returns:
            list[ObjectStatus]: Список объектов ObjectStatus, отражающих результат операции создания каждой таблицы.
                            Каждый объект содержит статус изменения (успешность/ошибка).

        Raises:
            ValueError: В случае если атрибут `db_objects` объекта `datastorage` равен None.

        Примечание:
            Функция создает таблицы в базе данных через репозиторий таблиц (`table_repository`),
            обрабатывая исключения и формируя результаты изменений для каждой создаваемой таблицы.
        """
        if datastorage.db_objects is None:
            raise ValueError(
                f"DataStorage.db_objects is None for {self.tenant_id}/{self.model_name}/{datastorage.name}"
            )
        try:
            change_object_result = await self.table_repository.create_tables(
                datastorage.db_objects,
                datastorage.fields,
                not_exist=if_not_exists,
                delete_if_failder=delete_if_failder,
            )
        except Exception as exc:
            logger.exception("Error while creating datastorage")
            change_object_result = []
            for db_object in datastorage.db_objects:
                change_object_result.append(DatabaseObjectGenerationResult(table=db_object, error=str(exc)))
        change_status_request = self.get_change_datastorage_status(datastorage.name, change_object_result)
        return change_status_request

    async def update_datastorage_in_db(
        self,
        datastorage: DataStorage,
        enable_delete_column: bool = True,
        enable_delete_not_empty: bool = False,
    ) -> list[ObjectStatus]:
        """
        Обновляет существующее хранилище данных (таблицы базы данных), указанное в параметре `datastorage`.

        Args:
            datastorage (DataStorage): Объект типа DataStorage, содержащий описание обновляемой структуры таблиц БД.
            enable_delete_column (bool, optional): Флаг, указывающий возможность удаления столбцов при обновлении схемы.
                                                По умолчанию True.
            enable_delete_not_empty (bool, optional): Разрешение удаления непустых таблиц при изменении структуры.
                                                    По умолчанию False.

        Returns:
            list[ObjectStatus]: Список объектов ObjectStatus, отображающий результат операции обновления каждой таблицы.
                            Каждый объект включает статус изменения (успешность/ошибка).

        Raises:
            ValueError: Возникает, когда атрибут `db_objects` объекта `datastorage` равен None.

        Примечание:
            Функция обновляет существующие таблицы в базе данных через репозиторий таблиц (`table_repository`),
            обрабатывая возможные ошибки и создавая итоговые объекты статуса после завершения всех операций.
        """
        if datastorage.db_objects is None:
            raise ValueError(
                f"DataStorage.db_objects is None for {self.tenant_id}/{self.model_name}/{datastorage.name}"
            )
        try:
            change_object_result = await self.table_repository.update_tables(
                datastorage.db_objects, datastorage.fields, enable_delete_column, enable_delete_not_empty
            )
        except Exception as exc:
            logger.exception("Error while creating datastorage")
            change_object_result = []
            for db_object in datastorage.db_objects:
                change_object_result.append(DatabaseObjectGenerationResult(table=db_object, error=str(exc)))
        change_status_request = self.get_change_datastorage_status(datastorage.name, change_object_result)
        return change_status_request

    async def create_datastorage_by_name(
        self,
        datastorage_name: str,
        if_not_exists: bool = False,
        delete_if_failder: bool = False,
    ) -> list[ObjectStatus]:
        """
        Создаёт хранилище данных (DataStorage) по имени и создаёт сопутствующее лог-хранилище,
        если оно указано в конфигурации исходного DataStorage.

        Args:
            datastorage_name (str): Имя создаваемого хранилища данных.
            if_not_exists (bool): Если установлено `True`, создание пропускается, если объект уже существует.
            delete_if_failder (bool): Если операция завершилась неудачей, удаляет созданный объект (если был создан).

        Returns:
            list[ObjectStatus]: Список статусов объектов после операции создания основного и лог-хранилищ.
                            Статусы включают результат (`SUCCESS`/`FAILURE`) и сообщение об ошибке, если применимо.
        """
        datastorage = await self.get_datastorage(datastorage_name)
        log_datastorage = None
        log_datastorage_res: list[ObjectStatus] = []
        if datastorage.log_data_storage_name:
            log_datastorage = await self.get_datastorage(datastorage.log_data_storage_name)
            log_datastorage_res = await self.create_datastorage_in_db(log_datastorage, if_not_exists, delete_if_failder)
        datastorage_res = await self.create_datastorage_in_db(datastorage, if_not_exists, delete_if_failder)
        log_success = True
        for res in log_datastorage_res:
            if res.status != Result.SUCCESS and res.object_type == ObjectType.DATA_STORAGE:
                log_success = False
        if not log_success:
            for res in datastorage_res:
                if res.object_type == ObjectType.DATA_STORAGE:
                    res.status = Result.FAILURE
                    res.msg = "" if not res.msg else res.msg
                    res.msg = f"Log datastorage {datastorage.log_data_storage_name} is not created;" + res.msg
        result = log_datastorage_res
        result.extend(datastorage_res)
        return result

    async def create_datastorage_by_name_and_send_status(
        self,
        datastorage_name: str,
        if_not_exists: bool = False,
        delete_if_failder: bool = False,
    ) -> list[RespObjectStatus]:
        """
        Создает хранилище данных (DataStorage), отправляет статусы созданных объектов в API сервиса модели.

        Args:
            datastorage_name (str): Имя создаваемого хранилища данных.
            if_not_exists (bool): Флаг игнорирования существующей сущности (по умолчанию — создать, даже если уже есть).
            delete_if_failder (bool): Удалять ли объекты, созданные неудачно (только при условии успешной начальной записи).

        Returns:
            list[RespObjectStatus]: Ответ от сервера с состояниями созданных объектов после отправки статуса.
        """
        statuses = await self.create_datastorage_by_name(datastorage_name, if_not_exists, delete_if_failder)
        resp = await change_model_status(self.tenant_id, self.model_name, self.mt_internal_api_client, statuses)
        return resp

    async def update_datastorage_by_name(
        self,
        datastorage_name: str,
        enable_delete_column: bool = True,
        enable_delete_not_empty: bool = False,
    ) -> list[ObjectStatus]:
        """
        Обновляет хранилище данных (DataStorage) по заданному имени и обновляет соответствующее лог-хранилище,
        если такое имеется в настройках.

        Параметры:
            datastorage_name (str): Имя обновляемого хранилища данных.
            enable_delete_column (bool): Разрешить удаление столбцов таблицы при обновлении?
            enable_delete_not_empty (bool): Позволить пересоздание непустых таблиц?

        Возвращает:
            list[ObjectStatus]: Список статусов объектов после обновления.
                            Каждый элемент содержит состояние объекта и описание произошедшего события или ошибки.
        """
        datastorage = await self.get_datastorage(datastorage_name)
        log_datastorage = None
        log_datastorage_res: list[ObjectStatus] = []
        if datastorage.log_data_storage_name:
            log_datastorage = await self.get_datastorage(datastorage.log_data_storage_name)
            log_datastorage_res = await self.update_datastorage_in_db(
                log_datastorage, enable_delete_column, enable_delete_not_empty
            )
        datastorage_res = await self.update_datastorage_in_db(
            datastorage, enable_delete_column, enable_delete_not_empty
        )
        log_success = True
        for res in log_datastorage_res:
            if res.status != Result.SUCCESS and res.object_type == ObjectType.DATA_STORAGE:
                log_success = False
        if not log_success:
            for res in datastorage_res:
                if res.object_type == ObjectType.DATA_STORAGE:
                    res.status = Result.FAILURE
                    res.msg = "" if not res.msg else res.msg
                    res.msg = f"Log datastorage {datastorage.log_data_storage_name} is not updated;" + res.msg
        result = log_datastorage_res
        result.extend(datastorage_res)
        return result

    async def update_datastorage_by_name_and_send_status(
        self,
        datastorage_name: str,
        enable_delete_column: bool = True,
        enable_delete_not_empty: bool = False,
    ) -> list[RespObjectStatus]:
        """
        Обновляет хранилище данных (data storage) по имени и отправляет статус выполнения операции.

        Args:
            datastorage_name (str): Имя хранилища данных.
            enable_delete_column (bool): Флаг, разрешающий удаление столбцов (по умолчанию — True).
            enable_delete_not_empty (bool): Флаг, разрешающий удаление даже при наличии данных (по умолчанию — False).

        Returns:
            list[RespObjectStatus]: Список объектов статуса после обновления хранилища данных.
        """
        statuses = await self.update_datastorage_by_name(
            datastorage_name, enable_delete_column, enable_delete_not_empty
        )
        resp = await change_model_status(self.tenant_id, self.model_name, self.mt_internal_api_client, statuses)
        return resp

    async def delete_datastorage(
        self,
        datastorage_name: str,
        tables: list[DbObject],
        fields: list[DataStorageField],
        if_exists: bool = False,
        recreate_if_failed: bool = False,
        check_possible_delete: bool = True,
    ) -> list[ObjectStatus]:
        """
        Удаляет указанное хранилище данных вместе с таблицами и полями.

        Args:
            datastorage_name (str): Название удаляемого хранилища данных.
            tables (list[DbObject]): Список объектов таблиц для удаления.
            fields (list[DataStorageField]): Список полей данных.
            if_exists (bool): Если True, пропускается ошибка при отсутствии таблицы (по умолчанию — False).
            recreate_if_failed (bool): Попытка воссоздать таблицу при неудаче (по умолчанию — False).
            check_possible_delete (bool): Проверять возможность удаления перед выполнением (по умолчанию — True).

        Returns:
            list[ObjectStatus]: Статус изменения объекта хранилища данных.
        """
        try:
            change_object_result = await self.table_repository.delete_tables(
                tables, fields, if_exists, recreate_if_failed, check_possible_delete
            )
        except Exception as exc:
            logger.exception("Error while creating datastorage")
            change_object_result = []
            for table in tables:
                change_object_result.append(DatabaseObjectGenerationResult(table=table, error=str(exc)))
        change_status_request = self.get_change_datastorage_status(datastorage_name, change_object_result)
        return change_status_request

    async def recreate_datastorage_in_db(
        self, datastorage: DataStorage, сheck_possible_to_drop_tables: bool = True
    ) -> list[ObjectStatus]:
        """
        Пересоздает хранилище данных в базе данных.

        Args:
            datastorage (DataStorage): Объект хранилища данных для пересоздания.
            сheck_possible_to_drop_tables (bool): Проверять возможность удаления существующих таблиц перед восстановлением (по умолчанию — True).

        Raises:
            ValueError: Если `db_objects` отсутствует в структуре хранилища данных.

        Returns:
            list[ObjectStatus]: Результат изменений состояния хранилища данных.
        """
        if datastorage.db_objects is None:
            raise ValueError(
                f"DataStorage.db_objects is None for {self.tenant_id}/{self.model_name}/{datastorage.name}"
            )
        try:
            change_object_result = await self.table_repository.recreate_tables(
                datastorage.db_objects, datastorage.fields, сheck_possible_to_drop_tables
            )
        except Exception as exc:
            logger.exception("Error while recreating datastorage")
            change_object_result = []
            for db_object in datastorage.db_objects:
                change_object_result.append(DatabaseObjectGenerationResult(table=db_object, error=str(exc)))
        change_status_request = self.get_change_datastorage_status(datastorage.name, change_object_result)
        return change_status_request

    async def recreate_dictionary_in_db(self, datastorage: DataStorage) -> list[ObjectStatus]:
        """
        Пересоздает словарь (dictionary) хранилища данных в ClickHouse.

        Args:
            datastorage (DataStorage): Объект хранилища данных для пересоздания словаря.

        Raises:
            ValueError: Если объекты базы данных (`db_objects`) отсутствуют в хранилище данных.

        Returns:
            list[ObjectStatus]: Состояния объектов после восстановления словаря.
        """
        if self.table_repository.database.type != DatabaseTypeEnum.CLICKHOUSE:
            return []
        if datastorage.db_objects is None:
            raise ValueError(
                f"DataStorage.db_objects is None for {self.tenant_id}/{self.model_name}/{datastorage.name}"
            )
        change_object_result: list[DatabaseObjectGenerationResult] = []
        try:
            recreate_result = await self.table_repository.recreate_dictionary(
                datastorage.db_objects,
                datastorage.fields,
            )
            if recreate_result:
                change_object_result.append(recreate_result)
        except Exception as exc:
            logger.exception("Error while recreating datastorage")
            change_object_result = []
            for db_object in datastorage.db_objects:
                change_object_result.append(DatabaseObjectGenerationResult(table=db_object, error=str(exc)))
        change_status_request = self.get_change_datastorage_status(datastorage.name, change_object_result)
        return change_status_request
