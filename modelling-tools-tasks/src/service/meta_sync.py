from py_common_lib.logger import EPMPYLogger

from src.integrations.modelling_tools_api.codegen import (
    CompositeGet as Composite,
    DataStorage,
    Dimension,
    ModelStatus,
    ObjectStatus,
    RespObjectStatus,
    Result,
)
from src.models.composite import CompositeFieldRefObjectEnum
from src.models.datastorage import DataStorageEnum
from src.repository.table import TableClickhouseRepository
from src.service.composite.composite import CompositeService
from src.service.datastorage import DataStorageService
from src.service.dimension import DimensionService
from src.service.utils import change_model_status
from src.utils.validators import get_not_null_or_raise

logger = EPMPYLogger(__name__)


class MetaSyncService:
    """
    Сервис для синхронизации метаданных и содержимого базы данных в модели.

    Данный класс позволяет осуществлять синхронизацию между метаданными
    (информацией о структуре моделей, измерениях и хранилищах данных)
    и фактическими данными, хранящимися в БД.

    Args:
        datastorage_service (DataStorageService | None): сервис работы с хранилищем данных
        dimension_service (DimensionService | None): сервис работы с измерениями
        composite_service (CompositeService | None): сервис для работы с композитами
        model_name (str | None): имя модели, которую нужно синхронизировать
        tenant_id (str | None): идентификатор тенанта
    """

    def __init__(
        self,
        datastorage_service: DataStorageService | None = None,
        dimension_service: DimensionService | None = None,
        composite_service: CompositeService | None = None,
        model_name: str | None = None,
        tenant_id: str | None = None,
    ) -> None:
        self._model_name: str | None = model_name
        self._tenant_id = tenant_id
        self._composite_service = composite_service
        self._dimension_service = dimension_service
        self._datastorage_service = datastorage_service

    @property
    def composite_service(self) -> CompositeService:
        return get_not_null_or_raise(self._composite_service, log_attr_name="_composite_service")

    @composite_service.setter
    def composite_service(self, service: CompositeService) -> None:
        self._composite_service = service

    @property
    def model_name(self) -> str:
        return get_not_null_or_raise(self._model_name, log_attr_name="_model_name")

    @model_name.setter
    def model_name(self, name: str) -> None:
        self._model_name = name

    @property
    def tenant_id(self) -> str:
        return get_not_null_or_raise(self._tenant_id, log_attr_name="_tenant_id")

    @tenant_id.setter
    def tenant_id(self, name: str) -> None:
        self._tenant_id = name

    @property
    def dimension_service(self) -> DimensionService:
        return get_not_null_or_raise(self._dimension_service, log_attr_name="_dimension_service")

    @dimension_service.setter
    def dimension_service(self, service: DimensionService) -> None:
        self._dimension_service = service

    @property
    def datastorage_service(self) -> DataStorageService:
        return get_not_null_or_raise(self._datastorage_service, log_attr_name="_datastorage_service")

    @datastorage_service.setter
    def datastorage_service(self, service: DataStorageService) -> None:
        self._datastorage_service = service

    async def delete_composites(self, composites: list[Composite]) -> None:
        """
        Удаляет композитные объекты (composites), обновляя статус моделей до ожидающего состояния.

        Эта функция асинхронно устанавливает состояние ожидания для всех переданных композитных объектов,
        удаляя их из базы данных и меняя статус связанных моделей на 'PENDING'.

        Args:
            composites (list[Composite]): Список композитных объектов Composite для удаления
        """
        await self.composite_service.set_pending_status(composites)
        await self.composite_service.delete_composite_in_db(composites)
        for composite in composites:
            models: list[ModelStatus] = get_not_null_or_raise(
                composite.models, log_attr_name="models", log_obj_name=composite.name
            )
            for model in models:
                model.status = Result.PENDING

    def get_datastorages_to_update(self, datastorages: list[DataStorage]) -> list[DataStorage]:
        """
        Получение списка хранилищ данных (`DataStorage`), подлежащих обновлению.

        Метод выбирает объекты `DataStorage`, у которых имеются модели, ассоциированные с текущей моделью
        (`self.model_name`) и находящиеся в статусе FAILURE или PENDING.

        Args:
            datastorages (list[DataStorage]):
                Список объектов `DataStorage` для анализа.

        Returns:
            list[DataStorage]:
                Список хранилищ данных, которые нужно обновить.
        """
        datastorage_to_update: list[DataStorage] = []
        success_objects = 0
        pending_objects = 0
        failed_objects = 0
        total = 0
        for datastorage in datastorages:
            if datastorage.type in {
                DataStorageEnum.DIMENSION_ATTRIBUTES,
                DataStorageEnum.DIMENSION_TEXTS,
                DataStorageEnum.DIMENSION_VALUES,
            }:
                continue
            total += 1
            models: list[ModelStatus] = get_not_null_or_raise(
                datastorage.models, log_attr_name="models", log_obj_name=datastorage.name
            )
            for model in models:
                if model.name == self.model_name and model.status in {Result.FAILURE, Result.PENDING}:
                    datastorage_to_update.append(datastorage)

                    if model.status == Result.PENDING:
                        pending_objects += 1
                    else:
                        failed_objects += 1
                elif model.name == self.model_name:
                    success_objects += 1
        logger.info(
            "Datastorages (Not linked to dimensions) statistics for model %s. Success: %s, Pending: %s, Failed: %s, Total: %s",
            self.model_name,
            success_objects,
            pending_objects,
            failed_objects,
            total,
        )
        logger.info(
            "Datastorages to update for model %s: %s",
            self.model_name,
            [datastorage.name for datastorage in datastorage_to_update],
        )
        return datastorage_to_update

    def get_dimensions_to_update(self, dimensions: list[Dimension]) -> list[Dimension]:
        """
        Возвращает список измерений (dimensions), которые нужно обновить.

        Метод проверяет статус моделей внутри каждого измерения. Если хотя бы одна
        модель имеет имя совпадающее с заданным `self.model_name` и её результат —
        либо 'FAILURE', либо 'PENDING' — измерение добавляется в итоговый список обновляемых.

        Args:
            dimensions (list[Dimension]):
                Список объектов измерений (`Dimension`), подлежащих проверке.

        Returns:
            list[Dimension]:
                Подмножество исходного списка измерений, требующих обновления.
        """
        dimension_to_update: list[Dimension] = []
        success_objects = 0
        pending_objects = 0
        failed_objects = 0
        total = 0
        for dimension in dimensions:
            total += 1
            models: list[ModelStatus] = get_not_null_or_raise(
                dimension.models, log_attr_name="models", log_obj_name=dimension.name
            )
            for model in models:
                if model.name == self.model_name and model.status in {Result.FAILURE, Result.PENDING}:
                    dimension_to_update.append(dimension)
                    if model.status == Result.PENDING:
                        pending_objects += 1
                    else:
                        failed_objects += 1
                elif model.name == self.model_name:
                    success_objects += 1
        logger.info(
            "Dimensions statistics for model %s. Success: %s, Pending: %s, Failed: %s, Total: %s",
            self.model_name,
            success_objects,
            pending_objects,
            failed_objects,
            total,
        )
        logger.info(
            "Dimensions to update for model %s: %s",
            self.model_name,
            [dimension.name for dimension in dimension_to_update],
        )
        return dimension_to_update

    def get_composites_to_update(self, composites: list[Composite]) -> list[Composite]:
        """
        Возвращает список композитных объектов (Composite), подлежащих обновлению.

        Метод проверяет статус моделей внутри каждого Composite объекта и формирует
        итоговый список тех объектов, модели которых имеют состояние FAILURE или PENDING.
        После обработки всех объектов выводит статистику успешности/ожидания/сбоя
        обработанных моделей.

        Args:
            composites (list[Composite]): Список композитных объектов для проверки

        Returns:
            list[Composite]: Список композитных объектов, нуждающихся в обновлении
        """
        composite_to_update: list[Composite] = []
        success_objects = 0
        pending_objects = 0
        failed_objects = 0
        total = 0
        for composite in composites:
            total += 1
            models: list[ModelStatus] = get_not_null_or_raise(
                composite.models, log_attr_name="models", log_obj_name=composite.name
            )
            for model in models:
                if model.name == self.model_name and model.status in {Result.FAILURE, Result.PENDING}:
                    composite_to_update.append(composite)
                    if model.status == Result.PENDING:
                        pending_objects += 1
                    else:
                        failed_objects += 1
                else:
                    success_objects += 1
        logger.info(
            "Composites statistics for model %s. Success: %s, Pending: %s, Failed: %s, Total: %s",
            self.model_name,
            success_objects,
            pending_objects,
            failed_objects,
            total,
        )
        logger.info(
            "Composites to update for model %s: %s",
            self.model_name,
            [composite.name for composite in composite_to_update],
        )
        return composite_to_update

    async def update_composites(
        self, pending_composites: list[Composite], composites: list[Composite], force_update_composites: bool = True
    ) -> list[ObjectStatus]:
        """
        Обновляет композитные объекты модели согласно топологическому порядку зависимостей.

        Args:
            pending_composites (list[Composite]):
                Список ожидающих обновления композитных объектов.

            composites (list[Composite]):
                Полный список всех композитных объектов данной модели.

            force_update_composites (bool, optional):
                Флаг принудительного обновления всех композитных объектов независимо от состояния ожидания. По умолчанию — `True`.

        Returns:
            list[ObjectStatus]:
                Список статусов обновлённых композитных объектов после завершения операции.
        """
        logger.info("Start update composites for model %s", self.model_name)
        statuses: list[ObjectStatus] = []
        if force_update_composites:
            logger.info("Force update composites enabled for model %s. Update all composites", self.model_name)
            composites_to_update = composites
        else:
            composites_to_update = pending_composites
        if not composites_to_update:
            logger.info("No composites to update for model %s", self.model_name)
            return []
        oriented_composites_to_update = self.composite_service.graph_service.get_topological_order_composites(
            composites_to_update
        )[::-1]
        for composite_num, composite in enumerate(oriented_composites_to_update):
            logger.info(
                "Update composite %s/%s: %s.%s",
                composite_num + 1,
                len(oriented_composites_to_update),
                self.model_name,
                composite.name,
            )
            statuses.extend(await self.composite_service.update_composite_in_db(composite))
            logger.info(
                "Updated composite: %s.%s",
                self.model_name,
                composite.name,
            )
        return statuses

    def get_linked_composites_for_datastorages(
        self, datastorages: list[DataStorage], all_composites: list[Composite]
    ) -> list[Composite]:
        """
        Получение списка композитных объектов (composites), связанных с указанными хранилищами данных (datastorages).

        Args:
            datastorages (list[DataStorage]):
                Список объектов DataStorage, имена которых используются для поиска связей.

            all_composites (list[Composite]):
                Полный список всех композитных объектов Composite, среди которых нужно найти связи.

        Returns:
            list[Composite]:
                Список композитных объектов, имеющих связь с одним из указанных хранилищ данных.
        """
        datastorage_names = {datastorage.name for datastorage in datastorages}
        appended_composites = set()
        linked_composites = []
        for composite in all_composites:
            for datasource in composite.datasources:
                if (
                    datasource.type == CompositeFieldRefObjectEnum.DATASTORAGE
                    and datasource.name in datastorage_names
                    and composite.name not in appended_composites
                ):
                    appended_composites.add(composite.name)
                    linked_composites.append(composite)
        return linked_composites

    def get_datastorage_dimensions(
        self, dimensions: list[Dimension], datastorages: list[DataStorage]
    ) -> list[DataStorage]:
        """
        Возвращает список хранилищ данных (datastroage), связанных с указанными измерениями (dimensions).

        Функция принимает два списка: один содержит измерения (`dimensions`), другой — хранилища данных (`datastorages`).
        Для каждого измерения она получает набор всех соответствующих имен хранилищ данных через метод `get_all_related_datastorage_names_by_dimension`.
        Затем проверяет наличие каждого хранилища среди полученных имён и возвращает список подходящих хранилищ.

        Args:
            dimensions (list[Dimension]): Список измерений, для которых нужно найти соответствующие хранилища данных.
            datastorages (list[DataStorage]): Полный список доступных хранилищ данных.

        Returns:
            list[DataStorage]: Фильтрованный список хранилищ данных, связанных хотя бы с одним из указанных измерений.
        """
        datastorages_linked_dimensions: list[DataStorage] = []
        names_datastorages = set()
        for dimension in dimensions:
            names_datastorages |= set(self.dimension_service.get_all_related_datastorage_names_by_dimension(dimension))
        for datastorage in datastorages:
            if datastorage.name in names_datastorages:
                datastorages_linked_dimensions.append(datastorage)
        return datastorages_linked_dimensions

    async def try_delete_composites(
        self, composite_delete: bool, blocked_composites: list[Composite], all_composites: list[Composite]
    ) -> bool:
        """
        Асинхронная функция удаления композитных объектов (composite).

        Выполняет удаление всех композитных объектов, если задано соответствующее условие (`composite_delete`)
        либо если есть блокирующие композиты (`blocked_composites`), которые нужно удалить.

        Args:
            composite_delete (bool): Флаг, указывающий необходимость принудительного удаления композитных объектов.
            blocked_composites (List[Composite]): Список блокирующих композитных объектов.
            all_composites (List[Composite]): Полный список композитных объектов для обработки.

        Returns:
            bool: Возвращает `True`, если удаление произошло вследствие наличия заблокированных объектов
                при отсутствии явной команды на удаление (`composite_delete=False`),
                иначе возвращает `False`.
        """
        if composite_delete or blocked_composites:
            await self.delete_composites(all_composites)
            if blocked_composites and not composite_delete:
                return True
        return False

    async def get_ds_and_dim_to_update(
        self,
        datastorage_update: bool,
        datastorage_create: bool,
        datastorages: list[DataStorage],
        dimensions: list[Dimension],
        composites: list[Composite],
        force_datastorage_update: bool = False,
        force_dimension_update: bool = False,
    ) -> tuple[list[DataStorage], list[Dimension], list[Composite]]:
        """
        Получение списка хранилищ данных (`datastroage`), измерений (`dimensions`) и композитных объектов (`composites`),
        подлежащих обновлению согласно переданным параметрам.

        Args:
            datastorage_update (bool): Флаг включения обновления хранилищ данных.
            datastorage_create (bool): Флаг включения пересоздания хранилищ данных.
            datastorages (List[DataStorage]): Список всех текущих хранилищ данных.
            dimensions (List[Dimension]): Список всех текущих измерений.
            composites (List[Composite]): Список всех композитных объектов.
            force_datastorage_update (bool, optional): Принудительное обновление всех хранилищ данных. По умолчанию False.
            force_dimension_update (bool, optional): Принудительное обновление всех измерений. По умолчанию False.

        Returns:
            Tuple[List[DataStorage], List[Dimension], List[Composite]]:
                кортеж списков, содержащий соответственно:
                1. список хранилищ данных, подлежащих обновлению;
                2. список измерений, подлежащих обновлению;
                3. список блокирующих композитных объектов.
        """
        datastorage_to_update: list[DataStorage] = []
        dimensions_datastorages_to_update: list[DataStorage] = []
        dimension_to_update: list[Dimension] = []
        blocked_composites: list[Composite] = []
        if datastorage_update or datastorage_create:
            if force_datastorage_update:
                for datastorage in datastorages:
                    if datastorage.type not in {
                        DataStorageEnum.DIMENSION_ATTRIBUTES,
                        DataStorageEnum.DIMENSION_TEXTS,
                        DataStorageEnum.DIMENSION_VALUES,
                    }:
                        datastorage_to_update.append(datastorage)
                logger.info(
                    "Force update datastorages enabled for model %s. Update all datastorages (not linked to dimensions). Total datastorages: %s",
                    self.model_name,
                    len(datastorage_to_update),
                )
            else:
                datastorage_to_update.extend(self.get_datastorages_to_update(datastorages))
            if force_dimension_update:
                logger.info(
                    "Force update dimensions enabled for model %s. Update all dimensions and datastorages (linked to dimensions). Total dimensions: %s",
                    self.model_name,
                    len(dimensions),
                )
                dimension_to_update = dimensions
            else:
                dimension_to_update.extend(self.get_dimensions_to_update(dimensions))
            dimensions_datastorages_to_update = self.get_datastorage_dimensions(dimension_to_update, datastorages)
        blocked_composites.extend(
            self.get_linked_composites_for_datastorages(
                dimensions_datastorages_to_update + datastorage_to_update, composites
            )
        )
        return datastorage_to_update, dimension_to_update, blocked_composites

    async def update_datastorages(
        self,
        datastorages: list[DataStorage],
        with_delete_columns: bool,
        with_delete_not_empty: bool,
        recreate: bool = False,
    ) -> list[ObjectStatus]:
        """
        Обновляет хранилища данных (data storage).

        Args:
            datastorages (list[DataStorage]): Список объектов DataStorage, подлежащих обновлению.
            with_delete_columns (bool): Флаг разрешения удаления столбцов во время обновления схемы таблиц.
            with_delete_not_empty (bool): Флаг разрешения удаление непустых таблиц.
            recreate (bool): Если True — пересоздавать таблицы заново, иначе обновлять существующие структуры.
                По умолчанию False.

        Returns:
            list[ObjectStatus]: Список статусов выполнения операций обновления каждого хранилища.
        """
        logger.info("Start update datastorages for model %s", self.model_name)
        logger.info("Delete columns mode for model %s: %s", self.model_name, with_delete_columns)
        logger.info("Delete not empty mode for model %s: %s", self.model_name, with_delete_not_empty)
        logger.info("Recreate datastorage mode for model %s: %s", self.model_name, recreate)
        statuses: list[ObjectStatus] = []
        len_datastorages = len(datastorages)
        if not datastorages:
            logger.info("No datastorages to update for model %s", self.model_name)
            return []
        for datastorage_num, datastorage in enumerate(datastorages):
            logger.info(
                "Update datastorage %s/%s: %s.%s",
                datastorage_num + 1,
                len_datastorages,
                self.model_name,
                datastorage.name,
            )
            if not recreate:
                statuses.extend(
                    await self.datastorage_service.update_datastorage_in_db(
                        datastorage,
                        enable_delete_column=with_delete_columns,
                        enable_delete_not_empty=with_delete_not_empty,
                    )
                )
            else:
                for datastorage_num, datastorage in enumerate(datastorages):
                    statuses.extend(
                        await self.datastorage_service.recreate_datastorage_in_db(
                            datastorage,
                            сheck_possible_to_drop_tables=not with_delete_not_empty,
                        )
                    )
            logger.info(
                "Update datastorage: %s.%s",
                self.model_name,
                datastorage.name,
            )
        return statuses

    async def update_dimensions(
        self,
        dimensions: list[Dimension],
        datastorages: list[DataStorage],
        with_delete_columns: bool,
        with_delete_not_empty: bool,
        recreate: bool = False,
    ) -> list[ObjectStatus]:
        """
        Обновляет измерения модели и соответствующие хранилища данных.

        Args:
            dimensions (list[Dimension]):
                Список объектов Dimension, представляющих обновляемые измерения.

            datastorages (list[DataStorage]):
                Список объектов DataStorage — хранилищ данных, связанных с моделью.

            with_delete_columns (bool):
                Флаг, указывающий, нужно ли удалять столбцы в таблицах базы данных при обновлении.

            with_delete_not_empty (bool):
                Флаг, указывающий, разрешено ли удаление непустых таблиц.

            recreate (bool, optional):
                Если True, хранилища данных будут полностью пересоздаваться. По умолчанию False.

        Returns:
            list[ObjectStatus]:
                Статус обновления каждой размерности после обработки всех хранилищ данных.
        """
        logger.info("Start update dimensions for model %s", self.model_name)
        logger.info("Delete columns mode for model %s: %s", self.model_name, with_delete_columns)
        logger.info("Delete not empty mode for model %s: %s", self.model_name, with_delete_not_empty)
        logger.info("Recreate datastorage mode for model %s: %s", self.model_name, recreate)
        _datastorage_dict = {datastorage.name: datastorage for datastorage in datastorages}
        total_datastorages = 0
        for dimension in dimensions:
            datastorages_names = self.dimension_service.get_all_related_datastorage_names_by_dimension(dimension)
            total_datastorages += len(datastorages_names)
        current_datastorage_num = 0
        dimensions_status: list[ObjectStatus] = []
        len_dimensions = len(dimensions)
        if not dimensions:
            logger.info("No dimensions to update for model %s", self.model_name)
            return []
        for dimension_num, dimension in enumerate(dimensions):
            logger.info(
                "Update dimension %s/%s: %s.%s", dimension_num + 1, len_dimensions, self.model_name, dimension.name
            )
            datastorages_status: list[ObjectStatus] = []
            datastorages_names = self.dimension_service.get_all_related_datastorage_names_by_dimension(dimension)
            if dimension.dimension_ref or dimension.is_virtual:
                logger.info(
                    "Is virtual dimension %s.%s. Skip %s datastorages.",
                    self.model_name,
                    dimension.name,
                    len(datastorages_names),
                )
                current_datastorage_num += len(datastorages_names)
            for datastorage_name in datastorages_names:
                current_datastorage_num += 1
                datastorage: DataStorage = get_not_null_or_raise(
                    _datastorage_dict.get(datastorage_name),
                    log_obj_name=f"{self.tenant_id}/{self.model_name}/{datastorage_name}",
                )
                logger.info(
                    "Update datastorage %s/%s: %s.%s (for dimension %s.%s)",
                    current_datastorage_num,
                    total_datastorages,
                    self.model_name,
                    datastorage.name,
                    self.model_name,
                    dimension.name,
                )
                if recreate and datastorage:
                    ds_statuses = await self.datastorage_service.recreate_datastorage_in_db(
                        datastorage,
                        сheck_possible_to_drop_tables=not with_delete_not_empty,
                    )
                    datastorages_status.extend(ds_statuses)
                elif datastorage:
                    ds_statuses = await self.datastorage_service.update_datastorage_in_db(
                        datastorage,
                        with_delete_columns,
                        with_delete_not_empty,
                    )
                    datastorages_status.extend(ds_statuses)
                logger.info(
                    "Updated datastorage: %s.%s (for dimension %s.%s)",
                    self.model_name,
                    datastorage.name,
                    self.model_name,
                    dimension.name,
                )
            logger.info(
                "Updated dimension: %s.%s",
                self.model_name,
                dimension.name,
            )
            dimensions_status.extend(
                self.dimension_service.get_dimension_status_by_datastorage_status(dimension.name, datastorages_status)
            )
        return dimensions_status

    async def recreate_dictionary(
        self, datastorages: list[DataStorage], dimensions: list[Dimension]
    ) -> list[ObjectStatus]:
        """
        Пересоздает словарь (dictionary) для указанных хранилищ данных (`DataStorage`) и измерений (`Dimension`).

        Метод последовательно обрабатывает каждое измерение, пересоздавая словарь для всех связанных хранилищ данных.
        Если используемое хранилище таблиц является экземпляром `TableClickhouseRepository`, метод проходит по списку измерений и создает соответствующие записи в ClickHouse-словаре.
        В конце метода выполняется дополнительная проверка оставшихся хранилищ данных, для которых еще не был воссоздан словарь.

        Args:
            datastorages (list[DataStorage]):
                Список объектов типа DataStorage — представляет собой набор хранилищ данных, используемых приложением.

            dimensions (list[Dimension]):
                Список объектов типа Dimension — содержит измерения, необходимые для реконструкции словаря.

        Returns:
            list[ObjectStatus]: список статусов выполнения операций обновления словарей для каждого хранилища данных.
        """
        if not isinstance(self.datastorage_service.table_repository, TableClickhouseRepository):
            return []
        recreated_dictionary_datastorage = set()
        _datastorage_dict = {datastorage.name: datastorage for datastorage in datastorages}
        statuses: list[ObjectStatus] = []
        len_dimensions = len(dimensions)
        for dimension_num, dimension in enumerate(dimensions):
            logger.info(
                "Recreate dictionary for dimension %s/%s: %s.%s",
                dimension_num + 1,
                len_dimensions,
                self.model_name,
                dimension.name,
            )
            datastorages_status: list[ObjectStatus] = []
            datastorages_names = self.dimension_service.get_all_related_datastorage_names_by_dimension(dimension)
            for datastorage_name in datastorages_names:
                logger.info(
                    "Recreate dictionary for dimension %s/%s: %s",
                    dimension_num + 1,
                    len_dimensions,
                    self.model_name,
                    dimension.name,
                )
                datastorage = _datastorage_dict.get(datastorage_name)
                if datastorage_name not in recreated_dictionary_datastorage and datastorage:
                    recreated_dictionary_datastorage.add(datastorage.name)
                    datastorages_status.extend(await self.datastorage_service.recreate_dictionary_in_db(datastorage))
            statuses.extend(
                self.dimension_service.get_dimension_status_by_datastorage_status(dimension.name, datastorages_status)
            )
        for datastorage in datastorages:
            if datastorage.name not in recreated_dictionary_datastorage:
                recreated_dictionary_datastorage.add(datastorage.name)
                statuses.extend(await self.datastorage_service.recreate_dictionary_in_db(datastorage))
        return statuses

    async def sync_meta(
        self,
        composite_delete_flag: bool = False,
        composite_create_flag: bool = False,
        datastorage_update_flag: bool = False,
        datastorage_create_flag: bool = False,
        with_delete_columns_flag: bool = False,
        with_delete_not_empty_flag: bool = False,
        recreate_dictionry_flag: bool = False,
        force_composites: bool = False,
        force_datastorages: bool = False,
        force_dimensions: bool = False,
    ) -> list[RespObjectStatus]:
        """
        Синхронизирует метаданные модели, обновляя хранилища данных (datastorages), измерения (dimensions),
        композитные объекты (composites).

        Args:
            composite_delete_flag (bool): Флаг удаления композитных объектов (по умолчанию False).
            composite_create_flag (bool): Флаг пересоздания композитных объектов (по умолчанию False).
            datastorage_update_flag (bool): Флаг обновления существующих хранилищ данных (по умолчанию False).
            datastorage_create_flag (bool): Флаг пересоздания хранилищ данных (по умолчанию False).
            with_delete_columns_flag (bool): Флаг разрешает удаление столбцов при обновлении хранилищ данных (по умолчанию False).
            with_delete_not_empty_flag (bool): Флаг разрешает удаление непустых таблиц при обновлении хранилищ данных (по умолчанию False).
            recreate_dictionry_flag (bool): Флаг включения пересоздания словаря (по умолчанию False).
            force_composites (bool): Принудительное обновление композитных объектов независимо от состояния (по умолчанию False).
            force_datastorages (bool): Принудительное обновление хранилищ данных независимо от состояния (по умолчанию False).
            force_dimensions (bool): Принудительное обновление измерений независимо от состояния (по умолчанию False).

        Returns:
            list[RespObjectStatus]: Список статусов изменения объектов после синхронизации.
        """
        statuses: list[ObjectStatus] = []
        blocked_composites: list[Composite] = []
        composites = await self.composite_service.get_composites()
        dimensions = await self.dimension_service.get_dimensions()
        datastorages = await self.datastorage_service.get_datastorages()
        datastorage_to_update, dimension_to_update, blocked_composites = await self.get_ds_and_dim_to_update(
            datastorage_update_flag,
            datastorage_create_flag,
            datastorages,
            dimensions,
            composites,
            force_datastorages,
            force_dimensions,
        )
        composites_to_update = self.get_composites_to_update(composites)
        recreate_composite_flag = await self.try_delete_composites(
            bool(composites_to_update) or force_composites or composite_delete_flag,
            blocked_composites,
            composites,
        )
        statuses.extend(
            await self.update_datastorages(
                datastorage_to_update, with_delete_columns_flag, with_delete_not_empty_flag, datastorage_create_flag
            )
        )
        statuses.extend(
            await self.update_dimensions(
                dimension_to_update,
                datastorages,
                with_delete_columns_flag,
                with_delete_not_empty_flag,
                datastorage_create_flag,
            )
        )
        composite_statuses = (
            await self.update_composites(composites_to_update, composites)
            if recreate_composite_flag or composite_create_flag or composites_to_update
            else []
        )
        statuses.extend(composite_statuses)
        resp: list[RespObjectStatus] = []
        if statuses:
            resp.extend(
                await change_model_status(
                    self.tenant_id, self.model_name, self.datastorage_service.mt_internal_api_client, statuses
                )
            )
        recreate_dictionary_statuses = (
            await self.recreate_dictionary(datastorages, dimensions) if recreate_dictionry_flag else []
        )
        if recreate_dictionary_statuses:
            resp.extend(
                await change_model_status(
                    self.tenant_id,
                    self.model_name,
                    self.datastorage_service.mt_internal_api_client,
                    recreate_dictionary_statuses,
                )
            )
        return resp
