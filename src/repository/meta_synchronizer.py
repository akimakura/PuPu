"""Создание модели в бд из текущего метаописания."""

from typing import Optional, Type

from py_common_lib.logger import EPMPYLogger
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.composite import Composite
from src.db.data_storage import DataStorage
from src.db.model import Model
from src.models.data_storage import DataStorageEnum
from src.models.meta_synchronizer import GeneratorResult, MetaSynchronizerResponse
from src.models.model import Model as ModelModel
from src.models.request_params import Pagination, SortDirectionEnum
from src.repository.composite import CompositeRepository
from src.repository.data_storage import DataStorageRepository
from src.repository.generators.base_generator import GeneratorRepository
from src.repository.generators.clickhouse_generator import GeneratorClickhouseRepository
from src.repository.generators.utils import get_generator
from src.repository.model import ModelRepository
from src.repository.utils import get_filtred_database_object_by_data_storage

logger = EPMPYLogger(__name__)


class MetaSynchronizerRepository:  # noqa

    def __init__(
        self,
        session: AsyncSession,
        model_repository: ModelRepository,
        composite_repository: CompositeRepository,
        datastorage_repository: DataStorageRepository,
    ) -> None:
        self.session = session
        self.model_repository = model_repository
        self.composite_repository = composite_repository
        self.datastorage_repository = datastorage_repository

    async def _recreate_data_storage_and_append_to_result_list(
        self,
        result: list,
        generator: Type[GeneratorRepository],
        data_storage: DataStorage,
        model: Model,
        сheck_possible_to_drop_data_storage: bool = False,
        fall_if_exception: bool = False,
    ) -> None:
        try:
            await generator.recreate_data_storage(data_storage, model, сheck_possible_to_drop_data_storage)
            result.append(
                MetaSynchronizerResponse(
                    tenant_id=data_storage.tenant_id,
                    model=model.name,
                    object_name=data_storage.name,
                    msg=None,
                    result=GeneratorResult.SUCCESS,
                )
            )
        except Exception as exc:  # noqa
            logger.exception("Error creating datastorage.")
            result.append(
                MetaSynchronizerResponse(
                    tenant_id=data_storage.tenant_id,
                    model=model.name,
                    object_name=data_storage.name,
                    msg=str(exc),
                    result=GeneratorResult.FAILURE,
                )
            )
            if fall_if_exception:
                raise Exception(str(exc))

    async def _create_ignore_message_and_append_to_result_list(
        self,
        result: list,
        data_storage: DataStorage,
        model: Model,
        comment: Optional[str] = None,
    ) -> None:
        db_objects = get_filtred_database_object_by_data_storage(data_storage, model.name)
        db_objects_strs = [f"{db_object.schema_name}.{db_object.name}" for db_object in db_objects]
        result.append(
            MetaSynchronizerResponse(
                tenant_id=data_storage.tenant_id,
                model=model.name,
                object_name=data_storage.name,
                msg=comment,
                result=GeneratorResult.IGNORED,
            )
        )
        logger.info(
            "Datastorage ignored (%s): %s (%s)!",
            comment,
            data_storage.name,
            db_objects_strs,
        )

    async def _recreate_composite(
        self,
        generator: Type[GeneratorRepository],
        composite: Composite,
        model: Model,
        generate_on_db: bool = True,
        replace: bool = True,
    ) -> MetaSynchronizerResponse:
        """Пересоздание View для Composite."""
        sql_expression = await self.composite_repository.generate_composite_sql_expression_by_composite(
            composite, model
        )
        if generate_on_db:
            _ = await generator.create_composite(composite, model, sql_expression, replace=replace)
        comment = None
        result = GeneratorResult.SUCCESS
        return MetaSynchronizerResponse(
            tenant_id=composite.tenant_id,
            model=model.name,
            object_name=composite.name,
            msg=comment,
            result=result,
        )

    async def _delete_composite(
        self,
        generator: Type[GeneratorRepository],
        composite: Composite,
        model: Model,
    ) -> MetaSynchronizerResponse:
        """Пересоздание View для Composite."""
        db_objects = get_filtred_database_object_by_data_storage(composite, model.name)
        _ = await generator.delete_composite(composite, model, db_objects)
        comment = None
        result = GeneratorResult.SUCCESS
        return MetaSynchronizerResponse(
            tenant_id=composite.tenant_id,
            model=model.name,
            object_name=composite.name,
            msg=comment,
            result=result,
        )

    async def _check_possible_to_drop_model(
        self, generator: Type[GeneratorRepository], data_storages: list[DataStorage], ignore: list[str], model: Model
    ) -> tuple[list[MetaSynchronizerResponse], bool]:
        """Проверка на пустоту таблицы, которая привязана к модели"""
        result = []
        possible_drop = True
        for data_storage in data_storages:
            if data_storage.name in ignore:
                comment = None
                result_status = GeneratorResult.IGNORED
            elif not await generator.is_possible_to_drop_data_storage(data_storage, model):
                comment = "Not empty"
                result_status = GeneratorResult.FAILURE
                possible_drop = False
            else:
                comment = None
                result_status = GeneratorResult.SUCCESS
            result.append(
                MetaSynchronizerResponse(
                    tenant_id=data_storage.tenant_id,
                    model=model.name,
                    object_name=data_storage.name,
                    msg=comment,
                    result=result_status,
                )
            )
        return result, possible_drop

    async def create_data_storages_in_database_from_meta(
        self,
        tenant_id: str,
        model_name: str,
        ignore: list[str],
        check_possible_to_drop_model: bool = True,
        сheck_possible_to_drop_data_storage: bool = True,
        fall_if_exception: bool = False,
    ) -> list[MetaSynchronizerResponse]:
        """
        Обновить все таблицы в базе данных, которые привязаны к модели.
        Создать или пересоздать таблицы (если они не пустые).
        Args:
            check_possible_to_drop_model (bool): Флаг запрещает пересоздавать все таблицы,
            ignore (list[str]): Список dso, пересоздание которых нужно игнорировать.
        если хотя бы одна из таблиц непустая.
        """
        model = await self.model_repository.get_model_orm_by_session_with_error(tenant_id, model_name)
        generator = get_generator(model)
        data_storages = await self.datastorage_repository.get_datastorage_orm_list_by_session(tenant_id, model_name)
        if check_possible_to_drop_model:
            check_result, possible_to_drop = await self._check_possible_to_drop_model(
                generator, data_storages, ignore, model
            )
            if not possible_to_drop:
                logger.error("The model cannot be synchronized")
                return check_result
        result: list[MetaSynchronizerResponse] = []
        for conter, data_storage in enumerate(data_storages):
            logger.info(
                "Datastorage %s.%s processing: %s/%s", model_name, data_storage.name, conter + 1, len(data_storages)
            )
            if data_storage.name in ignore:
                await self._create_ignore_message_and_append_to_result_list(
                    result, data_storage, model, "in ignore list"
                )
            else:
                await self._recreate_data_storage_and_append_to_result_list(
                    result, generator, data_storage, model, сheck_possible_to_drop_data_storage, fall_if_exception
                )
        return result

    async def create_composites_in_database_from_meta(
        self,
        tenant_id: str,
        model_name: str,
        ignore: list[str],
        fall_if_exception: bool = False,
        generate_on_db: bool = True,
        replace: bool = True,
    ) -> list[MetaSynchronizerResponse]:
        """
        Обновить все view в базе данных, которые привязаны к модели.
        Создать или пересоздать view (если они не пустые).
        Args:
            ignore (list[str]): Список view, пересоздание которых нужно игнорировать.
        """
        model = await self.model_repository.get_model_orm_by_session_with_error(tenant_id, model_name)
        generator = get_generator(model)
        composites = await self.composite_repository.get_composite_orm_list_by_session(
            tenant_id, model_name, Pagination(sort_direction=SortDirectionEnum.asc)
        )
        result = []
        for conter, composite in enumerate(composites):
            logger.info("Composite %s.%s processing: %s/%s", model_name, composite.name, conter + 1, len(composites))
            if composite.name in ignore:
                result.append(
                    MetaSynchronizerResponse(
                        tenant_id=composite.tenant_id,
                        model=model.name,
                        object_name=composite.name,
                        msg=None,
                        result=GeneratorResult.IGNORED,
                    )
                )
                schema_name = composite.database_objects[0].schema_name if composite.database_objects else "unknown"
                logger.warning(
                    "Composite ignored: %s.%s!",
                    composite.name,
                    schema_name,
                )
            else:
                try:
                    result.append(
                        await self._recreate_composite(
                            generator,
                            composite,
                            model,
                            generate_on_db=generate_on_db,
                            replace=replace,
                        )
                    )
                except Exception as exc:  # noqa
                    logger.exception("error creating composite.")
                    result.append(
                        MetaSynchronizerResponse(
                            tenant_id=composite.tenant_id,
                            model=model.name,
                            object_name=composite.name,
                            msg=str(exc),
                            result=GeneratorResult.FAILURE,
                        )
                    )
                    if fall_if_exception:
                        raise Exception(str(exc))
        return result

    async def delete_composites_in_database_from_meta(
        self,
        tenant_id: str,
        model_name: str,
        ignore: list[str],
        fall_if_exception: bool = False,
    ) -> list[MetaSynchronizerResponse]:
        """
        Удалить все view в базе данных, которые привязаны к модели.
        Args:
            ignore (list[str]): Список view, пересоздание которых нужно игнорировать.
        """
        model = await self.model_repository.get_model_orm_by_session_with_error(tenant_id, model_name)
        generator = get_generator(model)
        composites = await self.composite_repository.get_composite_orm_list_by_session(tenant_id, model_name)
        result = []
        for conter, composite in enumerate(composites):
            logger.info("Composite %s.%s deleting: %s/%s", model_name, composite.name, conter + 1, len(composites))
            if composite.name in ignore:
                result.append(
                    MetaSynchronizerResponse(
                        tenant_id=composite.tenant_id,
                        model=model.name,
                        object_name=composite.name,
                        msg=None,
                        result=GeneratorResult.IGNORED,
                    )
                )
                schema_name = composite.database_objects[0].schema_name if composite.database_objects else "unknown"
                logger.warning(
                    "Composite ignored: %s.%s!",
                    composite.name,
                    schema_name,
                )
            else:
                try:
                    result.append(await self._delete_composite(generator, composite, model))
                except Exception as exc:  # noqa
                    logger.exception("error deleting composite.")
                    result.append(
                        MetaSynchronizerResponse(
                            tenant_id=composite.tenant_id,
                            model=model.name,
                            object_name=composite.name,
                            msg=str(exc),
                            result=GeneratorResult.FAILURE,
                        )
                    )
                    if fall_if_exception:
                        raise Exception(str(exc))
        return result

    async def create_data_storage_in_database_from_meta(
        self,
        tenant_id: str,
        model_name: str,
        data_storage_name: str,
    ) -> MetaSynchronizerResponse:
        """
        Обновить таблицу в базе данных для DataStorage.
        Создать таблицу в базе данных, если её нет или пересоздать (если она пустая).
        """
        model = await self.model_repository.get_model_orm_by_session_with_error(tenant_id, model_name)
        generator = get_generator(model)
        data_storage = await self.datastorage_repository.get_datastorage_orm_by_session(
            tenant_id, model_name, data_storage_name
        )
        if not data_storage:
            raise NoResultFound(
                f"DataStorage with tenant_id={tenant_id}, model_name={model_name} and name={data_storage_name} not found."
            )
        try:
            await generator.recreate_data_storage(data_storage, model)
            return MetaSynchronizerResponse(
                tenant_id=data_storage.tenant_id,
                model=model.name,
                object_name=data_storage.name,
                msg=None,
                result=GeneratorResult.SUCCESS,
            )
        except Exception as exc:  # noqa
            logger.exception("Error creating datastorage.")
            return MetaSynchronizerResponse(
                tenant_id=data_storage.tenant_id,
                model=model.name,
                object_name=data_storage.name,
                msg=str(exc),
                result=GeneratorResult.FAILURE,
            )

    async def create_composite_in_database_from_meta(
        self,
        tenant_id: str,
        model_name: str,
        composite_name: str,
        generate_on_db: bool = True,
        replace: bool = True,
    ) -> MetaSynchronizerResponse:
        """
        Обновить view в базе данных для Composite.
        Создать view в базе данных, если её нет или пересоздать (если она пустая).
        """
        model = await self.model_repository.get_model_orm_by_session_with_error(tenant_id, model_name)
        generator = get_generator(model)
        composite = await self.composite_repository.get_composite_orm_by_session(tenant_id, model_name, composite_name)
        if not composite:
            raise NoResultFound(
                f"Composite with tenant_id={tenant_id}, model_name={model_name} and name={composite_name} not found."
            )
        return await self._recreate_composite(generator, composite, model, generate_on_db, replace)

    @classmethod
    def get_by_session(cls, session: AsyncSession) -> "MetaSynchronizerRepository":
        model_repository = ModelRepository.get_by_session(session)
        datastorage_repository = DataStorageRepository.get_by_session(session)
        composite_repository = CompositeRepository.get_by_session(session)
        return cls(
            session,
            model_repository,
            composite_repository,
            datastorage_repository,
        )

    async def upate_datastorages_in_database_from_meta(
        self,
        tenant_id: str,
        model_name: str,
        ignore: list[str],
        enable_delete_column: bool = False,
        enable_delete_not_empty: bool = False,
        fall_if_exception: bool = False,
        white_list_names: Optional[set[str]] = None,
        white_list_types: Optional[set[DataStorageEnum]] = None,
        generate_on_db: bool = True,
    ) -> list[MetaSynchronizerResponse]:
        """
        Обновляет хранилища данных в базе данных на основе метаданных модели.

        Асинхронно обрабатывает список хранилищ, исключая указанные в списке `ignore`.
        Для каждого хранилища вызывает генератор для обновления структуры данных.
        Результаты операций сохраняются в виде объектов MetaSynchronizerResponse.

        Args:
            tenant_id (str): Идентификатор тенанта.
            model_name (str): Название модели, на основе которой выполняется синхронизация.
            ignore (list[str]): Список имен хранилищ, которые должны быть пропущены.
            enable_delete_column (bool, optional): Флаг, разрешающий удаление колонок. Defaults to False.
            enable_delete_not_empty (bool, optional): Флаг удаления непустых хранилищ/колонок. Defaults to False.
            fall_if_exception (bool, optional): Флаг немедленного завершения при ошибке. Defaults to False.

        Returns:
            list[MetaSynchronizerResponse]: Список результатов обработки хранилищ:
                - tenant_id: Идентификатор тенанта.
                - model: Название модели.
                - object_name: Имя обработанного хранилища.
                - comment: Сообщение об ошибке (если есть).
                - result: Результат операции (SUCCESS/FAILURE).

        Raises:
            Exception: Выбрасывается при ошибке обновления, если `fall_if_exception=True`.
        """
        if white_list_names is None:
            white_list_names = set()
        if white_list_types is None:
            white_list_types = set()
        result: list[MetaSynchronizerResponse] = []
        model = await self.model_repository.get_model_orm_by_session_with_error(tenant_id, model_name)
        generator = get_generator(model)
        conter = 0
        data_storages = await self.datastorage_repository.get_datastorage_orm_list_by_session(tenant_id, model_name)
        for data_storage in data_storages:
            if white_list_names and data_storage.name not in white_list_names:
                continue
            if white_list_types and data_storage.type not in white_list_types:
                continue
            logger.info(
                "Datastorage %s.%s processing: %s/%s", model_name, data_storage.name, conter + 1, len(data_storages)
            )
            conter += 1
            if data_storage.name in ignore:
                await self._create_ignore_message_and_append_to_result_list(
                    result, data_storage, model, "in ignore list"
                )
                continue
            try:
                if generate_on_db:
                    await generator.update_datastorage(
                        data_storage, model, enable_delete_column, enable_delete_not_empty
                    )
                result.append(
                    MetaSynchronizerResponse(
                        tenant_id=data_storage.tenant_id,
                        model=model.name,
                        object_name=data_storage.name,
                        msg=None,
                        result=GeneratorResult.SUCCESS,
                    )
                )
            except Exception as exc:
                logger.exception("Error updating datastorage")
                result.append(
                    MetaSynchronizerResponse(
                        tenant_id=data_storage.tenant_id,
                        model=model.name,
                        object_name=data_storage.name,
                        msg=str(exc),
                        result=GeneratorResult.FAILURE,
                    )
                )
                if fall_if_exception:
                    raise Exception(str(exc))
                continue

        return result

    async def upate_datastorage_in_database_from_meta(
        self,
        tenant_id: str,
        model_name: str,
        data_storage_name: str,
        generate_on_db: bool = True,
    ) -> MetaSynchronizerResponse:
        """
        Обновить таблицу в базе данных для DataStorage.
        Создать таблицу в базе данных, если её нет или пересоздать (если она пустая).
        """
        model = await self.model_repository.get_model_orm_by_session_with_error(tenant_id, model_name)
        generator = get_generator(model)
        data_storage = await self.datastorage_repository.get_datastorage_orm_by_session(
            tenant_id, model_name, data_storage_name
        )
        if not data_storage:
            raise NoResultFound(
                f"DataStorage with tenant_id={tenant_id}, model_name={model_name} and name={data_storage_name} not found."
            )
        try:
            if generate_on_db:
                await generator.update_datastorage(data_storage, model)
            return MetaSynchronizerResponse(
                tenant_id=data_storage.tenant_id,
                model=model.name,
                object_name=data_storage.name,
                msg=None,
                result=GeneratorResult.SUCCESS,
            )
        except Exception as exc:  # noqa
            logger.exception("Error updating datastorage.")
            return MetaSynchronizerResponse(
                tenant_id=data_storage.tenant_id,
                model=model.name,
                object_name=data_storage.name,
                msg=str(exc),
                result=GeneratorResult.FAILURE,
            )

    async def upate_dictionary_in_model(
        self,
        tenant_id: str,
        model_name: str,
        ignore: list[str],
        fall_if_exception: bool = False,
    ) -> list[MetaSynchronizerResponse]:
        """
        Обновляет все объекты типа Dictionary в модели, связанной с ClickHouse базой данных.

        Args:
            tenant_id (str): Идентификатор тенанта.
            model_name (str): Название модели, для которой выполняется обновление.
            ignore (list[str]): Список имен Datastorage, которые следует игнорировать.
            fall_if_exception (bool, optional): Если True, выбрасывает исключение при ошибке.
                                            По умолчанию False.

        Returns:
            list[MetaSynchronizerResponse]: Список ответов о результатах обновления.
                                            Каждый элемент содержит информацию о статусе
                                            выполнения для конкретного объекта данных.

        Raises:
            Exception: Выбрасывается при ошибке обновления, если fall_if_exception=True.
        """
        result: list[MetaSynchronizerResponse] = []
        model = await self.model_repository.get_model_orm_by_session_with_error(tenant_id, model_name)
        generator = get_generator(model)
        if generator != GeneratorClickhouseRepository:
            logger.warning(
                "%s's model is not tied to a clickhouse..Generator %s not support update dictionary",
                generator.__class__.__name__,
            )
            return result
        model_model = ModelModel.model_validate(model)
        data_storages = await self.datastorage_repository.get_datastorage_orm_list_by_session(tenant_id, model_name)
        logger.info("Start update dictionary for model %s", model.name)
        success = 0
        failed = 0
        ignored = 0
        for data_storage in data_storages:
            if data_storage.name in ignore:
                await self._create_ignore_message_and_append_to_result_list(result, data_storage, model)
                ignored += 1
                continue
            try:
                _ = await generator.recreate_dictionary(data_storage, model_model)
                success += 1
                result.append(
                    MetaSynchronizerResponse(
                        tenant_id=data_storage.tenant_id,
                        model=model.name,
                        object_name=data_storage.name,
                        msg=None,
                        result=GeneratorResult.SUCCESS,
                    )
                )
            except Exception as exc:
                logger.exception("Error updating dictionary")
                result.append(
                    MetaSynchronizerResponse(
                        tenant_id=data_storage.tenant_id,
                        model=model.name,
                        object_name=data_storage.name,
                        msg=str(exc),
                        result=GeneratorResult.FAILURE,
                    )
                )
                failed += 1
                if fall_if_exception:
                    raise Exception(str(exc))
        logger.info(
            "End update dictionary for model %s. Success: %s. Failed: %s. Ignored: %s. Total: %s",
            model.name,
            success,
            failed,
            ignored,
            len(data_storages),
        )
        return result
