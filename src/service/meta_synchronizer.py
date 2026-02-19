"""
Сервис синхронизации метаинформации с базой.
Тут реализована вся логика работы сервиса.
Работа с БД инкапсулирована через DataRepository.
"""

from typing import Optional

from py_common_lib.logger import EPMPYLogger

from src.integration.aor import ClientAOR
from src.integration.pv_dictionaries.models import PVDictionaryWithoutName
from src.integration.worker_manager import ClientWorkerManager
from src.models.dimension import Dimension as DimensionModel
from src.models.meta_synchronizer import (
    DetailMetaSynchronizerResponse,
    DetailsMetaSynchronizerResponse,
    GeneratorResult,
    MetaSynchronizerResponse,
)
from src.repository.aor import AorRepository
from src.repository.dimension import DimensionRepository
from src.repository.meta_synchronizer import MetaSynchronizerRepository
from src.repository.model_relations import ModelRelationsRepository
from src.service.dimension import DimensionService

logger = EPMPYLogger(__name__)


class MetaSynchronizerService:
    def __init__(
        self,
        data_repository: MetaSynchronizerRepository,
        client_worker_manager: ClientWorkerManager,
        aor_client: ClientAOR,
    ) -> None:
        self.data_repository: MetaSynchronizerRepository = data_repository
        self.client_worker_manager = client_worker_manager
        self.aor_client = aor_client

    async def create_data_storages_in_database_from_meta(
        self,
        tenant_id: str,
        model_name: str,
        ignore: list[str],
        check_possible_to_drop_model: bool = True,
    ) -> DetailsMetaSynchronizerResponse:
        """
        Обновить все таблицы в базе данных, которые привязаны к модели.
        Создать или пересоздать таблицы (если они не пустые).
        Args:
            check_possible_to_drop_model (bool): Флаг запрещает пересоздавать все таблицы,
        если хотя бы одна из таблиц непустая.
        """
        result = await self.data_repository.create_data_storages_in_database_from_meta(
            tenant_id, model_name, ignore, check_possible_to_drop_model
        )
        return DetailsMetaSynchronizerResponse(detail=result)

    async def update_data_storages_in_database_from_meta(
        self,
        tenant_id: str,
        model_name: str,
        ignore: list[str],
        fall_if_exception: bool = False,
        generate_on_db: bool = True,
        enable_delete_column: bool = True,
        enable_delete_not_empty: bool = False,
    ) -> DetailsMetaSynchronizerResponse:
        """
        Обновить все таблицы в базе данных, которые привязаны к модели.
        Создать или пересоздать таблицы (если они не пустые).
        Args:
            check_possible_to_drop_model (bool): Флаг запрещает пересоздавать все таблицы,
        если хотя бы одна из таблиц непустая.
        """
        result = await self.data_repository.upate_datastorages_in_database_from_meta(
            tenant_id, model_name, ignore, enable_delete_column, fall_if_exception, generate_on_db=generate_on_db
        )
        if not generate_on_db:
            await self.client_worker_manager.update_data_storage(
                tenant_id,
                [model_name],
                [response.object_name for response in result],
                enable_delete_column,
                enable_delete_not_empty,
            )
        return DetailsMetaSynchronizerResponse(detail=result)

    async def create_data_storage_in_database_from_meta(
        self,
        tenant_id: str,
        model_name: str,
        data_storage_name: str,
    ) -> DetailMetaSynchronizerResponse:
        """
        Обновить таблицу в базе данных для DataStorage.
        Создать таблицу в базе данных, если её нет или пересоздать (если она пустая).
        """
        result = await self.data_repository.create_data_storage_in_database_from_meta(
            tenant_id, model_name, data_storage_name
        )
        return DetailMetaSynchronizerResponse(detail=result)

    async def update_data_storage_in_database_from_meta(
        self,
        tenant_id: str,
        model_name: str,
        data_storage_name: str,
        generate_on_db: bool = True,
        enable_delete_column: bool = True,
        enable_delete_not_empty: bool = False,
    ) -> DetailMetaSynchronizerResponse:
        """
        Обновить таблицу в базе данных для DataStorage.
        Создать таблицу в базе данных, если её нет или пересоздать (если она пустая).
        """
        result = await self.data_repository.upate_datastorage_in_database_from_meta(
            tenant_id,
            model_name,
            data_storage_name,
            generate_on_db=generate_on_db,
        )
        if not generate_on_db:
            await self.client_worker_manager.update_data_storage(
                tenant_id, [model_name], [data_storage_name], enable_delete_column, enable_delete_not_empty
            )
        return DetailMetaSynchronizerResponse(detail=result)

    async def create_composites_in_database_from_meta(
        self,
        tenant_id: str,
        model_name: str,
        ignore: list[str],
        generate_on_db: bool = True,
        replace: bool = True,
    ) -> DetailsMetaSynchronizerResponse:
        """
        Обновить все view в базе данных, которые привязаны к модели.
        Создать или пересоздать view.
        """
        result = await self.data_repository.create_composites_in_database_from_meta(
            tenant_id,
            model_name,
            ignore,
            generate_on_db=generate_on_db,
            replace=replace,
        )
        if not generate_on_db:
            await self.client_worker_manager.create_composite(
                tenant_id, [model_name], [result_composite.object_name for result_composite in result], replace
            )
        return DetailsMetaSynchronizerResponse(detail=result)

    async def create_composite_in_database_from_meta(
        self,
        tenant_id: str,
        model_name: str,
        composite_name: str,
        generate_on_db: bool = True,
        replace: bool = True,
    ) -> DetailMetaSynchronizerResponse:
        """
        Обновить view в базе данных для Compiste.
        Создать view в базе данных, если её нет или пересоздать.
        """
        result = await self.data_repository.create_composite_in_database_from_meta(
            tenant_id,
            model_name,
            composite_name,
            generate_on_db=generate_on_db,
            replace=replace,
        )
        if not generate_on_db:
            await self.client_worker_manager.create_composite(tenant_id, [model_name], [result.object_name], replace)
        return DetailMetaSynchronizerResponse(detail=result)

    async def create_all_pvds_for_dimensions_in_model(
        self,
        tenant_id: str,
        model_name: str,
        pv_dictionary_without_name: Optional[PVDictionaryWithoutName] = None,
        recreate: bool = False,
    ) -> DetailsMetaSynchronizerResponse:
        """
        Создать все справочники в PVD для модели.

        Args:
            tenant_id (str): Имя тенанта, в котором находятся справочники
            model_name (str): Имя модели, в которой находятся справочники
            pv_dictionary_without_name (Optional[PVDictionaryWithoutName]): Поля pv_dictionary с доменом и тенантом
        Returns:
            list[MetaSynchronizerResponse]: Список успешно или неуспешно созданных справочников
        """
        service = DimensionService(
            DimensionRepository.get_by_session(self.data_repository.session),
            ModelRelationsRepository.get_by_session(self.data_repository.session),
            self.client_worker_manager,
            self.aor_client,
            aor_repository=AorRepository.get_by_session(self.data_repository.session),
        )
        result = []
        dimensions: list[DimensionModel] = await service.data_repository.get_list(tenant_id, model_name)
        for dimension in dimensions:
            if dimension.pv_dictionary is not None:
                logger.debug("The object %s is already created dimension", dimension.name)
                result.append(
                    MetaSynchronizerResponse(
                        tenant_id=tenant_id,
                        model=model_name,
                        object_name=dimension.name,
                        msg="already created",
                        result=GeneratorResult.IGNORED,
                    )
                )
                continue
            if dimension.is_virtual or dimension.dimension_name:
                logger.debug("The object %s is reference or virtual", dimension.name)
                result.append(
                    MetaSynchronizerResponse(
                        tenant_id=tenant_id,
                        model=model_name,
                        object_name=dimension.name,
                        msg="Reference or virtual",
                        result=GeneratorResult.IGNORED,
                    )
                )
                continue
            try:
                await service.data_repository.create_pv_dictionary_by_dimension(
                    tenant_id, dimension.name, with_error=False, commit=True
                )
            except Exception as exc:
                logger.exception("Error process dimension: %s", dimension.name)
                result.append(
                    MetaSynchronizerResponse(
                        tenant_id=tenant_id,
                        model=model_name,
                        object_name=dimension.name,
                        msg=str(exc),
                        result=GeneratorResult.FAILURE,
                    )
                )
                continue
            result.append(
                MetaSynchronizerResponse(
                    tenant_id=tenant_id,
                    model=model_name,
                    object_name=dimension.name,
                    msg=None,
                    result=GeneratorResult.SUCCESS,
                )
            )
        return DetailsMetaSynchronizerResponse(detail=result)

    def __repr__(self) -> str:
        return "MetaSynchronizerService"
