from operator import or_
from typing import Optional

from py_common_lib.logger import EPMPYLogger
from sqlalchemy import Update, case, func, select, update
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from src.db.composite import Composite, CompositeDatasource, CompositeField, CompositeModelRelation
from src.db.data_storage import DataStorage, DataStorageField, DataStorageModelRelation
from src.db.database_object import DatabaseObject, DatabaseObjectModelRelation
from src.db.dimension import Dimension, DimensionAttribute, DimensionModelRelation, PVDctionary
from src.db.measure import DimensionFilter, Measure, MeasureModelRelation
from src.db.model import Model
from src.models.model_relations import ChangeObjectStatusRequest, ChangeObjectStatusResponse
from src.models.tenant import SemanticObjects
from src.repository.composite import CompositeRepository
from src.repository.data_storage import DataStorageRepository
from src.repository.measure import MeasureRepository
from src.repository.model import ModelRepository

logger = EPMPYLogger(__name__)


class ModelRelationsRepository:

    def __init__(
        self,
        session: AsyncSession,
        model_repository: ModelRepository,
        datastorage_repository: DataStorageRepository,
        measure_repository: MeasureRepository,
        composite_repository: CompositeRepository,
    ) -> None:
        self.session = session
        self.model_repository = model_repository
        self.datastorage_repository = datastorage_repository
        self.measure_repository = measure_repository
        self.composite_repository = composite_repository

    async def _get_dimensions_names_where_attr_is_target_dimension(
        self, target_dimension_id: int, model_id: int
    ) -> list[str]:
        """
        Функция, которая возвращает имена измерений (dimensions) модели,
        имеющих атрибут target_dimension.
        """
        query = (
            select(
                Dimension.name,
            )
            .select_from(DimensionAttribute)
            .join(DimensionModelRelation, DimensionAttribute.dimension_id == DimensionModelRelation.dimension_id)
            .join(Dimension, DimensionAttribute.dimension_id == Dimension.id)
            .where(
                DimensionAttribute.dimension_attribute_id == target_dimension_id,
                DimensionModelRelation.model_id == model_id,
                DimensionAttribute.dimension_id != target_dimension_id,
            )
        )
        result = list((await self.session.execute(query)).unique().scalars().all())
        return result

    async def _get_dimensions_names_where_ref_dimension_is_target_dimension(
        self, target_dimension_id: int, model_id: int
    ) -> list[str]:
        """
        Функция, которая возвращает имена измерений (dimensions) модели,
        имеющих атрибут ссылочное измерение target_dimension.
        """
        query = (
            select(
                Dimension.name,
            )
            .select_from(Dimension)
            .join(DimensionModelRelation, Dimension.id == DimensionModelRelation.dimension_id)
            .where(
                Dimension.dimension_id == target_dimension_id,
                DimensionModelRelation.model_id == model_id,
            )
        )
        result = list((await self.session.execute(query)).unique().scalars().all())
        return result

    async def _get_measures_names_where_unit_is_target_dimension(
        self,
        target_dimension_id: int,
        model_id: int,
    ) -> list[str]:
        """
        Функция, которая возвращает имена показателей (measure) модели,
        имеющих unit_of_measure target_dimension.
        """
        query = (
            select(
                Measure.name,
            )
            .select_from(Measure)
            .join(MeasureModelRelation, Measure.id == MeasureModelRelation.measure_id)
            .where(
                Measure.dimension_id == target_dimension_id,
                MeasureModelRelation.model_id == model_id,
            )
        )
        result = list((await self.session.execute(query)).unique().scalars().all())
        return result

    async def _get_measures_names_where_filters_is_target_dimension(
        self,
        target_dimension_id: int,
        model_id: int,
    ) -> list[str]:
        """
        Функция, которая возвращает имена показателей (measure) модели,
        имеющих filter target_dimension.
        """
        query = (
            select(
                Measure.name,
            )
            .select_from(DimensionFilter)
            .join(MeasureModelRelation, DimensionFilter.measure_id == MeasureModelRelation.measure_id)
            .join(Measure, Measure.id == DimensionFilter.measure_id)
            .where(
                DimensionFilter.dimension_id == target_dimension_id,
                MeasureModelRelation.model_id == model_id,
            )
        )
        result = list((await self.session.execute(query)).unique().scalars().all())
        return result

    async def _get_composites_names_where_field_is_target_dimension(
        self, target_dimension_id: int, model_id: int
    ) -> list[str]:
        """
        Функция, которая возвращает имена композитов (Composites) модели,
        имеющих атрибут target_dimension.
        """
        query = (
            select(
                Composite.name,
            )
            .select_from(CompositeField)
            .join(CompositeModelRelation, CompositeField.composite_id == CompositeModelRelation.composite_id)
            .join(Composite, CompositeField.composite_id == Composite.id)
            .where(
                CompositeField.dimension_id == target_dimension_id,
                CompositeModelRelation.model_id == model_id,
            )
        )
        result = list((await self.session.execute(query)).unique().scalars().all())
        return result

    async def _get_datastorages_names_where_field_is_target_dimension(
        self,
        target_dimension_id: int,
        model_id: int,
        exclude_dso: Optional[list[int]] = None,
    ) -> list[str]:
        """
        Функция, которая возвращает имена хранилищ (datastorages) модели,
        имеющих атрибут target_dimension.
        """
        if not exclude_dso:
            exclude_dso = []
            exclude_dso_query = select(
                Dimension.text_table_id, Dimension.attributes_table_id, Dimension.values_table_id
            ).where(Dimension.id == target_dimension_id)
            result_exclude_dso = (await self.session.execute(exclude_dso_query)).unique().tuples().all()
            for row in result_exclude_dso:
                for table_id in row:
                    if table_id is not None:
                        exclude_dso.append(table_id)
        query = (
            select(
                DataStorage.name,
            )
            .select_from(DataStorageField)
            .join(
                DataStorageModelRelation, DataStorageModelRelation.data_storage_id == DataStorageField.data_storage_id
            )
            .join(DataStorage, DataStorage.id == DataStorageField.data_storage_id)
            .where(
                DataStorageModelRelation.model_id == model_id,
                DataStorageField.dimension_id == target_dimension_id,
                DataStorage.id.not_in(exclude_dso),
            )
        )
        result = list((await self.session.execute(query)).unique().scalars().all())
        return result

    async def check_if_dimension_can_be_deleted(
        self,
        target_dimension_id: int,
        model_id: int,
        exclude_dso: Optional[list[int]] = None,
    ) -> tuple[bool, Optional[dict]]:
        """
        Возвращает флаг возможности удаления dimension и словарь объекты, которые мешают удалению.
        Args:
            target_dimension_id (int): id измерения,
            model_id (int): id модели,
            exclude_dso (list[int]): список id хранилищ, которые не должны быть удалены
        Returns:
            tuple[bool, Optional[dict]]: флаг возможности удаления dimension и словарь объектов, которые мешают удалению
        """
        related_objects = await self._get_dimension_related_objects(target_dimension_id, model_id, exclude_dso)
        if not related_objects.is_empty():
            return False, {
                "Dimensions": related_objects.dimensions,
                "DataStorages": related_objects.data_storages,
                "Measures": related_objects.measures,
                "Composites": related_objects.composites,
            }
        return True, None

    async def _get_dimension_related_objects(
        self, dimension_id: int, model_id: int, exclude_dso: Optional[list[int]]
    ) -> SemanticObjects:
        dimension_names = await self._get_dimensions_names_where_attr_is_target_dimension(
            dimension_id,
            model_id,
        )
        dimension_names.extend(
            await self._get_dimensions_names_where_ref_dimension_is_target_dimension(
                dimension_id,
                model_id,
            )
        )
        data_storages = await self._get_datastorages_names_where_field_is_target_dimension(
            dimension_id,
            model_id,
            exclude_dso,
        )
        measures = await self._get_measures_names_where_unit_is_target_dimension(
            dimension_id,
            model_id,
        )
        measures.extend(
            await self._get_measures_names_where_filters_is_target_dimension(
                dimension_id,
                model_id,
            )
        )
        composites = await self._get_composites_names_where_field_is_target_dimension(
            dimension_id,
            model_id,
        )
        return SemanticObjects(
            dimensions=dimension_names,
            data_storages=data_storages,
            measures=measures,
            composites=composites,
        )

    async def _find_dimension_id_by_name(self, dimension_name: str, model_id: int) -> int:
        """
            найти id измерения по имени
        Args:
            dimension_name: имя измерения
            model_id: id модели
        Returns:
            int: id измерения
        """
        query = (
            select(Dimension.id)
            .select_from(Dimension)
            .join(DimensionModelRelation, Dimension.id == DimensionModelRelation.dimension_id)
            .where(
                Dimension.name == dimension_name,
                DimensionModelRelation.model_id == model_id,
            )
        )
        result = (await self.session.execute(query)).unique().scalar()
        if not result:
            raise NoResultFound(f"Не удалось найти измерение с именем {dimension_name} в модели с id {model_id}")
        return result

    async def get_dimension_related_objects(
        self,
        model_name: str,
        object_name: str,
        tenant_name: str,
    ) -> SemanticObjects:
        """
            получить все объекты, которые связаны с измерением
        Args:
            model_name: имя модели
            object_name: имя измерения
            tenant_name: имя тенанта
        Returns:
            SemanticObjects: объект с именами связанных объектов
        """
        model_id = await self.model_repository.get_id_by_name(tenant_name, model_name)
        dimension_id = await self._find_dimension_id_by_name(object_name, model_id)

        return await self._get_dimension_related_objects(dimension_id, model_id, exclude_dso=None)

    async def _get_measure_related_dimensions(self, measure_id: int, model_id: int) -> list[str]:
        """
            получить все измерения, которые связаны с показателем
        Args:
            measure_id: id показателя
            model_id: id модели
        Returns:
            list[str]: список имен измерений
        """
        query = (
            select(
                Dimension.name,
            )
            .select_from(DimensionAttribute)
            .join(DimensionModelRelation, DimensionAttribute.dimension_id == DimensionModelRelation.dimension_id)
            .join(Dimension, DimensionAttribute.dimension_id == Dimension.id)
            .where(
                DimensionAttribute.measure_attribute_id == measure_id,
                DimensionModelRelation.model_id == model_id,
            )
        )
        return list((await self.session.execute(query)).unique().scalars().all())

    async def _get_measure_related_data_storage(
        self,
        measure_id: int,
        model_id: int,
    ) -> list[str]:
        """
            получить все хранилища данных, которые связаны с показателем
        Args:
            measure_id: id показателя
            model_id: id модели
        Returns:
            list[str]: список имен хранилищ данных
        """
        query = (
            select(
                DataStorage.name,
            )
            .select_from(DataStorageField)
            .join(
                DataStorageModelRelation, DataStorageModelRelation.data_storage_id == DataStorageField.data_storage_id
            )
            .join(DataStorage, DataStorage.id == DataStorageField.data_storage_id)
            .where(
                DataStorageModelRelation.model_id == model_id,
                DataStorageField.measure_id == measure_id,
            )
        )
        return list((await self.session.execute(query)).unique().scalars().all())

    async def _get_measure_related_composite(self, measure_id: int, model_id: int) -> list[str]:
        """
            получить все композиты, которые связаны с показателем
        Args:
            measure_id: id показателя
            model_id: id модели
        Returns:
            list[str]: список имен композитов
        """
        query = (
            select(
                Composite.name,
            )
            .select_from(CompositeField)
            .join(CompositeModelRelation, CompositeModelRelation.composite_id == CompositeField.composite_id)
            .join(Composite, Composite.id == CompositeField.composite_id)
            .where(
                CompositeModelRelation.model_id == model_id,
                CompositeField.measure_id == measure_id,
            )
        )
        return list((await self.session.execute(query)).unique().scalars().all())

    async def _get_measure_related_objects(
        self,
        measure_id: int,
        model_id: int,
    ) -> SemanticObjects:
        """
            получить все объекты, которые связаны с показателем
        Args:
            measure_id: id показателя
            model_id: id модели
        Returns:
            SemanticObjects: объект, содержащий имена связанных объектов
        """
        dimension_names = await self._get_measure_related_dimensions(measure_id, model_id)
        data_storages = await self._get_measure_related_data_storage(measure_id, model_id)
        composites = await self._get_measure_related_composite(measure_id, model_id)
        return SemanticObjects(
            dimensions=dimension_names, data_storages=data_storages, composites=composites, measures=[]
        )

    async def get_measure_related_objects(
        self,
        model_name: str,
        object_name: str,
        tenant_name: str,
    ) -> SemanticObjects:
        """
            получить все объекты, которые связаны с показателем
        Args:
            model_name: имя модели
            object_name: имя показателя
            tenant_name: имя тенанта
        Returns:
            SemanticObjects: объект, содержащий имена связанных объектов
        """
        model_id = await self.model_repository.get_id_by_name(tenant_name, model_name)
        measure_id = await self.measure_repository.get_id_by_name(
            tenant_id=tenant_name, model_name=model_name, name=object_name
        )
        return await self._get_measure_related_objects(measure_id, model_id)

    async def _get_composite_related_composites(self, composite_id: int, model_id: int) -> list[str]:
        """
            получить все композиты, которые связаны с композитом
        Args:
            composite_id: id композита
            model_id: id модели
        Returns:
            list[str]: список имен композитов
        """
        query = (
            select(Composite.name)
            .select_from(CompositeDatasource)
            .join(CompositeModelRelation, CompositeDatasource.composite_id == CompositeModelRelation.composite_id)
            .join(Composite, CompositeDatasource.composite_id == Composite.id)
            .where(
                CompositeDatasource.composite_datasource_id == composite_id,
                CompositeModelRelation.model_id == model_id,
            )
        )
        return list((await self.session.execute(query)).unique().scalars().all())

    async def _get_composite_related_objects(
        self,
        composite_id: int,
        model_id: int,
    ) -> SemanticObjects:
        """
            получить все объекты, которые связаны с композитом
        Args:
            composite_id: id композита
            model_id: id модели
        Returns:
            SemanticObjects: объект, содержащий все связанные объекты
        """
        composites = await self._get_composite_related_composites(
            composite_id,
            model_id,
        )

        return SemanticObjects(dimensions=[], data_storages=[], measures=[], composites=composites)

    async def get_composite_related_objects(
        self,
        model_name: str,
        object_name: str,
        tenant_name: str,
    ) -> SemanticObjects:
        """
            получить все объекты, которые связаны с композитом
        Args:
            model_name: имя модели
            object_name: имя композита
            tenant_name: имя тенанта
        Returns:
            SemanticObjects: объект, содержащий все связанные объекты
        """
        model_id = await self.model_repository.get_id_by_name(tenant_name, model_name)
        composite_id = await self.composite_repository.get_id_by_name(
            tenant_id=tenant_name, model_name=model_name, name=object_name
        )

        return await self._get_composite_related_objects(composite_id, model_id)

    async def _get_datastorage_related_composites(
        self,
        data_storage_id: int,
        model_id: int,
    ) -> list[str]:
        """
            получить все композиты, которые связаны с datastorage
        Args:
            data_storage_id: id datastorage
            model_id: id модели
        Returns:
            list[str]: список имен композитов
        """
        query = (
            select(Composite.name)
            .select_from(CompositeDatasource)
            .join(CompositeModelRelation, CompositeDatasource.composite_id == CompositeModelRelation.composite_id)
            .join(Composite, CompositeDatasource.composite_id == Composite.id)
            .where(
                CompositeDatasource.datastorage_datasource_id == data_storage_id,
                CompositeModelRelation.model_id == model_id,
            )
        )
        return list((await self.session.execute(query)).unique().scalars().all())

    async def _get_datastorage_related_datastorages(
        self,
        data_storage_id: int,
        model_id: int,
    ) -> list[str]:
        """
        получить все объекты datastorage, которые связаны с datastorage
        Args:
            data_storage_id: id datastorage
            model_id: id модели
        Returns:
            list[str]: список имен datastorage
        """
        query = (
            select(
                DataStorage.name,
            )
            .select_from(DataStorage)
            .join(DataStorageModelRelation, DataStorageModelRelation.data_storage_id == DataStorage.id)
            .where(DataStorageModelRelation.model_id == model_id, DataStorage.log_data_storage_id == data_storage_id)
        )
        return list((await self.session.execute(query)).unique().scalars().all())

    async def _get_datastorage_related_dimensions(
        self,
        data_storage_id: int,
        model_id: int,
    ) -> list[str]:
        """
            получить все измерения, которые связаны с datastorage
        Args:
            data_storage_id: id datastorage
            model_id: id модели
        Returns:
            list[str]: список имен dimension
        """
        query = (
            select(
                Dimension.name,
            )
            .select_from(Dimension)
            .join(DimensionModelRelation, DimensionModelRelation.dimension_id == Dimension.id)
            .where(
                or_(
                    or_(Dimension.attributes_table_id == data_storage_id, Dimension.values_table_id == data_storage_id),
                    Dimension.text_table_id == data_storage_id,
                ),
                DimensionModelRelation.model_id == model_id,
            )
        )
        return list((await self.session.execute(query)).unique().scalars().all())

    async def _get_datastorage_related_objects(
        self,
        data_storage_id: int,
        model_id: int,
    ) -> SemanticObjects:
        """
            получить все объекты, которые связаны с datastorage
        Args:
            data_storage_id: id datastorage
            model_id: id модели
        Returns:
            SemanticObjects: Набор объектов семантики

        """
        composites = await self._get_datastorage_related_composites(
            data_storage_id,
            model_id,
        )
        data_storages = await self._get_datastorage_related_datastorages(
            data_storage_id,
            model_id,
        )
        dimensions = await self._get_datastorage_related_dimensions(
            data_storage_id,
            model_id,
        )
        return SemanticObjects(dimensions=dimensions, data_storages=data_storages, measures=[], composites=composites)

    async def get_datastorage_related_objects(
        self,
        model_name: str,
        object_name: str,
        tenant_name: str,
    ) -> SemanticObjects:
        """
            получить все объекты, которые связаны с datastorage
        Args:
            model_name: имя модели
            object_name: имя datastorage
            tenant_name: имя тенанта
        Returns:
            SemanticObjects: Набор объектов семантики
        """
        model_id = await self.model_repository.get_id_by_name(tenant_name, model_name)
        datastorage_id = await self.datastorage_repository.get_id_by_name(tenant_name, model_name, object_name)

        return await self._get_datastorage_related_objects(datastorage_id, model_id)

    @staticmethod
    def get_by_session(session: AsyncSession) -> "ModelRelationsRepository":
        if not isinstance(session, AsyncSession):
            raise TypeError("session must be an AsyncSession instance")
        else:
            return ModelRelationsRepository(
                session=session,
                model_repository=ModelRepository.get_by_session(session),
                datastorage_repository=DataStorageRepository.get_by_session(session),
                measure_repository=MeasureRepository.get_by_session(session),
                composite_repository=CompositeRepository.get_by_session(session),
            )

    async def _execute_stmt_and_form_change_object_response(
        self, stmt: Update, requests: list[ChangeObjectStatusRequest]
    ) -> list[ChangeObjectStatusResponse]:
        responses = []
        try:
            _ = await self.session.execute(stmt)
            for request in requests:
                responses.append(
                    ChangeObjectStatusResponse(
                        object_name=request.object_name,
                        status=request.status,
                        msg=request.msg,
                        schema_name=request.schema_name,
                        model=request.model,
                        updated=True,
                        object_type=request.object_type,
                    )
                )
        except Exception:
            for request in requests:
                responses.append(
                    ChangeObjectStatusResponse(
                        object_name=request.object_name,
                        status=request.status,
                        msg=request.msg,
                        schema_name=request.schema_name,
                        model=request.model,
                        updated=False,
                        object_type=request.object_type,
                    )
                )
            logger.exception("Error while updating object status")
        return responses

    async def update_dimension_model_relations_status_without_commit(
        self, tenant_id: str, requests: list[ChangeObjectStatusRequest]
    ) -> list[ChangeObjectStatusResponse]:
        """
        Обновляет статус отношений между измерениями (dimensions) и моделями (models), используя SQL-запросы.

        Функция принимает список запросов на изменение статуса объектов (`requests`), формирует словарь соответствий имен моделей/измерений новым состояниям и сообщениям, после чего обновляет записи в таблице `DimensionModelRelation` через выражение SQLAlchemy.

        Важно отметить, что транзакция НЕ фиксируется автоматически — фиксацию нужно вызывать отдельно после вызова данной функции.

        Args:
            tenant_id (str): Идентификатор тенанта (tenant).
            requests (list[ChangeObjectStatusRequest]): Список запросов на изменения статуса объектов формата ChangeObjectStatusRequest.

        Returns:
            list[ChangeObjectStatusResponse]: Список результатов выполнения запроса изменений объекта.
        """
        dim_alias = aliased(Dimension)
        model_alias = aliased(Model)
        status_dict = {f"{item.model}.{item.object_name}": item.status for item in requests}
        msg_dict = {f"{item.model}.{item.object_name}": item.msg for item in requests}
        stmt = (
            update(DimensionModelRelation)
            .values(
                status=case(status_dict, value=func.concat(model_alias.name, ".", dim_alias.name)),
                msg=case(msg_dict, value=func.concat(model_alias.name, ".", dim_alias.name)),
            )
            .where(
                DimensionModelRelation.dimension_id == dim_alias.id, DimensionModelRelation.model_id == model_alias.id
            )
            .where(
                func.concat(model_alias.name, ".", dim_alias.name).in_(status_dict.keys()),
                dim_alias.tenant_id == tenant_id,
            )
            .returning(DimensionModelRelation.id)
        )
        return await self._execute_stmt_and_form_change_object_response(stmt, requests)

    async def update_measure_model_relations_status_without_commit(
        self, tenant_id: str, requests: list[ChangeObjectStatusRequest]
    ) -> list[ChangeObjectStatusResponse]:
        """
        Обновляет статус связей между показателями (Measure) и моделями (Model), используя предоставленные запросы изменения статуса объектов.

        Args:
            tenant_id (str): Идентификатор тенанта (тенанта).
            requests (list[ChangeObjectStatusRequest]): Список запросов на изменение статуса объекта.

        Returns:
            list[ChangeObjectStatusResponse]: Список результатов выполнения запроса обновления состояния объектов.
        """
        meas_alias = aliased(Measure)
        model_alias = aliased(Model)
        status_dict = {f"{item.model}.{item.object_name}": item.status for item in requests}
        msg_dict = {f"{item.model}.{item.object_name}": item.msg for item in requests}
        stmt = (
            update(MeasureModelRelation)
            .values(
                status=case(status_dict, value=func.concat(model_alias.name, ".", meas_alias.name)),
                msg=case(msg_dict, value=func.concat(model_alias.name, ".", meas_alias.name)),
            )
            .where(
                MeasureModelRelation.measure_id == meas_alias.id,
                MeasureModelRelation.model_id == model_alias.id,
            )
            .where(
                func.concat(model_alias.name, ".", meas_alias.name).in_(status_dict.keys()),
                meas_alias.tenant_id == tenant_id,
            )
            .returning(MeasureModelRelation.id)
        )
        return await self._execute_stmt_and_form_change_object_response(stmt, requests)

    async def update_composite_model_relations_status_without_commit(
        self, tenant_id: str, requests: list[ChangeObjectStatusRequest]
    ) -> list[ChangeObjectStatusResponse]:
        """
        Обновляет статус связей между композитами (Composite) и моделями (Model), используя предоставленные запросы изменения статуса объектов.

        Args:
            tenant_id (str): Идентификатор тенанта (тенанта).
            requests (list[ChangeObjectStatusRequest]): Список запросов на изменение статуса объекта.

        Returns:
            list[ChangeObjectStatusResponse]: Список результатов выполнения запроса обновления состояния объектов.
        """
        com_alias = aliased(Composite)
        model_alias = aliased(Model)
        status_dict = {f"{item.model}.{item.object_name}": item.status for item in requests}
        msg_dict = {f"{item.model}.{item.object_name}": item.msg for item in requests}
        stmt = (
            update(CompositeModelRelation)
            .values(
                status=case(status_dict, value=func.concat(model_alias.name, ".", com_alias.name)),
                msg=case(msg_dict, value=func.concat(model_alias.name, ".", com_alias.name)),
            )
            .where(
                CompositeModelRelation.composite_id == com_alias.id,
                CompositeModelRelation.model_id == model_alias.id,
            )
            .where(
                func.concat(model_alias.name, ".", com_alias.name).in_(status_dict.keys()),
                com_alias.tenant_id == tenant_id,
            )
            .returning(CompositeModelRelation.id)
        )
        return await self._execute_stmt_and_form_change_object_response(stmt, requests)

    async def update_data_storage_model_relations_status_without_commit(
        self, tenant_id: str, requests: list[ChangeObjectStatusRequest]
    ) -> list[ChangeObjectStatusResponse]:
        """
        Обновляет статус связей между хранилищами (DataStorage) и моделями (Model), используя предоставленные запросы изменения статуса объектов.

        Args:
            tenant_id (str): Идентификатор тенанта (тенанта).
            requests (list[ChangeObjectStatusRequest]): Список запросов на изменение статуса объекта.

        Returns:
            list[ChangeObjectStatusResponse]: Список результатов выполнения запроса обновления состояния объектов.
        """
        storage_alias = aliased(DataStorage)
        model_alias = aliased(Model)
        status_dict = {f"{item.model}.{item.object_name}": item.status for item in requests}
        msg_dict = {f"{item.model}.{item.object_name}": item.msg for item in requests}
        stmt = (
            update(DataStorageModelRelation)
            .values(
                status=case(status_dict, value=func.concat(model_alias.name, ".", storage_alias.name)),
                msg=case(msg_dict, value=func.concat(model_alias.name, ".", storage_alias.name)),
            )
            .where(
                DataStorageModelRelation.data_storage_id == storage_alias.id,
                DataStorageModelRelation.model_id == model_alias.id,
            )
            .where(
                func.concat(model_alias.name, ".", storage_alias.name).in_(status_dict.keys()),
                storage_alias.tenant_id == tenant_id,
            )
            .returning(DataStorageModelRelation.id)
        )
        return await self._execute_stmt_and_form_change_object_response(stmt, requests)

    async def update_pv_dictionary_model_relations_status_without_commit(
        self, tenant_id: str, requests: list[ChangeObjectStatusRequest]
    ) -> list[ChangeObjectStatusResponse]:
        """
        Обновляет статус pv_dictionary для указанных объектов.

        Args:
            tenant_id (str): Идентификатор тенанта (тенанта).
            requests (list[ChangeObjectStatusRequest]): Список запросов на изменение статуса объекта.

        Returns:
            list[ChangeObjectStatusResponse]: Список результатов выполнения запроса обновления состояния объектов.
        """
        dim_alias = aliased(Dimension)
        status_dict = {item.object_name: item.status for item in requests}
        msg_dict = {item.object_name: item.msg for item in requests}
        stmt = (
            update(PVDctionary)
            .values(
                status=case(status_dict, value=dim_alias.name),
                msg=case(msg_dict, value=dim_alias.name),
            )
            .where(
                PVDctionary.id == dim_alias.pv_dictionary_id,
            )
            .where(
                dim_alias.name.in_(status_dict.keys()),
                dim_alias.tenant_id == tenant_id,
            )
            .returning(PVDctionary.id)
        )
        return await self._execute_stmt_and_form_change_object_response(stmt, requests)

    async def update_database_object_model_relations_status_without_commit(
        self, tenant_id: str, requests: list[ChangeObjectStatusRequest]
    ) -> list[ChangeObjectStatusResponse]:
        """
        Обновляет статус связей между объектами БД (DatabaseObject) и моделями (Model), используя предоставленные запросы изменения статуса объектов.

        Args:
            tenant_id (str): Идентификатор тенанта (тенанта).
            requests (list[ChangeObjectStatusRequest]): Список запросов на изменение статуса объекта.

        Returns:
            list[ChangeObjectStatusResponse]: Список результатов выполнения запроса обновления состояния объектов.
        """
        do_alias = aliased(DatabaseObject)
        model_alias = aliased(Model)
        status_dict = {f"{item.model}.{item.schema_name}.{item.object_name}": item.status for item in requests}
        msg_dict = {f"{item.model}.{item.schema_name}.{item.object_name}": item.msg for item in requests}
        stmt = (
            update(DatabaseObjectModelRelation)
            .values(
                status=case(
                    status_dict, value=func.concat(model_alias.name, ".", do_alias.schema_name, ".", do_alias.name)
                ),
                msg=case(msg_dict, value=func.concat(model_alias.name, ".", do_alias.schema_name, ".", do_alias.name)),
            )
            .where(
                DatabaseObjectModelRelation.database_object_id == do_alias.id,
                DatabaseObjectModelRelation.model_id == model_alias.id,
            )
            .where(
                func.concat(model_alias.name, ".", do_alias.schema_name, ".", do_alias.name).in_(
                    list(status_dict.keys())
                ),
                do_alias.tenant_id == tenant_id,
            )
            .returning(DatabaseObjectModelRelation.id)
        )
        return await self._execute_stmt_and_form_change_object_response(stmt, requests)
