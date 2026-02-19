"""
Репозиторий показателей.
"""

from typing import Any, Optional

from py_common_lib.utils import timeit
from sqlalchemy import select, update
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import logger
from src.db.measure import DimensionFilter, Measure, MeasureLabel, MeasureModelRelation
from src.db.model import Model
from src.models.measure import (
    Measure as MeasureModel,
    MeasureCreateRequest as MeasureCreateRequestModel,
    MeasureEditRequest as MeasureEditRequestModel,
)
from src.models.request_params import Pagination
from src.repository.history.measure import MeasureHistoryRepository
from src.repository.model import ModelRepository
from src.repository.utils import (
    add_missing_labels,
    convert_labels_list_to_orm,
    get_dimension_orm_model_by_session,
    get_select_query_with_offset_limit_order,
)


class MeasureRepository:

    def __init__(self, session: AsyncSession, model_repository: ModelRepository) -> None:
        self.session = session
        self.model_repository = model_repository
        self.measure_history_repository = MeasureHistoryRepository(session)

    async def _get_measure_orm_model_by_session(
        self, tenant_id: str, name: str, model_name: Optional[str] = None
    ) -> Any:
        """Получить объект SQLAlchemy Measure"""
        query = select(Measure).where(
            Measure.name == name,
            Measure.tenant_id == tenant_id,
        )
        query = query.where(Measure.models.any(Model.name == model_name)) if model_name else query
        result = (await self.session.execute(query)).scalars().one_or_none()
        return result

    async def _convert_filter_list_to_orm(
        self, tenant_id: str, model_names: Optional[list[str]], filters: list[dict]
    ) -> list[DimensionFilter]:
        """Создать список  SQLAlchemy DimensionFilter из списка словарей."""
        result_filter = []
        for filter_obj in filters:
            dimension = await get_dimension_orm_model_by_session(
                self.session, tenant_id=tenant_id, name=filter_obj["dimension_id"], model_names=model_names
            )
            filter_model = DimensionFilter(
                dimension_id=dimension.id,
                dimension_value=filter_obj.get("dimension_value"),
            )
            filter_model.dimension = dimension
            result_filter.append(filter_model)
        return result_filter

    def _prepare_unit_of_measure(self, measure: dict) -> None:
        """Конвертирует поле  unit_of_measure в поля dimension_id, dimension_value."""
        unit_of_measure = measure.pop("unit_of_measure", None)
        if isinstance(unit_of_measure, dict):
            measure["dimension_id"] = unit_of_measure["dimension_id"]
            measure["dimension_value"] = unit_of_measure["dimension_value"]
        else:
            measure["dimension_id"] = unit_of_measure
            measure["dimension_value"] = None

        return None

    @timeit
    async def get_by_name(self, tenant_id: str, name: str, model_name: Optional[str] = None) -> MeasureModel:
        """Получить измерение по имени."""
        result = await self._get_measure_orm_model_by_session(tenant_id=tenant_id, name=name, model_name=model_name)
        if not result:
            raise NoResultFound(
                f"Measure with tenant_id={tenant_id}, model_name={model_name} and name={name} not found."
            )
        return MeasureModel.model_validate(result)

    @timeit
    async def get_id_by_name(self, tenant_id: str, name: str, model_name: str) -> int:
        """
            Получить id измерение по имени.
        Args:
            tenant_id (str): Идентификатор клиента.
            name (str): Имя измерения.
            model_name (str): Имя модели.
        Returns:
            int: id измерения.
        """
        if result := await self._get_measure_orm_model_by_session(
            tenant_id=tenant_id, name=name, model_name=model_name
        ):
            return result.id
        else:
            raise NoResultFound(
                f"Measure with tenant_id={tenant_id}, model_name={model_name} and name={name} not found."
            )

    async def set_owner_model(self, measures: list[Measure], model: Model) -> None:
        """Обновляет состояние владельца модели."""
        measures_ids = [measure.id for measure in measures]
        await self.session.execute(
            update(MeasureModelRelation)
            .where(
                MeasureModelRelation.measure_id.in_(measures_ids),
            )
            .values({"is_owner": False})
        )
        await self.session.execute(
            update(MeasureModelRelation)
            .where(
                MeasureModelRelation.measure_id.in_(measures_ids),
                MeasureModelRelation.model_id == model.id,
            )
            .values({"is_owner": True})
        )

    @timeit
    async def get_list(
        self,
        tenant_id: str,
        model_name: str,
        names: Optional[list[str]] = None,
        pagination: Optional[Pagination] = None,
    ) -> list[MeasureModel]:
        """
            Получить список измерений.
        Args:
            tenant_id (str): Идентификатор клиента.
            model_name (str): Имя модели.
            names (list[str]): Список имён измерений.
            pagination (Pagination): Параметры пагинации.
        Returns:
            list[MeasureModel]: Список измерений.
        """
        if names is None:
            query = select(Measure).where(
                Measure.models.any(Model.name == model_name),
                Measure.tenant_id == tenant_id,
            )
        else:
            query = select(Measure).where(
                Measure.models.any(Model.name == model_name),
                Measure.tenant_id == tenant_id,
                Measure.name.in_(names),
            )
        query = get_select_query_with_offset_limit_order(query, Measure.name, pagination)
        result = (await self.session.execute(query)).scalars().all()
        return [MeasureModel.model_validate(measure) for measure in result]

    @timeit
    async def delete_by_name(self, tenant_id: str, model_name: str, name: str) -> None:
        """Удалить показатель по имени."""
        result = await self._get_measure_orm_model_by_session(tenant_id, name, model_name)
        if result is None:
            raise NoResultFound(
                f"Measure with tenant_id={tenant_id}, model_name={model_name} and name={name} not found."
            )
        if len(result.models) > 1:
            await self.measure_history_repository.save_history(measure=result, forced=True)
            result.models = list(filter(lambda model: model.name != model_name, result.models))
            await self.measure_history_repository.update_version(result)
        else:
            await self.measure_history_repository.save_history(measure=result, deleted=True)
            await self.session.delete(result)
        await self.session.commit()

    @timeit
    async def create_by_schema(
        self,
        tenant_id: str,
        model_name: str,
        measure: MeasureCreateRequestModel,
    ) -> MeasureModel:
        """Создать показатель."""
        measure_dict = measure.model_dump(mode="json")
        measure_dict["tenant_id"] = tenant_id
        add_missing_labels(measure_dict["labels"], measure.name)
        measure_dict["labels"] = convert_labels_list_to_orm(measure_dict["labels"], MeasureLabel)
        model_names = [model_name]
        self._prepare_unit_of_measure(measure_dict)
        unit_of_measure = measure_dict.pop("dimension_id", None)
        models = await self.model_repository.get_list_orm_by_names_and_session(tenant_id=tenant_id, names=model_names)
        measure_dict["filter"] = await self._convert_filter_list_to_orm(
            tenant_id=tenant_id, model_names=model_names, filters=measure_dict["filter"]
        )
        measure_orm = Measure(**measure_dict)
        if unit_of_measure:
            measure_orm.dimension = await get_dimension_orm_model_by_session(
                self.session, tenant_id=tenant_id, name=unit_of_measure, model_names=model_names
            )
        measure_orm.models = models
        self.session.add(measure_orm)
        await self.session.flush()
        await self.set_owner_model([measure_orm], models[0])
        await self.measure_history_repository.update_version(measure_orm, create=True)
        await self.session.commit()
        returned_measure = await self._get_measure_orm_model_by_session(
            tenant_id=tenant_id, model_name=model_name, name=measure_orm.name
        )
        return MeasureModel.model_validate(returned_measure)

    @timeit
    async def update_by_name_and_schema(
        self,
        tenant_id: str,
        model_name: str,
        name: str,
        measure: MeasureEditRequestModel,
    ) -> MeasureModel:
        """Обновить показатель."""
        measure_dict = measure.model_dump(mode="json", exclude_unset=True)
        original_measure: Optional[Measure] = await self._get_measure_orm_model_by_session(
            tenant_id=tenant_id,
            model_name=model_name,
            name=name,
        )
        if not original_measure:
            raise NoResultFound(
                f"Measure with tenant_id={tenant_id}, model_name={model_name} and name={name} not found."
            )
        await self.measure_history_repository.save_history(measure=original_measure, edit_model=measure_dict)
        model_names = [
            model_status.name for model_status in MeasureModel.model_validate(original_measure).models_statuses
        ]
        if measure_dict.get("labels") is not None:
            add_missing_labels(measure_dict["labels"], name)
            original_measure.labels = convert_labels_list_to_orm(
                measure_dict.pop("labels"),
                MeasureLabel,
            )
        if "unit_of_measure" in measure_dict:
            self._prepare_unit_of_measure(measure_dict)
            unit_of_measure = measure_dict.pop("dimension_id", None)
            if unit_of_measure:
                original_measure.dimension = await get_dimension_orm_model_by_session(
                    self.session, tenant_id=tenant_id, name=unit_of_measure, model_names=model_names
                )
            else:
                original_measure.dimension = None
        if measure_dict.get("filter") is not None:
            original_measure.filter = await self._convert_filter_list_to_orm(
                tenant_id=tenant_id, model_names=model_names, filters=measure_dict.pop("filter")
            )
        if measure_dict:
            await self.session.execute(
                update(Measure)
                .where(
                    Measure.name == original_measure.name,
                    Measure.models.any(Model.name == model_name),
                    Measure.tenant_id == tenant_id,
                )
                .values(measure_dict)
            )
        await self.session.flush()
        await self.measure_history_repository.update_version(original_measure)
        await self.session.commit()
        returned_measure = await self._get_measure_orm_model_by_session(
            tenant_id=tenant_id,
            model_name=model_name,
            name=name,
        )
        return MeasureModel.model_validate(returned_measure)

    async def copy_model_measure_orm_by_session(
        self, tenant_id: str, name: str, model_names: list[str], validate_existing: bool
    ) -> Optional[Measure]:
        """
        Копирует существующую меру (`Measure`) в указанные модели через сессию ORM.

        Args:
            tenant_id (str): Идентификатор арендатора.
            name (str): Имя меры (`measure`), которую нужно скопировать.
            model_names (list[str]): Список названий моделей, куда будет выполнена копия.
            validate_existing (bool): Признак, указывающий, проверять ли существование меры в целевой модели.

        Returns:
            Optional[Measure]: Новая мера после успешной операции копирования либо None, если операция завершилась неудачно.
        """
        measure: Optional[Measure] = await self._get_measure_orm_model_by_session(
            tenant_id=tenant_id,
            name=name,
        )
        if not measure:
            return None
        measure_model = MeasureModel.model_validate(measure)
        measure_dict = measure_model.model_dump(mode="json")
        if measure_model.unit_of_measure:
            unit_name = (
                measure_model.unit_of_measure
                if isinstance(measure_model.unit_of_measure, str)
                else measure_model.unit_of_measure.dimension_name
            )
            _ = await get_dimension_orm_model_by_session(
                session=self.session, tenant_id=tenant_id, name=unit_name, model_names=model_names
            )
        if measure_model.filter:
            for filter in measure_dict["filter"]:
                filter["dimension_id"] = filter.pop("dimension_name")
            _ = await self._convert_filter_list_to_orm(tenant_id, model_names, measure_dict["filter"])
        measure_model_names = {
            model_status.name for model_status in MeasureModel.model_validate(measure).models_statuses
        }
        destination_model_names = set(model_names)
        model_names_where_measure_not_exists = set(destination_model_names) - measure_model_names

        if validate_existing and model_names_where_measure_not_exists != destination_model_names:
            raise ValueError(
                f"Measure with name={name} exists in Models with names={destination_model_names & measure_model_names}. "
            )
        elif not validate_existing and model_names_where_measure_not_exists != destination_model_names:
            logger.info(
                "Measure with name=%snot exists in Models with names=%s. Measure will be copied to all models.",
                name,
                model_names,
            )

        models = await self.model_repository.get_list_orm_by_names_and_session(tenant_id, model_names)
        await self.measure_history_repository.save_history(measure, forced=True)
        measure.models.extend(models)
        await self.measure_history_repository.update_version(measure)
        return measure

    async def copy_list_of_measures(
        self,
        tenant_id: str,
        model_names: list[str],
        measures: list[MeasureModel],
    ) -> tuple[list[Measure], dict[str, str]]:
        """
        Копирует список мер (`Measures`) в указанные модели.

        Args:
            tenant_id (str): Идентификатор арендатора.
            model_names (list[str]): Список названий моделей, куда будут скопированы меры.
            measures (list[MeasureModel]): Список мер, которые подлежат копированию.

        Returns:
            list[Measure]: Список вновь созданных экземпляров мер после завершения операции.
        """
        result = []
        not_copied_measures: dict[str, str] = {}
        for measure in measures:
            try:
                copied_measure = await self.copy_model_measure_orm_by_session(
                    tenant_id, measure.name, model_names, validate_existing=False
                )
            except Exception as exc:
                not_copied_measures[measure.name] = str(exc)
                continue
            if copied_measure:
                result.append(copied_measure)
                await self.session.flush([copied_measure])

        return result, not_copied_measures

    async def copy_model_measure(self, tenant_id: str, name: str, model_names: list[str]) -> MeasureModel:
        """
        Копирует существующую меру (`Measure`) в указанные модели.

        Args:
            tenant_id (str): Идентификатор арендатора.
            name (str): Имя исходной меры (`Measure`), которую нужно скопировать.
            model_names (list[str]): Список названий моделей, куда будет выполнена копия.

        Returns:
            MeasureModel: Скопированная мера после завершения операции.
        """
        measure = await self.copy_model_measure_orm_by_session(tenant_id, name, model_names, validate_existing=True)
        if not measure:
            raise NoResultFound(f"Measure with tenant_id={tenant_id} and name={name} not found.")
        await self.session.flush()
        await self.measure_history_repository.update_version(measure)
        await self.session.commit()
        await self.session.refresh(measure)
        return MeasureModel.model_validate(measure)

    @classmethod
    def get_by_session(cls, session: AsyncSession) -> "MeasureRepository":
        model_repository = ModelRepository.get_by_session(session)
        return cls(
            session,
            model_repository,
        )
