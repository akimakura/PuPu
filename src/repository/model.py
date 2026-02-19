"""
Репозиторий моделей.
"""

from typing import Any, Optional
from uuid import UUID

from py_common_lib.utils import timeit
from sqlalchemy import select
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession
from typing_extensions import Sequence

from src.db.model import Model, ModelLabel
from src.models.model import (
    Model as ModelModel,
    ModelCreateRequest as ModelCreateRequestModel,
    ModelEditRequest as ModelEditRequestModel,
)
from src.models.request_params import Pagination
from src.repository.database import DatabaseRepository
from src.repository.history.model import ModelHistoryRepository
from src.repository.utils import (
    add_missing_labels,
    convert_labels_list_to_orm,
    get_select_query_with_offset_limit_order,
)


class ModelRepository:

    def __init__(
        self,
        session: AsyncSession,
        database_repository: DatabaseRepository,
    ) -> None:
        self.session = session
        self.database_repository = database_repository
        self.model_history_repository = ModelHistoryRepository(session)

    def _convert_labels_list_to_orm(self, labels: list[dict[str, Any]]) -> list[ModelLabel]:
        """Конвертирует список словарей в модель список моделей SQLAlchemy ModelLabel."""
        return [ModelLabel(**label) for label in labels]

    async def get_model_orm(self, tenant_id: str, name: str) -> Optional[Model]:
        """
        Получает модель по её имени и идентификатору арендатора.

        Args:
            tenant_id (str): Идентификатор арендатора, к которому относится модель.
            name (str): Имя модели, по которому она должна быть получена.

        Returns:
            Optional[Model]: Экземпляр модели, если она найдена, иначе None.
        """

        result = (
            (await self.session.execute(select(Model).where(Model.name == name, Model.tenant_id == tenant_id)))
            .scalars()
            .one_or_none()
        )
        return result

    async def get_model_orm_by_session_with_error(self, tenant_id: str, name: str) -> Model:
        """
        Получает модель по её имени и идентификатору арендатора.
        Если модель не найдена, возбуждается исключение.

        Args:
            tenant_id (str): Идентификатор арендатора, к которому относится модель.
            name (str): Имя модели, по которому она должна быть получена.

        Raises:
            NoResultFound: Исключение, если модель с таким именем и идентификатором арендатора не найдена.

        Returns:
            Model: Найденная модель.
        """
        model = await self.get_model_orm(tenant_id=tenant_id, name=name)
        if model:
            return model
        raise NoResultFound(f"""Model with tenant_id={tenant_id} and name={name} not found.""")

    @timeit
    async def get_list(self, tenant_id: str, pagination: Optional[Pagination] = None) -> list[ModelModel]:
        """Получить список всех моделей"""
        query = select(Model).where(Model.tenant_id == tenant_id)
        query = get_select_query_with_offset_limit_order(query, Model.name, pagination)
        result = (await self.session.execute(query)).scalars().all()
        return [ModelModel.model_validate(model) for model in result]

    @timeit
    async def get_list_by_names(self, tenant_id: str, names: list[str]) -> list[Model]:
        """
        Возвращает список моделей по их именам для указанного тенанта.

        Args:
            tenant_id (str): Идентификатор арендатора (тенанта).
            names (list[str]): Список имён моделей, которые необходимо получить.

        Returns:
            list[Model]: Список моделей, соответствующих переданным именам.
                         Если модель не найдена, она не включается в результат.
        """

        query = select(Model).where(Model.tenant_id == tenant_id, Model.name.in_(names))
        return list((await self.session.execute(query)).scalars().all())

    @timeit
    async def get_list_orm_by_names_and_session(
        self, tenant_id: str, names: list[str], pagination: Optional[Pagination] = None
    ) -> Sequence[Model]:
        """Получить список моделей, которые есть в списке"""
        query = select(Model).where(Model.tenant_id == tenant_id, Model.name.in_(names))
        query = get_select_query_with_offset_limit_order(query, Model.name, pagination)
        result = (await self.session.execute(query)).scalars().all()
        return result

    @timeit
    async def get_by_name(self, tenant_id: str, name: str) -> ModelModel:
        """
        Получает модель по её имени и идентификатору арендатора.

        Args:
            tenant_id (str): Идентификатор арендатора, к которому относится модель.
            name (str): Имя модели, по которому она должна быть получена.

        Returns:
            ModelModel: Модель, соответствующая указанным параметрам.
        """
        result = await self.get_model_orm(tenant_id=tenant_id, name=name)
        if result is None:
            raise NoResultFound(f"Model with tenant_id={tenant_id} and name={name} not found.")
        return ModelModel.model_validate(result)

    @timeit
    async def get_id_by_name(self, tenant_id: str, name: str) -> int:
        """
            Получить id модели по её имени.
        Args:
            tenant_id: Идентификатор тенанта.
            name: Имя модели.
        Returns:
            id модели.
        """
        if result := await self.get_model_orm(tenant_id=tenant_id, name=name):
            return result.id
        else:
            raise NoResultFound(f"Model with tenant_id={tenant_id} and name={name} not found.")

    @timeit
    async def delete_by_name(self, tenant_id: str, name: str) -> None:
        """
        Удаляет модель по её имени и идентификатору арендатора.

        Args:
            tenant_id (str): Идентификатор арендатора, к которому относится модель.
            name (str): Имя модели, которую нужно удалить.
        """
        result = await self.get_model_orm(tenant_id=tenant_id, name=name)
        if result is None:
            raise NoResultFound(f"Model with tenant_id={tenant_id} and name={name} not found.")
        await self.model_history_repository.save_history(result, deleted=True)
        await self.session.delete(result)
        await self.session.commit()

    @timeit
    async def create_by_schema(self, tenant_id: str, model: ModelCreateRequestModel) -> ModelModel:
        """
        Создает новую модель на основе переданной схемы.

        Args:
            tenant_id (str): Идентификатор арендатора, к которому привязывается новая модель.
            model (ModelCreateRequestModel): Схема создания модели, содержащая необходимые данные.

        Returns:
            ModelModel: Новая созданная модель.
        """
        model_dict = model.model_dump(mode="json")
        model_dict["tenant_id"] = tenant_id
        if model_dict.get("aor_space_id"):
            model_dict["aor_space_id"] = UUID(model_dict["aor_space_id"])
        labels = model_dict.pop("labels", [])
        add_missing_labels(labels, model.name)
        labels = convert_labels_list_to_orm(labels, ModelLabel)
        model_dict["labels"] = labels
        database = await self.database_repository.get_database_orm_by_session_with_error(
            tenant_id=tenant_id, database_name=model_dict["database_id"]
        )
        model_orm = Model(**model_dict)
        model_orm.database = database
        self.session.add(model_orm)
        await self.session.flush()
        await self.model_history_repository.update_version(model_orm, create=True)
        await self.session.commit()
        returned_model = await self.get_model_orm(tenant_id=tenant_id, name=model_orm.name)
        return ModelModel.model_validate(returned_model)

    @timeit
    async def update_by_schema_and_name(self, tenant_id: str, name: str, model: ModelEditRequestModel) -> ModelModel:
        """
        Обновляет существующую модель по её имени и идентификатору арендатора.

        Args:
            tenant_id (str): Идентификатор арендатора, к которому относится обновляемая модель.
            name (str): Текущее имя модели, которую нужно обновить.
            model (ModelEditRequestModel): Данные для обновления модели.

        Returns:
            ModelModel: Обновлённая модель.
        """

        original_model = await self.get_model_orm(tenant_id=tenant_id, name=name)
        if original_model is None:
            raise NoResultFound(f"Model with tenant_id={tenant_id} and name={name} not found.")
        model_dict = model.model_dump(mode="json", exclude_none=True)
        if model_dict.get("aor_space_id"):
            model_dict["aor_space_id"] = UUID(model_dict["aor_space_id"])
        await self.model_history_repository.save_history(original_model, edit_model=model_dict)
        if model_dict.get("database_id") is not None:
            database = await self.database_repository.get_database_orm_by_session_with_error(
                tenant_id=tenant_id, database_name=model_dict.pop("database_id")
            )
            original_model.database = database
            model_dict["database_id"] = database.id
        if model_dict.get("labels") is not None:
            add_missing_labels(model_dict["labels"], name)
            labels = convert_labels_list_to_orm(model_dict.pop("labels"), ModelLabel)
            original_model.labels = labels
        if model_dict:
            for attribute_name, attribute_value in model_dict.items():
                setattr(original_model, attribute_name, attribute_value)
        await self.model_history_repository.update_version(original_model)
        await self.session.commit()
        returned_model = await self.get_model_orm(tenant_id=tenant_id, name=name)
        return ModelModel.model_validate(returned_model)

    @classmethod
    def get_by_session(cls, session: AsyncSession) -> "ModelRepository":
        database_repository = DatabaseRepository(session)
        return cls(session, database_repository)
