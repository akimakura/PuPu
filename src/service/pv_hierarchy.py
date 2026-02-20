"""
Сервис управления иерархиями в PVD.

Содержит логику создания, обновления и удаления иерархий
во внешней системе PV Dictionaries, а также синхронизацию
данных между Семантическим слоем и PVD.
"""

from typing import Optional

from sqlalchemy.exc import NoResultFound

from src.config import settings
from src.db.dimension import PVDctionary
from src.db.hierarchy import HierarchyMeta, TimeDependencyType
from src.integration.pv_dictionaries.client import ClientPVDictionaries
from src.integration.pv_dictionaries.models import PVHierarchyPayload, PVLabels
from src.models.hierarchy import HierarchyMetaOut, HierarchyPvdCreateRequest
from src.models.label import LabelType
from src.repository.hierarchy import HierarchyRepository
from src.service.dimension import DimensionService
from src.utils.validators import snake_to_camel

_TIME_DEPENDENCY_TYPE_TO_PVD: dict[str | None, str] = {
    TimeDependencyType.WHOLE: "ALLTIMEDEPENDENT",
    TimeDependencyType.NODE: "NODETIMEDEPENDENT",
}
"""Маппинг значений времязависимости Семантического слоя в значения PVD."""


class HierarchyPvdService:
    """Сервис для управления иерархиями в PV Dictionaries."""

    def __init__(
        self,
        hierarchy_repo: HierarchyRepository,
        dimension_service: DimensionService,
    ) -> None:
        self.hierarchy_repo = hierarchy_repo
        self.dimension_service = dimension_service

    @staticmethod
    def _extract_labels_for_pvd(labels: list) -> tuple[Optional[str], Optional[str]]:
        """
        Извлекает displayName и description из меток иерархии для PVD.

        Из списка меток берётся первый SHORT-label как displayName
        и первый LONG-label как description.

        Args:
            labels: Список меток иерархии (ORM-объекты HierarchyLabel).

        Returns:
            Кортеж (display_name, description).
        """
        pv_labels = PVLabels()
        for label in labels:
            lang = getattr(label, "language", None)
            ltype = getattr(label, "type", None)
            text = getattr(label, "text", None)

            if ltype not in (LabelType.SHORT, "SHORT", LabelType.LONG, "LONG"):
                continue

            suffix = "short" if ltype in (LabelType.SHORT, "SHORT") else "long"
            prefix = next((p for p in ("ru", "en") if lang and p in lang), None)
            attr = f"{prefix}_{suffix}" if prefix else suffix
            setattr(pv_labels, attr, text)
        display_name = pv_labels.ru_short or pv_labels.en_short or pv_labels.short or pv_labels.other
        description = pv_labels.ru_long or pv_labels.en_long or pv_labels.long
        return display_name, description

    async def _build_pvd_hierarchy_name(
        self,
        hierarchy_orm: HierarchyMeta,
        pv_request: Optional[HierarchyPvdCreateRequest],
        tenant_id: str,
        model_name: str,
    ) -> str:
        """
        Формирует имя иерархии в PVD.

        Если имя передано в запросе — используется оно.
        Иначе формируется по шаблону: {dictionary_name базового измерения}__{имя иерархии}.

        Args:
            hierarchy_orm: ORM-объект иерархии.
            pv_request: Тело запроса от пользователя (может быть None).
            tenant_id: Идентификатор тенанта.
            model_name: Имя модели.

        Returns:
            Строка — имя иерархии в PVD.

        Raises:
            ValueError: Если базовое измерение не найдено или не имеет PV Dictionary.
        """
        if pv_request and pv_request.name:
            return pv_request.name

        dimensions = await self.hierarchy_repo.get_base_dimension_names_by_hierarchy_id(hierarchy_orm.id)
        base_dimension_name = next((dim_name for dim_name, is_base in dimensions if is_base), None)
        if not base_dimension_name:
            raise ValueError(f"У иерархии {hierarchy_orm.name} не найдено базовое измерение.")

        base_dimension = await self.dimension_service.get_dimension_orm_model(
            tenant_id=tenant_id, model_name=model_name, name=base_dimension_name
        )
        if not base_dimension or not base_dimension.pv_dictionary:
            raise ValueError(
                f"Базовое измерение '{base_dimension_name}' не имеет PV Dictionary. "
                f"Сначала создайте PVD для измерения."
            )
        return f"{base_dimension.pv_dictionary.object_name}__{snake_to_camel(hierarchy_orm.name)}"

    async def _build_dictionary_name_list(
        self, hierarchy_orm: HierarchyMeta, tenant_id: str, model_name: str
    ) -> list[str]:
        """
        Формирует список словарей измерений для payload PVD.

        Использует явный запрос связей hierarchy->dimensions из БД, чтобы избежать
        несинхронизированного состояния ORM-коллекции base_dimensions в рамках одной сессии.
        """
        dimensions = await self.hierarchy_repo.get_base_dimension_names_by_hierarchy_id(hierarchy_orm.id)
        dictionary_name_list: list[str] = []
        for dim_name, _ in dimensions:
            dimension = await self.dimension_service.get_dimension_orm_model(
                tenant_id=tenant_id,
                model_name=model_name,
                name=dim_name,
            )
            if (
                dimension
                and dimension.pv_dictionary
                and dimension.pv_dictionary.object_name
                and dimension.pv_dictionary.object_name not in dictionary_name_list
            ):
                dictionary_name_list.append(dimension.pv_dictionary.object_name)
        return dictionary_name_list

    async def _build_pvd_payload(
        self, hierarchy_orm: HierarchyMeta, tenant_id: str, model_name: str
    ) -> PVHierarchyPayload:
        """
        Формирует payload для отправки в PVD из ORM-объекта иерархии.

        Args:
            hierarchy_orm: ORM-объект иерархии.

        Returns:
            PVHierarchyPayload: Сформированный payload.
        """
        dictionary_name_list = await self._build_dictionary_name_list(
            hierarchy_orm=hierarchy_orm,
            tenant_id=tenant_id,
            model_name=model_name,
        )
        display_name, description = self._extract_labels_for_pvd(hierarchy_orm.labels)

        return PVHierarchyPayload(
            hierarchy_name=hierarchy_orm.pv_dictionary.object_name,  # type: ignore[union-attr]
            display_name=display_name,
            description=description,
            is_versioned=hierarchy_orm.is_versioned,
            is_time_dependent=hierarchy_orm.is_time_dependent,
            timedependent_type=_TIME_DEPENDENCY_TYPE_TO_PVD.get(hierarchy_orm.time_dependency_type, "NONE"),
            dictionary_name_list=dictionary_name_list,
        )

    async def create_hierarchy_in_pvd(
        self,
        tenant_id: str,
        model_name: str,
        dimension_name: str,
        hierarchy_name: str,
        pv_request: Optional[HierarchyPvdCreateRequest] = None,
        commit: bool = True,
    ) -> HierarchyMetaOut:
        """
        Создать иерархию в PVD.

        Находит иерархию, формирует имя и payload, отправляет в PVD и сохраняет в БД.

        Args:
            tenant_id: Идентификатор тенанта.
            model_name: Имя модели.
            dimension_name: Имя измерения.
            hierarchy_name: Имя иерархии.
            pv_request: Тело запроса с параметрами PVD (необязательно).
            commit: Фиксация транзакции внутри метода.

        Returns:
            HierarchyMetaOut: Обновлённая иерархия с заполненным pvDictionary.

        Raises:
            ValueError: Если иерархия уже имеет PVD или невозможно сформировать имя.
        """
        hierarchy_orm_list = await self.hierarchy_repo.get_list(
            model_name=model_name,
            dimension_names=[dimension_name],
            hierarchy_names=[hierarchy_name],
            tenant_id=tenant_id,
        )
        if not hierarchy_orm_list:
            raise NoResultFound(f"Иерархия '{hierarchy_name}' не найдена.")
        hierarchy_orm = hierarchy_orm_list[0]

        pvd_name = await self._build_pvd_hierarchy_name(
            hierarchy_orm, pv_request, tenant_id=tenant_id, model_name=model_name
        )
        domain_name = (
            pv_request.domain_name if pv_request and pv_request.domain_name else None
        ) or settings.PV_DICTIONARIES_DEFAULT_DOMAIN_NAME
        domain_label = (
            pv_request.domain_label if pv_request and pv_request.domain_label else None
        ) or settings.PV_DICTIONARIES_DEFAULT_DOMAIN_LABEL

        if hierarchy_orm.pv_dictionary_id is not None and hierarchy_orm.pv_dictionary is not None:
            hierarchy_orm.pv_dictionary.object_name = pvd_name
            hierarchy_orm.pv_dictionary.domain_name = domain_name
            hierarchy_orm.pv_dictionary.domain_label = domain_label
            self.hierarchy_repo.session.add(hierarchy_orm.pv_dictionary)
        else:
            dictionary_name_list = await self._build_dictionary_name_list(
                hierarchy_orm=hierarchy_orm,
                tenant_id=tenant_id,
                model_name=model_name,
            )
            display_name, description = self._extract_labels_for_pvd(hierarchy_orm.labels)

            payload = PVHierarchyPayload(
                hierarchy_name=pvd_name,
                display_name=display_name,
                description=description,
                is_versioned=hierarchy_orm.is_versioned,
                is_time_dependent=hierarchy_orm.is_time_dependent,
                timedependent_type=_TIME_DEPENDENCY_TYPE_TO_PVD.get(hierarchy_orm.time_dependency_type, "NONE"),
                dictionary_name_list=dictionary_name_list,
            )

            client = ClientPVDictionaries()
            await client.create_hierarchy(payload)

            pv_dict_orm = PVDctionary(
                object_id=0,
                object_name=pvd_name,
                object_type="HIERARCHY",
                domain_name=domain_name,
                domain_label=domain_label,
                status="ACTIVE",
            )
            self.hierarchy_repo.session.add(pv_dict_orm)
            await self.hierarchy_repo.session.flush()

            hierarchy_orm.pv_dictionary_id = pv_dict_orm.id
            hierarchy_orm.pv_dictionary = pv_dict_orm
            self.hierarchy_repo.session.add(hierarchy_orm)

        if commit:
            await self.hierarchy_repo.session.commit()
            await self.hierarchy_repo.session.refresh(hierarchy_orm)
        else:
            await self.hierarchy_repo.session.flush()

        enriched = await self._enrich_hierarchy_orm_with_dimension_data(hierarchy_orm)
        return HierarchyMetaOut.model_validate(enriched)

    async def update_hierarchy_in_pvd(
        self,
        tenant_id: str,
        model_name: str,
        dimension_name: str,
        hierarchy_name: str,
        pv_request: Optional[HierarchyPvdCreateRequest] = None,
        commit: bool = True,
    ) -> HierarchyMetaOut:
        """
        Обновить иерархию в PVD.

        Args:
            tenant_id: Идентификатор тенанта.
            model_name: Имя модели.
            dimension_name: Имя измерения.
            hierarchy_name: Имя иерархии.
            pv_request: Тело запроса с параметрами PVD (необязательно).
            commit: Фиксация транзакции внутри метода.

        Returns:
            HierarchyMetaOut: Обновлённая иерархия.

        Raises:
            ValueError: Если иерархия не имеет PV Dictionary.
        """
        hierarchy_orm_list = await self.hierarchy_repo.get_list(
            model_name=model_name,
            dimension_names=[dimension_name],
            hierarchy_names=[hierarchy_name],
            tenant_id=tenant_id,
        )
        if not hierarchy_orm_list:
            raise NoResultFound(f"Иерархия '{hierarchy_name}' не найдена.")
        hierarchy_orm = hierarchy_orm_list[0]

        if hierarchy_orm.pv_dictionary_id is None or hierarchy_orm.pv_dictionary is None:
            return await self.create_hierarchy_in_pvd(
                tenant_id=tenant_id,
                model_name=model_name,
                dimension_name=dimension_name,
                hierarchy_name=hierarchy_name,
                pv_request=pv_request,
                commit=commit,
            )

        payload = await self._build_pvd_payload(
            hierarchy_orm=hierarchy_orm,
            tenant_id=tenant_id,
            model_name=model_name,
        )

        client = ClientPVDictionaries()
        await client.update_hierarchy(hierarchy_orm.pv_dictionary.object_name, payload)

        if pv_request:
            if pv_request.domain_name:
                hierarchy_orm.pv_dictionary.domain_name = pv_request.domain_name
            if pv_request.domain_label:
                hierarchy_orm.pv_dictionary.domain_label = pv_request.domain_label
            self.hierarchy_repo.session.add(hierarchy_orm.pv_dictionary)

        if commit:
            await self.hierarchy_repo.session.commit()
            await self.hierarchy_repo.session.refresh(hierarchy_orm)
        else:
            await self.hierarchy_repo.session.flush()

        enriched = await self._enrich_hierarchy_orm_with_dimension_data(hierarchy_orm)
        return HierarchyMetaOut.model_validate(enriched)

    async def delete_hierarchy_from_pvd(
        self,
        tenant_id: str,
        model_name: str,
        dimension_name: str,
        hierarchy_name: str,
        commit: bool = True,
    ) -> None:
        """
        Удалить иерархию из PVD.

        Удаляет иерархию из PVD и очищает ссылку. Сама иерархия в Семантическом слое не удаляется.

        Args:
            tenant_id: Идентификатор тенанта.
            model_name: Имя модели.
            dimension_name: Имя измерения.
            hierarchy_name: Имя иерархии.
            commit: Фиксация транзакции внутри метода.

        Raises:
            ValueError: Если иерархия не имеет PV Dictionary.
        """
        hierarchy_orm_list = await self.hierarchy_repo.get_list(
            model_name=model_name,
            dimension_names=[dimension_name],
            hierarchy_names=[hierarchy_name],
            tenant_id=tenant_id,
        )
        if not hierarchy_orm_list:
            raise NoResultFound(f"Иерархия '{hierarchy_name}' не найдена.")
        hierarchy_orm = hierarchy_orm_list[0]

        if hierarchy_orm.pv_dictionary_id is None or hierarchy_orm.pv_dictionary is None:
            raise ValueError(f"Иерархия '{hierarchy_name}' не имеет PV Dictionary. Удалять нечего.")

        pvd_name = hierarchy_orm.pv_dictionary.object_name
        client = ClientPVDictionaries()
        await client.delete_hierarchy(pvd_name)

        pv_dict_to_delete = hierarchy_orm.pv_dictionary
        hierarchy_orm.pv_dictionary_id = None
        hierarchy_orm.pv_dictionary = None
        self.hierarchy_repo.session.add(hierarchy_orm)
        await self.hierarchy_repo.session.delete(pv_dict_to_delete)
        if commit:
            await self.hierarchy_repo.session.commit()
        else:
            await self.hierarchy_repo.session.flush()

    async def _enrich_hierarchy_orm_with_dimension_data(self, hierarchy_orm: HierarchyMeta) -> HierarchyMeta:
        """
        Обогащает ORM-объект иерархии данными об измерениях.

        Args:
            hierarchy_orm: ORM-объект иерархии.

        Returns:
            ORM-объект с дополнительными полями base_dimension и additional_dimensions.
        """
        dimensions = await self.hierarchy_repo.get_base_dimension_names_by_hierarchy_id(hierarchy_orm.id)
        hierarchy_orm.base_dimension = next(  # type: ignore[attr-defined]
            (dim_name for dim_name, is_base in dimensions if is_base), None
        )
        hierarchy_orm.additional_dimensions = [  # type: ignore[attr-defined]
            dim_name for dim_name, is_base in dimensions if not is_base
        ]
        return hierarchy_orm

    def __repr__(self) -> str:
        return "HierarchyPvdService"
