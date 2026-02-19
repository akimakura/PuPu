from datetime import UTC, datetime
from typing import Any, Optional

from py_common_lib.logger import EPMPYLogger
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.db import HierarchyMeta
from src.pkg.history_meta.history_meta import obj_attr_changed
from src.repository.hierarchy import HierarchyRepository
from src.repository.history.base_history import BaseHistoryRepository

logger = EPMPYLogger(__name__)


class HierarchyHistoryRepository(BaseHistoryRepository):

    def __init__(self, session: AsyncSession, hierarchy_repo: HierarchyRepository) -> None:
        self.hierarchy_repo = hierarchy_repo
        super().__init__(session)

    def is_modified(self, obj: HierarchyMeta, edit_model: Optional[dict] = None) -> bool:
        """
        Проверяет, были ли внесены изменения в объект иерархии по сравнению с его последней сохранённой версией.

        Метод сравнивает текущее состояние объекта `obj` с предыдущей версией из истории.
        Опционально учитывает изменения в модели, переданные через `edit_model`.

        Args:
            obj (HierarchyMeta): Объект иерархии, изменения в котором необходимо проверить.
            edit_model (Optional[dict]): Словарь с описанием изменений в модели (например, структура полей). Если None — проверяются только данные иерархии.

        Returns:
            bool: True, если объект был изменён относительно последней версии; иначе — False.

        Notes:
            - Сравнение может включать поля: структура узлов, метаданные, временные метки и т.д.
            - Если история отсутствует, метод может вернуть True (считается, что объект новой версии).
        """
        if not edit_model:
            return False

        edited_labels = edit_model.get("labels") is not None
        edited_model_fields = False
        for field_name in list(obj.__class__.__versioned__["check_modified_fields"]):
            edited_model_fields |= obj_attr_changed(obj, field_name, edit_model.get(field_name))
        return edited_labels or edited_model_fields

    async def get_last_version(self, tenant_id: str, name: str) -> int:
        """
        Возвращает номер последней (максимальной) версии иерархии по её имени в рамках указанного арендатора.

        Метод запрашивает хранилище истории и возвращает номер самой свежей доступной версии
        иерархии с заданным именем.

        Args:
            tenant_id (str): ID арендатора, в контексте которого ищется иерархия.
            name (str): Имя иерархии, для которой запрашивается последняя версия.

        Returns:
            int: Номер последней версии иерархии. Если версии отсутствуют, возвращается 0 или минимальное значение, в зависимости от реализации.

        Raises:
            HistoryNotFound: Если иерархия с указанным именем не найдена (опционально, в зависимости от реализации).
        """
        HierarchyHistory = HierarchyMeta.__history_mapper__.class_  # type: ignore
        result = await self.session.execute(
            select(func.max(HierarchyHistory.version)).where(HierarchyHistory.name == name)
        )
        return result.scalar() or 0

    async def update_version(
        self,
        obj: Any,
        create: bool = False,
        forced_version: int | None = None,
        forced_timestamp: datetime | None = None,
        tenant_id: str | None = None,
    ) -> None:
        """
        Обновляет или создает запись версии объекта в системе хранения истории.

        Метод используется для управления версионированием объектов: при создании новой версии
        или обновлении существующей. Поддерживает принудительную установку номера версии и временной метки.

        Args:
            obj (Any): Объект, версия которого должна быть обновлена или сохранена.
            create (bool): Флаг, указывающий, что необходимо создать новую версию. Если False — обновляется существующая. По умолчанию False.
            forced_version (int | None): Принудительный номер версии. Если указан, используется вместо автоматически генерируемого. По умолчанию None.
            forced_timestamp (datetime | None): Принудительная временная метка для версии. Если None — используется текущее время. По умолчанию None.
            tenant_id (str | None): ID арендатора, если требуется явно указать контекст мультитенантности. По умолчанию None.

        Returns:
            None: Метод не возвращает значение, он изменяет состояние хранилища версий.
        """
        if not tenant_id:
            # кидаем ошибку, вместо изменения типизации для поддержания контракта
            raise ValueError("Tenant id is required")

        await self.try_set_version_to_created_obj(
            obj,
            create,
            tenant_id=tenant_id,
            name=obj.name,
            forced_version=forced_version,
            forced_timestamp=forced_timestamp,
        )
        new_version = obj.version if forced_version is None else forced_version  # type: ignore
        new_timestamp = forced_timestamp or obj.timestamp
        self.update_obj_version(obj.labels, new_timestamp, obj.user, new_version)
        self.update_obj_version(
            await self.hierarchy_repo.get_hierarchy_model_relations(obj.id), new_timestamp, obj.user, new_version
        )
        self.update_obj_version(
            await self.hierarchy_repo.get_hierarchy_base_dimension_relations(obj.id),
            new_timestamp,
            obj.user,
            new_version,
        )

    async def save_history(
        self,
        obj: HierarchyMeta,
        edit_model: Optional[dict] = None,
        deleted: bool = False,
        forced: bool = False,
        forced_version: Optional[int] = None,
    ) -> None:
        """
        Сохраняет историческую версию иерархии с возможностью указания дополнительных параметров.

        Метод создает и сохраняет снимок состояния иерархии (obj) в системе хранения истории.
        Поддерживает пометку удаления, принудительное сохранение и установку версии вручную.

        Args:
            obj (HierarchyMeta): Объект иерархии, для которого сохраняется история.
            edit_model (Optional[dict]): Опциональный словарь с описанием изменений модели, если они были. По умолчанию None.
            deleted (bool): Флаг, указывающий, была ли иерархия удалена. Если True — сохраняется как удалённая версия. По умолчанию False.
            forced (bool): Флаг принудительного сохранения, игнорирующий обычные проверки (например, изменений не было). По умолчанию False.
            forced_version (Optional[int]): Принудительная установка номера версии. Если указано, используется вместо автоматически генерируемого. По умолчанию None.

        Returns:
            None: Метод не возвращает значение, он сохраняет данные в хранилище истории.
        """

        if not deleted and not forced and not self.is_modified(obj, edit_model):
            return None
        logger.info("Hierarchy %s modified or deleted. Saving history.", obj.name)
        await self.copy_obj_to_history(await self.hierarchy_repo.get_hierarchy_model_relations(obj.id), deleted=deleted)
        await self.copy_obj_to_history(obj.labels, deleted=deleted)
        await self.copy_obj_to_history(
            await self.hierarchy_repo.get_hierarchy_base_dimension_relations(obj.id), deleted=deleted
        )
        new_version = forced_version or self.get_new_version(obj)
        await self.copy_obj_to_history(obj, deleted=deleted)
        self.update_obj_version(obj, user=None, version=new_version, timestamp=datetime.now(UTC))

        await self.session.flush()
        logger.info("Hierarchy %s saves with version %s", obj.name, new_version)
        return None
