"""
Модуль реализует репозиторий для работы с историей тенантов (Tenant).
Обеспечивает функциональность проверки изменений, обновления версий, получения информации из истории и сохранения изменений.
"""

import datetime
from typing import Optional

from py_common_lib.logger import EPMPYLogger
from sqlalchemy import func, select

from src.db.tenant import Tenant
from src.repository.history.base_history import BaseHistoryRepository

logger = EPMPYLogger(__name__)


class TenantHistoryRepository(BaseHistoryRepository):
    """
    Репозиторий для управления историей изменений тенантов (Tenant).
    Расширяет базовый класс BaseHistoryRepository, добавляя специфическую логику для Tenant-объектов.

    Обрабатывает:
    - Проверку изменений в структуре Tenant
    - Обновление версий связанных объектов
    - Получение последней версии из истории
    - Сохранение изменений в историческую таблицу
    """

    def is_modified(self, tenant: Tenant, edit_model: Optional[dict] = None) -> bool:
        """
        Проверяет, были ли внесены изменения в Tenant-объект.

        Args:
            tenant (Tenant): Объект Tenant для проверки
            edit_model (Optional[dict]): Необязательный словарь с данными редактирования

        Returns:
            bool: True, если были изменения, иначе False
        """
        if not edit_model:
            return False
        return edit_model.get("labels") is not None

    async def update_version(
        self,
        tenant: Tenant,
        create: bool = False,
        forced_version: int | None = None,
        forced_timestamp: datetime.datetime | None = None,
        tenant_id: str | None = None,
    ) -> None:
        """
        Обновляет версию Tenant-объекта и всех связанных с ним объектов.

        Args:
            tenant (Tenant): Объект Tenant для обновления
            create (bool): Флаг создания нового объекта (по умолчанию False)
            forced_version (Optiona[int]): Принудительная версия (опционально)
            forced_timestamp(Any): Принудительное время (опционально)
            tenant_id: Идентификатор тенанта
        """
        await self.try_set_version_to_created_obj(tenant, create, tenant.name)
        self.update_obj_version(tenant.labels, tenant.timestamp, tenant.user, tenant.version)  # type: ignore

    async def get_last_version(self, tenant_name: str) -> int:
        """
        Получает последнюю версию Tenant-объекта.

        Args:
            tenant_name (str): Идентификатор тенанта

        Returns:
            int: Номер последней версии (0, если объект не найден)
        """
        TenantHistory = Tenant.__history_mapper__.class_  # type: ignore
        result = await self.session.execute(
            select(func.max(TenantHistory.version)).where(TenantHistory.name == tenant_name)
        )
        version = result.scalar()
        if version is None:
            return 0
        return version

    async def save_history(
        self,
        tenant: Tenant,
        edit_model: Optional[dict] = None,
        deleted: bool = False,
        forced: bool = False,
    ) -> None:
        """
        Сохраняет историческую запись для Tenant-объекта.

        Args:
            tenant (Tenant): Объект Tenant для сохранения
            edit_model (Optional[dict]): Необязательный словарь с данными редактирования
            deleted (bool): Флаг удаления объекта (по умолчанию False)
            forced (bool): Принудительное сохранение (даже без изменений)
            forced_version (Optional[int]): Принудительная версия (опционально)
        """
        if not deleted and not forced and not self.is_modified(tenant, edit_model):
            return None
        logger.info("Tenant %s modified or deleted. Saving history.", tenant.name)
        await self.copy_obj_to_history(tenant.labels, deleted)
        await self.copy_obj_to_history(tenant, deleted)
        self.update_obj_version(tenant, datetime.datetime.now(datetime.UTC), None, tenant.version + 1)  # type: ignore
        await self.session.flush()
        logger.debug("Tenant %s saved with version %s", tenant.name, tenant.version - 1)  # type: ignore
        return None
