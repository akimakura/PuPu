from __future__ import annotations

import argparse
import asyncio
from typing import Optional

from src.db.engine import database_connector
from src.repository.data_storage import DataStorageRepository
from src.repository.database_object import DatabaseObjectRepository
from src.repository.model import ModelRepository
from src.service.data_storage import DataStorageService


def _parse_data_storage_names(raw: Optional[str]) -> Optional[list[str]]:
    """Преобразует строку с именами хранилищ в список или возвращает None."""
    if raw is None:
        return None
    names = [name.strip() for name in raw.split(",") if name.strip()]
    return names or None


def _parse_args() -> argparse.Namespace:
    """Читает параметры командной строки для запуска сбора view."""
    parser = argparse.ArgumentParser(
        description="Тестовый запуск сбора view без эндпоинта.",
    )
    parser.add_argument("--tenant", required=True, help="Имя тенанта.")
    parser.add_argument("--model", required=True, help="Имя модели.")
    parser.add_argument(
        "--data-storages",
        default=None,
        help="Список хранилищ через запятую. Если не задан, берутся все.",
    )
    return parser.parse_args()


async def _collect_views(tenant_id: str, model_name: str, data_storage_names: Optional[list[str]]) -> list[int]:
    """Запускает сбор view для модели и возвращает список id созданных объектов."""
    engine, session_maker = await database_connector.get_not_pg_is_in_recovery()
    try:
        async with session_maker() as session:
            model_repository = ModelRepository.get_by_session(session)
            database_object_repository = DatabaseObjectRepository(session)
            data_repository = DataStorageRepository(session, model_repository, database_object_repository)
            service = DataStorageService(
                data_repository=data_repository,
                dimension_repository=None,
                model_relations_repo=None,
                worker_manager_client=None,
                aor_client=None,
                aor_repository=None,
            )
            return await service.collect_views_for_model(
                tenant_id=tenant_id,
                model_name=model_name,
                data_storage_names=data_storage_names,
            )
    finally:
        await engine.dispose()


def main() -> int:
    """Точка входа для запуска скрипта."""
    args = _parse_args()
    data_storage_names = _parse_data_storage_names(args.data_storages)
    ids = asyncio.run(_collect_views(args.tenant, args.model, data_storage_names))
    print(f"Собрано view: {len(ids)}")
    if ids:
        print("ID database_object:", ", ".join(str(item) for item in ids))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
