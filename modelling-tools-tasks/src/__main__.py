"""
Модуль для локального запуска сервиса.
"""

import asyncio
from argparse import ArgumentParser

import uvicorn
from py_common_lib.logger import get_logger
from py_common_lib.starlette_context_plugins.aiohttp_patch import setup_aiohttp_tracing

from src import migrate
from src.config import settings
from src.models.args import ArgsModel
from src.utils.logging.setup import setup_logging

logger = get_logger(__name__)


def parse_args() -> ArgsModel:
    """
    Парсинг аргументов командной строки.

    Returns:
        Namespace: Аргументы командной строки.
    """
    parser = ArgumentParser(description="Запуск миграций или приложения")
    parser.add_argument("-rm", "--run-migrate", action="store_true", help="Запуск миграций")
    parser.add_argument("-t", "--tenant", action="store", help="Имя тенанта")
    parser.add_argument("-m", "--model", action="append", help="Модель для миграции")
    parser.add_argument("-cd", "--composite-delete", action="store_true", help="Удалить все композиты.")
    parser.add_argument("-cc", "--composite-create", action="store_true", help="Включить миграцию композитов")
    parser.add_argument(
        "-du", "--datastorage-update", action="store_true", help="Включить миграцию хранилищ через обновление"
    )
    parser.add_argument(
        "-dc", "--datastorage-create", action="store_true", help="Включить миграцию хранилищ через пересоздание"
    )
    parser.add_argument("-wc", "--with-delete-columns", action="store_true", help="Удалять ли колонки")
    parser.add_argument(
        "-wd", "--with-delete-not-empty", action="store_true", help="Включить пересоздание непустых сущностей."
    )
    parser.add_argument("-rd", "--recreate-dictionry", action="store_true", help="Включить пересоздание словарей.")
    parser.add_argument("-rs", "--raw-sql", action="store_true", help="Включить выполнение raw sql.")
    parser.add_argument(
        "-fc", "--force-composites", action="store_true", help="Обновить все композиты, даже если они не PENDING"
    )
    parser.add_argument(
        "-fds",
        "--force-datastorages",
        action="store_true",
        help="Обновить все хранилища данных, даже если они не PENDING.",
    )
    parser.add_argument(
        "-fdm", "--force-dimensions", action="store_true", help="Обновить все измерения, даже если они не PENDING."
    )
    args = ArgsModel.model_validate(parser.parse_args())
    return args


if __name__ == "__main__":
    setup_logging(filename=settings.LOGGING_PATH)
    setup_aiohttp_tracing()
    args = parse_args()
    if args.run_migrate:
        asyncio.run(migrate.run(args))
    else:
        uvicorn.run(
            "src.main:create_app",
            host=settings.BIND_IP,
            port=settings.BIND_PORT,
            reload=settings.RELOAD,
            log_config=None,  # Отключаем стандартное логирование, чтобы работал логгер из py-common-lib, а не uvicorn.
        )
