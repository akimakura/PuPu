"""
Методы для настройки окружения в контейнере на запуске.
"""

from src.config import settings
from src.utils.logging.setup import setup_logging


async def setup_logging_configs() -> None:
    """
    Инициализация Логгера.
    """
    setup_logging(filename=settings.LOGGING_PATH)
