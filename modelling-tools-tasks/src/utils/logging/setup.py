"""
setup_logging
"""

from logging.config import dictConfig

import yaml

from src.config import settings


def setup_logging(filename: str) -> None:
    """Настройка логирования."""

    with open(filename, "r") as file:
        config = yaml.safe_load(file)
    DEBUG_LVL = "DEBUG"

    if settings.EPMPY_LOG_LEVEL == DEBUG_LVL:
        for logger in config.get("loggers", {}).values():
            logger["level"] = DEBUG_LVL
        if root := config.get("root"):
            root["level"] = DEBUG_LVL
    elif "loggers" in config and "src" in config["loggers"]:
        config["loggers"]["src"]["level"] = settings.EPMPY_LOG_LEVEL

    dictConfig(config)
