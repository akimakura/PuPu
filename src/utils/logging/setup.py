from logging.config import dictConfig

import yaml

from src.config import settings


def setup_logging(filename: str) -> None:
    with open(filename, "r") as file:
        config = yaml.safe_load(file)

    if webapp_conf := config["loggers"].get("src"):
        webapp_conf["level"] = settings.LOGGING_LEVEL

    dictConfig(config)
