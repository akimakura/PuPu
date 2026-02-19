import json
from datetime import datetime
from typing import Optional

from py_common_lib.logger import EPMPYLogger

from src.config import settings
from src.events.kafka import kafka_connector
from src.models.dimension import ChangeDictionaryStuctureActionsEnum, Dimension

logger = EPMPYLogger(__name__)


class DimensionEventsProcessor:
    """Обработчик сообщений справочников, посылаемых в брокер."""

    def __init__(self) -> None:
        if settings.ENABLE_KAFKA:
            self.producer = kafka_connector.get_producer()

    async def change_dictionary_structure(
        self, tenant_name: str, model_name: str, type_action: ChangeDictionaryStuctureActionsEnum, dimension: Dimension
    ) -> Optional[dict]:
        """Отправть событие изменения структуры справочника в kafka."""
        if not settings.ENABLE_KAFKA:
            return None
        data_storages = []
        if dimension.attributes_table_name:
            data_storages.append(dimension.attributes_table_name)
        if dimension.text_table_name:
            data_storages.append(dimension.text_table_name)
        if dimension.values_table_name:
            data_storages.append(dimension.values_table_name)
        msg_to_send = {
            "updated": datetime.now().astimezone().isoformat(),
            "tenantName": tenant_name,
            "modelName": model_name,
            "dimensionName": dimension.name,
            "typeAction": type_action,
            "datastorageName": data_storages,
        }
        await self.producer.send(settings.KAFKA_UPDATE_ENTITIES_TOPIC, json.dumps(msg_to_send).encode("utf-8"))
        logger.info(
            "The message has been sent to kafka",
        )
        logger.debug("topic: '%s', msg: %s", settings.KAFKA_UPDATE_ENTITIES_TOPIC, msg_to_send)
        return msg_to_send
