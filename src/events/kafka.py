"""Инициализация кафки."""

from ssl import SSLContext
from typing import Optional

from aiokafka import AIOKafkaProducer  # type: ignore

from src.config import settings
from src.utils.cert import get_ssl_context_by_certs

ssl_context = get_ssl_context_by_certs(
    str(settings.KAFKA_PATH_TO_CA_CERT) if settings.KAFKA_PATH_TO_CA_CERT is not None else None,
    str(settings.KAFKA_PATH_TO_CLIENT_CERT) if settings.KAFKA_PATH_TO_CLIENT_CERT is not None else None,
    str(settings.KAFKA_PATH_TO_CLIENT_CERT_KEY) if settings.KAFKA_PATH_TO_CLIENT_CERT_KEY is not None else None,
    settings.KAFKA_CLIENT_CERT_PASSWORD,
)


class KafkaConnector:
    """Класс коннектора к кафке."""

    _instances: dict[str, "KafkaConnector"] = {}

    def __new__(cls, hosts: list[str], ssl_context: Optional[SSLContext], client_id: str) -> "KafkaConnector":
        """Создание экземпляра класса и возврат, если уже создан для данных хостов."""
        host_key = "".join(hosts)
        if cls._instances.get(host_key):
            return cls._instances[host_key]
        instance = super().__new__(cls)
        cls._instances[host_key] = instance
        return instance

    def __init__(self, hosts: list[str], ssl_context: Optional[SSLContext], client_id: str) -> None:
        self.hosts = hosts
        self.ssl_context = ssl_context
        self.client_id = client_id

    def init_producer(self) -> None:
        """Инициализация producer kafka."""
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.hosts, ssl_context=self.ssl_context, client_id=self.client_id
        )

    def get_producer(self) -> AIOKafkaProducer:
        """Получить producer kafka."""
        return self._producer


kafka_connector = KafkaConnector(settings.KAFKA_SERVERS, ssl_context, settings.KAFKA_CLIENT_ID)
