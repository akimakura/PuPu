import pytest

from src.events import dimension
from src.events.dimension import DimensionEventsProcessor
from src.models.dimension import ChangeDictionaryStuctureActionsEnum
from tests.unit_tests.fixtures.dimension import dimension_model_list


class MockProducer:

    async def send(self, topic: str, value: bytes) -> None:
        return None


class MockKafkaConnector:

    def get_producer(self) -> MockProducer:
        return MockProducer()


class TestDimensionEventsProcessor:

    async def test_change_dictionary_structure(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(dimension.settings, "ENABLE_KAFKA", True)
        monkeypatch.setattr(dimension, "kafka_connector", MockKafkaConnector())
        events_processor = DimensionEventsProcessor()
        msg = await events_processor.change_dictionary_structure(
            "test_tenant", "test_model", ChangeDictionaryStuctureActionsEnum.CREATE, dimension_model_list[0]
        )
        assert msg is not None
        assert msg["tenantName"] == "test_tenant"
        assert msg["modelName"] == "test_model"
        assert msg["dimensionName"] == dimension_model_list[0].name
        assert msg["typeAction"] == ChangeDictionaryStuctureActionsEnum.CREATE
        assert msg["datastorageName"] == ["test_dim5_attributes", "test_dim5_texts", "test_dim5_values"]
