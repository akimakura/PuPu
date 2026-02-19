from src.service.utils import get_updated_fields_object


class TestServiceUtils:

    def test_get_updated_fields_object(self) -> None:
        original_object = {"test": "123"}
        updated_object = {"test": "1234"}
        excepted_object = {"PREV_test": "123", "test": "1234"}
        assert excepted_object == get_updated_fields_object(original_object, updated_object)
