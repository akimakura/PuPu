"""
Тест для сравнения моделей pydantic и моделей в openapi.
"""

import copy
import json
from typing import Any, Optional

import pytest
import yaml
from pydantic import BaseModel

from src.models.composite import CompositeCreateRequest, CompositeEditRequest, CompositeV0
from src.models.data_storage import DataStorageCreateRequest, DataStorageEditRequest, DataStorageV0
from src.models.database import Database, DatabaseCreateRequest, DatabaseEditRequest
from src.models.database_object import DatabaseObject
from src.models.dimension import DimensionCreateRequest, DimensionEditRequest, DimensionV0
from src.models.hierarchy import HierarchyCreateRequest, HierarchyEditRequest, HierarchyMetaOut
from src.models.measure import MeasureCreateRequest, MeasureEditRequest, MeasureV0
from src.models.model import Model, ModelCreateRequest, ModelEditRequest
from src.models.tenant import Tenant, TenantCreateRequest, TenantEditRequest


@pytest.fixture(scope="session")
def open_api_data() -> dict:
    """Получить openapi описание модели."""
    with open("tests/unit_tests/fixtures/openapi.yaml", "r") as file:
        data = file.read()
        schemas_data = yaml.safe_load(data)
    return schemas_data


class OpenApiParser:
    """
    Класс для преобразования openapi и pydantic к единому формату.
    """

    def __init__(self, open_api_data: dict) -> None:
        self.open_api_data = open_api_data

    def get_last_linked_object(self, open_api_object: dict) -> dict:
        """
        Преобразует ссылку на объект в сам объект:.
        Например:
        {"$ref": "#/compotents/schemas/dimension"} -> {"type": "object", "title": "Dimension", "properties": {}}
        """
        if "$ref" not in open_api_object:
            return copy.deepcopy(open_api_object)
        linked_object_name = self.parse_object_name(open_api_object["$ref"])
        return self.get_last_linked_object(self.open_api_data["components"]["schemas"][linked_object_name])

    def parse_object_name(self, object_name: str) -> str:
        """Преобразует #/compotents/schemas/{object_name} в object_name"""
        if object_name[0] == "#":
            return object_name.split("/")[-1]
        return object_name

    def get_required_by_object_name_or_object(
        self, object_name: Optional[str] = None, open_api_object: Optional[dict] = None
    ) -> list[str]:
        """Возвращает поле required из объекта или имени объекта."""
        if object_name:
            object_name = self.parse_object_name(object_name)
            return self.open_api_data["components"]["schemas"][object_name].get("required", [])
        if open_api_object:
            return self.get_last_linked_object(open_api_object).get("required", [])
        raise ValueError(
            "In the get_required_by_object_name_or_object, the object_name or open_api_object arguments must not be empty."
        )

    def get_properties_by_object_name_or_object(
        self, object_name: Optional[str] = None, open_api_object: Optional[dict] = None
    ) -> dict:
        """Возвращает поле properties из объекта или имени объекта."""
        if object_name:
            object_name = self.parse_object_name(object_name)
            return self.open_api_data["components"]["schemas"][object_name].get("properties", {})
        if open_api_object:
            return self.get_last_linked_object(open_api_object).get("properties", {})
        raise ValueError(
            "In the get_properties_by_object_name_or_object, the object_name or open_api_object arguments must not be empty."
        )

    def concatenate_all_off_enums(self, schema: dict) -> None:
        """Соединяет enum, в один объект, если он разбит на несколько объектов под свойсвом 'allOf'."""
        if "allOf" not in schema:
            return None
        total_enum = set()
        count_enums = 0
        for _, all_of_element in enumerate(schema["allOf"]):
            if "enum" in all_of_element:
                total_enum.update(all_of_element["enum"])
                count_enums += 1
        if count_enums == len(schema["allOf"]) and count_enums != 0:
            schema["allOf"][0]["enum"] = list(total_enum)
            schema.update(schema.pop("allOf")[0])
        return None

    def get_concatenate_all_off_object(self, schema: dict) -> dict:
        """Получить соединенный allOf объект."""
        result_object: dict = {}
        for _, all_of_element in enumerate(schema["allOf"]):
            if "$ref" in all_of_element:
                all_of_element = self.get_last_linked_object(all_of_element)
            if not result_object:
                result_object = all_of_element
            else:
                result_object["properties"].update(all_of_element["properties"])
        return result_object

    def parse_properties(self, properties: dict) -> None:
        """
        Рекурсивно преобразовать поле properties к формату подходящему для сравнения объектов.
        Конвертирует все ссылки в конкретные объекты, а также чистит ненужные allOf, oneOf и т.д.
        """
        type_null_dict = {"type": "null"}
        for property_name, property_value in properties.items():
            if "$ref" in property_value:
                linked_property = self.get_last_linked_object(property_value)
                properties[property_name] = linked_property
            if "items" in property_value and "$ref" in property_value["items"]:
                linked_items = self.get_last_linked_object(property_value["items"])
                properties[property_name]["items"] = linked_items
            for operator in ["oneOf", "allOf", "anyOf"]:
                if operator in property_value:
                    for index, element in enumerate(property_value[operator]):
                        linked_one_of = self.get_last_linked_object(element)
                        property_value[operator][index] = linked_one_of
                        if element.get("items") and element["items"].get("$ref"):
                            linked_one_of = self.get_last_linked_object(element["items"])
                            property_value[operator][index]["items"] = linked_one_of
                    if len(property_value[operator]) == 1 or (
                        len(property_value[operator]) == 2 and property_value[operator][1] == type_null_dict
                    ):
                        property_value.update(property_value.pop(operator)[0])
            if properties[property_name].get("items") and properties[property_name]["items"].get("properties"):
                self.parse_properties(properties[property_name]["items"]["properties"])
            if properties[property_name].get("properties"):
                self.parse_properties(properties[property_name]["properties"])
            for operator in ["oneOf", "allOf", "anyOf"]:
                if properties[property_name].get(operator):
                    for _, element in enumerate(property_value[operator]):
                        if element.get("properties"):
                            self.parse_properties(element["properties"])
                        if element.get("items") and element["items"].get("properties"):
                            self.parse_properties(element["items"]["properties"])
                    if len(property_value[operator]) == 1 or (
                        len(property_value[operator]) == 2 and property_value[operator][1] == type_null_dict
                    ):
                        property_value.update(property_value.pop(operator)[0])
                    else:
                        property_value[operator].sort(
                            key=lambda x: len(x.get("properties", {"type": "string"})), reverse=True
                        )
            self.concatenate_all_off_enums(properties[property_name])
            if properties[property_name].get("items") and properties[property_name]["items"].get("allOf"):
                properties[property_name]["items"] = self.get_concatenate_all_off_object(
                    properties[property_name]["items"]
                )
                self.parse_properties(properties[property_name]["items"]["properties"])

    def parse_object_by_object_name(self, object_name: str) -> dict:
        """
        Преобразовать объект к формату подходящему для сравнения.
        Конвертирует все ссылки в конкретные объекты, а также чистит ненужные allOf, oneOf и т.д.
        """
        result_object: dict[str, Any] = {"type": "object", "required": [], "properties": {}}
        original_object = self.open_api_data["components"]["schemas"][object_name]
        properties = self.get_properties_by_object_name_or_object(object_name=object_name)
        required = self.get_required_by_object_name_or_object(object_name=object_name)
        all_off_objects = original_object.get("allOf", [])
        for all_off_object in all_off_objects:
            result_object["required"].extend(self.get_required_by_object_name_or_object(open_api_object=all_off_object))
            result_object["properties"].update(
                self.get_properties_by_object_name_or_object(open_api_object=all_off_object)
            )
        result_object["required"].extend(required)
        result_object["properties"].update(properties)
        self.parse_properties(result_object["properties"])
        result_object["required"].sort()
        return result_object


class PydanticValidator:
    """Класс для сравнения OpenApi и Pydantic."""

    def __init__(self, open_api_data: dict) -> None:
        self.open_api_data = open_api_data

    def _parse_object_by_object_name_in_openapi(self, object_name: str) -> dict:
        """Возвращает преоброазованный openapi объект."""
        parser = OpenApiParser(self.open_api_data)
        return parser.parse_object_by_object_name(object_name)

    def _parse_object_by_pydantic_model(self, model: type[BaseModel]) -> dict:
        """Возвращает преоброазованный pydantic объект."""
        pydantic_open_api = model.model_json_schema(
            by_alias=True, ref_template="#/components/schemas/{model}", mode="serialization"
        )
        schemas = pydantic_open_api.pop("$defs")
        new_schemas = {"components": {"schemas": {model.__qualname__: pydantic_open_api}}}
        new_schemas["components"]["schemas"].update(schemas)
        new_schemas_str = json.dumps(new_schemas)
        new_schemas_str = new_schemas_str.replace("\"anyOf\"", "\"oneOf\"")
        new_schemas = json.loads(new_schemas_str)
        parser = OpenApiParser(new_schemas)
        return parser.parse_object_by_object_name(model.__qualname__)

    def _compare_schemas(
        self, schema1: dict, schema2: dict, black_list_fields: Optional[list] = None, label: str = ""
    ) -> bool:
        """Рекурсивно сравнивает все поля передаваемых схем."""
        if black_list_fields is None:
            black_list_fields = []
        items1 = schema1.get("items")
        items2 = schema2.get("items")
        assert not bool(items1) ^ bool(items2)
        properties1 = schema1.get("properties") if items1 is None else items1.get("properties")
        properties2 = schema2.get("properties") if items2 is None else items2.get("properties")
        assert not bool(properties1) ^ bool(properties2)
        if properties1 is None:
            if ("oneOf" not in schema1) or ("oneOf" not in schema2):
                return True
            for index, schema1_value in enumerate(schema1["oneOf"]):
                if index < len(schema2["oneOf"]) and index < len(schema1["oneOf"]):
                    assert self._compare_schemas(schema1_value, schema2["oneOf"][index], black_list_fields, label)
        else:
            if properties2 is None:
                properties2 = {}
            for property1_key, property1_value in properties1.items():
                if property1_key in black_list_fields:
                    continue
                assert property1_key in properties2, f"{property1_key} not found in {properties2}"
                property2_value = properties2[property1_key]
                assert property2_value.get("maxLength") == property1_value.get(
                    "maxLength"
                ), f"{label} {property1_key} error maxLength. {property2_value.get('maxLength')} != {property1_value.get('maxLength')}"
                assert property2_value.get("minLength") == property1_value.get(
                    "minLength"
                ), f"{label} {property1_key} error minLength. {property2_value.get('minLength')} != {property1_value.get('minLength')}"
                assert property2_value.get("type") == property1_value.get(
                    "type"
                ), f"{label} {property1_key} error type. {property2_value.get('type')} != {property1_value.get('type')}"
                pattern1 = property1_value.get("pattern")
                pattern2 = property2_value.get("pattern")
                if pattern1 and pattern2:
                    pattern1 = pattern1.replace("\\n", "\n").replace("\\t", "\t")
                    pattern2 = pattern2.replace("\\n", "\n").replace("\\t", "\t")
                assert pattern1 == pattern2, f"{label} {property1_key} error pattern. {pattern1} != {pattern2}"
                assert property2_value.get("maxItems") == property1_value.get(
                    "maxItems"
                ), f"{label} {property1_key} error maxItems. {property2_value.get('maxItems')} != {property1_value.get('maxItems')}"
                assert property2_value.get("minItems") == property1_value.get(
                    "minItems"
                ), f"{label} {property1_key} error minItems. {property2_value.get('minItems')} != {property1_value.get('minItems')}"
                assert property2_value.get("minimum") == property1_value.get(
                    "minimum"
                ), f"{label} {property1_key} error minimum. {property2_value.get('minimum')} != {property1_value.get('minimum')}"
                assert property2_value.get("maximum") == property1_value.get(
                    "maximum"
                ), f"{label} {property1_key} error maximum. {property2_value.get('maximum')} != {property1_value.get('maximum')}"
                enum1 = property2_value.get("enum")
                enum2 = property1_value.get("enum")
                if enum1 and enum2:
                    enum1.sort()
                    enum2.sort()
                assert (
                    enum1 == enum2
                ), f"{label} {property1_key} error enum. {enum1} != {enum2}, {property1_value}, {property2_value}"
                assert self._compare_schemas(property1_value, property2_value, black_list_fields, label)

        return True

    def compare_schemas(
        self, object_name: str, pydantic_model: type[BaseModel], black_list_fields: Optional[list] = None
    ) -> bool:
        """Сравнить openapi схему и pydantic модель."""
        data_storage_openapi = self._parse_object_by_object_name_in_openapi(object_name)
        data_storage_pydantic = self._parse_object_by_pydantic_model(pydantic_model)
        assert self._compare_schemas(
            data_storage_openapi, data_storage_pydantic, black_list_fields, label="openapi/pydantic"
        )
        assert self._compare_schemas(
            data_storage_pydantic, data_storage_openapi, black_list_fields, label="pydantic/openapi"
        )
        return True


class TestOpenAPI:

    @pytest.mark.parametrize(
        ("openapi_schema_name", "pydantic_class", "black_list_fields"),
        [
            ("tenant", Tenant, []),
            ("tenant", TenantCreateRequest, ["updatedAt", "aorType", "updatedBy", "version"]),
            ("tenantEdit", TenantEditRequest, ["updatedAt", "aorType", "updatedBy", "version"]),
            ("database", Database, []),
            ("database", DatabaseCreateRequest, ["updatedAt", "aorType", "updatedBy", "version"]),
            ("databaseEdit", DatabaseEditRequest, ["updatedAt", "aorType", "updatedBy", "version"]),
            ("model", Model, []),
            ("model", ModelCreateRequest, ["updatedAt", "aorType", "updatedBy", "version"]),
            ("modelEdit", ModelEditRequest, ["updatedAt", "aorType", "updatedBy", "version"]),
            (
                "dimension",
                DimensionV0,
                [
                    "aiPrompt",
                ],
            ),
            (
                "dimension",
                DimensionCreateRequest,
                [
                    "aorType",
                    "models",
                    "hierarchyVersionsTable",
                    "hierarchyTextVersionsTable",
                    "hierarchyNodesTable",
                    "hierarchyTextNodesTable",
                    "haveHierarchy",
                    "updatedAt",
                    "updatedBy",
                    "version",
                ],
            ),
            (
                "dimensionEdit",
                DimensionEditRequest,
                [
                    "aorType",
                    "hierarchyVersionsTable",
                    "hierarchyTextVersionsTable",
                    "hierarchyNodesTable",
                    "hierarchyTextNodesTable",
                    "haveHierarchy",
                    "updatedAt",
                    "updatedBy",
                    "version",
                ],
            ),
            ("hierarchy", HierarchyMetaOut, ["pvDictionary", "version"]),
            ("hierarchyEdit", HierarchyEditRequest, ["pvDictionary", "aorType", "updatedAt", "updatedBy", "version"]),
            (
                "hierarchy",
                HierarchyCreateRequest,
                [
                    "pvDictionary",
                    "models",
                    "aorType",
                    "baseDimension",
                    "dataStorages",
                    "updatedAt",
                    "updatedBy",
                    "version",
                ],
            ),
            ("measure", MeasureV0, ["measureRef"]),
            ("measure", MeasureCreateRequest, ["models", "aorType", "measureRef", "updatedAt", "updatedBy", "version"]),
            ("measureEdit", MeasureEditRequest, ["measureRef", "aorType", "updatedAt", "updatedBy", "version"]),
            ("dataStorage", DataStorageV0, ["isTechField"]),
            (
                "dataStorage",
                DataStorageCreateRequest,
                ["models", "sqlColumnType", "aorType", "isTechField", "updatedAt", "updatedBy", "version"],
            ),
            (
                "dataStorageEdit",
                DataStorageEditRequest,
                ["sqlColumnType", "aorType", "isTechField", "updatedAt", "updatedBy", "version"],
            ),
            ("compositeGet", CompositeV0, []),
            (
                "composite",
                CompositeCreateRequest,
                ["sqlColumnType", "aorType", "dbObjects", "schemaName", "updatedAt", "updatedBy", "version"],
            ),
            (
                "compositeEdit",
                CompositeEditRequest,
                ["sqlColumnType", "aorType", "dbObjects", "schemaName", "updatedAt", "updatedBy", "version"],
            ),
            ("dbObject", DatabaseObject, []),
        ],
    )
    def test_swagger_schema(
        self, openapi_schema_name: str, pydantic_class: Any, black_list_fields: list, open_api_data: dict
    ) -> None:
        parser = PydanticValidator(open_api_data)
        assert parser.compare_schemas(openapi_schema_name, pydantic_class, black_list_fields)
