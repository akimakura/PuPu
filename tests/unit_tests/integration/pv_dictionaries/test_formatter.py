from contextlib import nullcontext as does_not_raise
from typing import Optional

import pytest

from src.db.data_storage import DataStorage
from src.db.dimension import Dimension
from src.integration.pv_dictionaries.formatter import PVDictionaryFormatter
from src.integration.pv_dictionaries.models import PVAttribute, PVLabels
from tests.unit_tests.fixtures.pv_dictionaries import (
    pv_dictionary_attribute,
    pvd_formatter_expected_get_attributes,
    pvd_formatter_expected_get_attributes_by_data_storage,
    pvd_formatter_expected_set_attributes_and_keys_to_dictionary_xml,
    pvd_formatter_expected_set_dictionary_to_domain_xml,
    pvd_formatter_expected_set_domain_to_model_xml,
    pvd_formatter_expected_set_model_to_document_xml,
)


class TestPVDictionaryFormatter:

    def test_convert_labels_to_pv_labels(self, pv_dimensions: list[Dimension]) -> None:
        formatter = PVDictionaryFormatter(pv_dimensions[0], {})
        pv_labels_dimension = formatter._convert_labels_to_pv_labels(pv_dimensions[0].attributes[0].labels)
        excepted_value = PVLabels(
            ru_short="short_ru",
            ru_long="long_ru",
            en_short="short_en",
            en_long="long_en",
            short="short",
            long="long",
            other="other",
        )

        assert pv_labels_dimension == excepted_value

    def test_get_label_by_dimension(self, pv_dimensions: list[Dimension]) -> None:
        formatter = PVDictionaryFormatter(
            pv_dimensions[0],
            {
                "dictionary_name": "test_dictionary",
            },
        )
        assert formatter._get_label_by_dimension() == "test_short"

    @pytest.mark.parametrize(
        ("index_field", "expected_value", "expected_exception"),
        [
            (0, ("String", 3, None), does_not_raise()),
            (1, ("Double", 12, 1), does_not_raise()),
            (2, ("Reference", 4, None), does_not_raise()),
            (3, ("BigDecimal", 12, 1), does_not_raise()),
            (4, ("", ""), pytest.raises(ValueError)),
        ],
    )
    def test_get_field_params(
        self,
        pv_dimensions: list[Dimension],
        pv_data_storages: list[DataStorage],
        index_field: int,
        expected_value: tuple[str, str],
        expected_exception: pytest.RaisesExc,
    ) -> None:
        formatter = PVDictionaryFormatter(pv_dimensions[0], {})
        field = pv_data_storages[0].fields[index_field]
        with expected_exception:
            assert expected_value == formatter._get_field_params(field)

    @pytest.mark.parametrize(
        ("index_field", "expected_value", "expected_exception", "pv_dictionary"),
        [
            (
                2,
                PVAttribute(
                    name="dsoField2",
                    label="test",
                    is_key=False,
                    type="Reference",
                    key="ID",
                    dictionary="testPv1",
                    pv_attribute_type=None,
                    description=None,
                    precision=4,
                    scale=0,
                    regex="^[А-ЯЁа-яёA-Za-z0-9_#-:;+.,]+$",
                ),
                does_not_raise(),
                {"dictionary_name": "test_dict", "domain_name": "test_name", "domain_label": "test_label"},
            ),
            (5, None, pytest.raises(ValueError), {}),
        ],
    )
    def test_convert_data_storage_field_to_pv_attribute(
        self,
        pv_dimensions: list[Dimension],
        pv_data_storages: list[DataStorage],
        index_field: int,
        pv_dictionary: dict,
        expected_value: Optional[PVAttribute],
        expected_exception: pytest.RaisesExc,
    ) -> None:
        formatter = PVDictionaryFormatter(pv_dimensions[0], pv_dictionary)
        field = pv_data_storages[0].fields[index_field]
        with expected_exception:
            assert expected_value == formatter._convert_data_storage_field_to_pv_attribute(field)

    @pytest.mark.parametrize(
        ("index_data_storage", "expected_value", "pv_dictionary"),
        [
            (
                1,
                pvd_formatter_expected_get_attributes_by_data_storage,
                {"dictionary_name": "testPv2", "domain_name": "test_name", "domain_label": "test_label"},
            ),
            (None, {}, {}),
        ],
    )
    def test_get_attributes_by_data_storage(
        self,
        pv_data_storages: list[DataStorage],
        pv_dimensions: list[Dimension],
        pv_dictionary: dict,
        index_data_storage: int,
        expected_value: dict[str, PVAttribute],
    ) -> None:
        formatter = PVDictionaryFormatter(pv_dimensions[0], pv_dictionary)
        datastorage = pv_data_storages[index_data_storage] if index_data_storage is not None else None
        assert formatter._get_attributes_by_data_storage(datastorage) == expected_value

    def test_get_attributes(
        self,
        pv_dimensions: list[Dimension],
    ) -> None:
        formatter = PVDictionaryFormatter(
            pv_dimensions[0], {"dictionary_name": "testPv", "domain_name": "test_name", "domain_label": "test_label"}
        )
        assert pvd_formatter_expected_get_attributes == formatter._get_attributes()

    def test_get_label_and_description_by_attribute(
        self,
        pv_dimensions: list[Dimension],
    ) -> None:
        formatter = PVDictionaryFormatter(pv_dimensions[0], {})
        assert ("short_ru", "long_ru") == formatter._get_label_and_description_by_attribute(
            pv_dimensions[0].attributes[0]
        )

    def test_set_attributes_and_keys_to_dictionary(
        self,
        pv_dimensions: list[Dimension],
    ) -> None:
        formatter = PVDictionaryFormatter(
            pv_dimensions[0],
            pv_dictionary_attribute,
        )
        dictionary = formatter.root.createElement("Dictionary")
        formatter._set_attributes_and_keys_to_dictionary(dictionary)
        formatter.root.appendChild(dictionary)
        assert (
            formatter.root.toprettyxml(encoding="UTF-8", standalone=True)
            == pvd_formatter_expected_set_attributes_and_keys_to_dictionary_xml
        )

    @pytest.mark.parametrize("domain_name", ["DataModelDF", "ERM", "AnyDomain"])
    def test_set_configurations_to_dictionary_exportable_always_false(
        self,
        pv_dimensions: list[Dimension],
        domain_name: str,
    ) -> None:
        formatter = PVDictionaryFormatter(
            pv_dimensions[0],
            {
                "object_name": "testPv",
                "domain_name": domain_name,
                "domain_label": "test",
            },
        )
        dictionary = formatter.root.createElement("Dictionary")
        formatter._set_configurations_to_dictionary(dictionary)
        formatter.root.appendChild(dictionary)
        xml = formatter.root.toprettyxml(encoding="UTF-8", standalone=True)
        assert b'<Config name="exportable" value="false"/>' in xml

    def test_set_dictionary_to_domain(
        self,
        pv_dimensions: list[Dimension],
    ) -> None:
        formatter = PVDictionaryFormatter(
            pv_dimensions[0],
            pv_dictionary_attribute,
        )
        domain = formatter.root.createElement("Domain")
        formatter._set_dictionary_to_domain(domain)
        formatter.root.appendChild(domain)

        assert (
            formatter.root.toprettyxml(encoding="UTF-8", standalone=True)
            == pvd_formatter_expected_set_dictionary_to_domain_xml
        )

    def test_set_domain_to_model(self, pv_dimensions: list[Dimension]) -> None:
        formatter = PVDictionaryFormatter(
            pv_dimensions[0],
            pv_dictionary_attribute,
        )
        model = formatter.root.createElement("Model")
        formatter._set_domain_to_model(model)
        formatter.root.appendChild(model)
        assert (
            formatter.root.toprettyxml(encoding="UTF-8", standalone=True)
            == pvd_formatter_expected_set_domain_to_model_xml
        )

    def test_set_model_to_document(self, pv_dimensions: list[Dimension]) -> None:
        formatter = PVDictionaryFormatter(
            pv_dimensions[0],
            pv_dictionary_attribute,
        )
        formatter._set_model_to_document()
        assert (
            formatter.root.toprettyxml(encoding="UTF-8", standalone=True)
            == pvd_formatter_expected_set_model_to_document_xml
        )

    def test_create_xml_document(self, pv_dimensions: list[Dimension]) -> None:
        formatter = PVDictionaryFormatter(
            pv_dimensions[0],
            pv_dictionary_attribute,
        )
        root = formatter._create_xml_document()
        assert root.toprettyxml(encoding="UTF-8", standalone=True) == pvd_formatter_expected_set_model_to_document_xml

    def test_get_xml_create_document(self, pv_dimensions: list[Dimension]) -> None:
        formatter = PVDictionaryFormatter(
            pv_dimensions[0],
            pv_dictionary_attribute,
        )
        assert formatter.get_xml_create_document() == pvd_formatter_expected_set_model_to_document_xml
