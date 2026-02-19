from src.integration.pv_dictionaries.models import PVAttribute, PVAttributeType
from src.utils.validators import read_file_as_bytes

pvd_formatter_expected_set_attributes_and_keys_to_dictionary_xml = read_file_as_bytes(
    "tests/unit_tests/fixtures/data_xml/pv_dictionaries_set_attributes.xml"
)
pvd_formatter_expected_set_dictionary_to_domain_xml = read_file_as_bytes(
    "tests/unit_tests/fixtures/data_xml/pv_dictionaries_set_dictionaries.xml"
)

pvd_formatter_expected_set_domain_to_model_xml = read_file_as_bytes(
    "tests/unit_tests/fixtures/data_xml/pv_dictionaries_set_domain.xml"
)

pvd_formatter_expected_set_model_to_document_xml = read_file_as_bytes(
    "tests/unit_tests/fixtures/data_xml/pv_dictionaries_set_model.xml"
)


pvd_formatter_expected_get_attributes_by_data_storage = {
    "dso_field1": PVAttribute(
        name="dsoField1",
        label="test",
        is_key=False,
        type="Double",
        key=None,
        dictionary=None,
        description=None,
        precision=12,
        scale=1,
        regex="^[А-ЯЁа-яёA-Za-z0-9_#-:;+.,]+$",
        pv_attribute_type=None,
    ),
    "dso_field2": PVAttribute(
        name="dsoField2",
        label="test",
        is_key=False,
        type="Reference",
        key="ID",
        dictionary="testPv1",
        description=None,
        precision=4,
        scale=0,
        regex="^[А-ЯЁа-яёA-Za-z0-9_#-:;+.,]+$",
        pv_attribute_type=None,
    ),
    "dso_field3": PVAttribute(
        name="dsoField3",
        label="test",
        is_key=False,
        type="BigDecimal",
        key=None,
        dictionary=None,
        description=None,
        precision=12,
        scale=1,
        regex="^[А-ЯЁа-яёA-Za-z0-9_#-:;+.,]+$",
        pv_attribute_type=None,
    ),
    "test_pv": PVAttribute(
        name="testPv",
        label="test",
        is_key=True,
        type="String",
        key=None,
        dictionary=None,
        description=None,
        precision=3,
        scale=0,
        regex="^[А-ЯЁа-яёA-Za-z0-9_#-:;+.,]+$",
        pv_attribute_type=PVAttributeType.DIMENSION_KEY,
    ),
}


pvd_formatter_expected_get_attributes = {
    "dimension_test": PVAttribute(
        name="dimensionTest",
        label="test",
        is_key=True,
        type="Reference",
        key="ID",
        dictionary="dimensionTest",
        description=None,
        precision=3,
        scale=0,
        regex="^[А-ЯЁа-яёA-Za-z0-9_#-:;+.,]+$",
        time_dependency=False,
        pv_attribute_type=PVAttributeType.ATTRIBUTE,
    ),
    "dimension_test2": PVAttribute(
        name="dimensionTest2",
        label="test",
        is_key=True,
        type="Long",
        key=None,
        dictionary=None,
        description=None,
        precision=6,
        scale=0,
        regex="^[А-ЯЁа-яёA-Za-z0-9_#-:;+.,]+$",
        pv_attribute_type=PVAttributeType.ATTRIBUTE,
        time_dependency=False,
    ),
    "dso_field1": PVAttribute(
        name="dsoField1",
        label="test",
        is_key=False,
        type="Double",
        key=None,
        dictionary=None,
        description=None,
        precision=12,
        scale=1,
        regex="^[А-ЯЁа-яёA-Za-z0-9_#-:;+.,]+$",
        pv_attribute_type=PVAttributeType.ATTRIBUTE,
    ),
    "dso_field2": PVAttribute(
        name="dsoField2",
        label="test",
        is_key=False,
        type="BigDecimal",
        key=None,
        dictionary=None,
        description=None,
        precision=3,
        scale=1,
        regex="^[А-ЯЁа-яёA-Za-z0-9_#-:;+.,]+$",
        pv_attribute_type=PVAttributeType.ATTRIBUTE,
        time_dependency=True,
    ),
    "test_pv": PVAttribute(
        name="testPv",
        label="short_test",
        is_key=True,
        type="String",
        key=None,
        dictionary=None,
        description="long_test",
        precision=5,
        scale=0,
        regex="^[А-ЯЁа-яёA-Za-z0-9_#-:;+.,]+$",
        pv_attribute_type=PVAttributeType.DIMENSION_KEY,
        time_dependency=False,
    ),
    "txtlong": PVAttribute(
        name="txtlong",
        label="test",
        is_key=False,
        type="String",
        key=None,
        dictionary=None,
        description=None,
        precision=4,
        scale=1,
        regex="^[А-ЯЁа-яёA-Za-z0-9_#-:;+.,]+$",
        time_dependency=True,
        pv_attribute_type=PVAttributeType.TEXT,
    ),
}

pv_dictionary_attribute = {
    "dictionary_id": 1,
    "dictionary_name": "testPv",
    "domain_name": "ERM",
    "domain_label": "EPM",
}
