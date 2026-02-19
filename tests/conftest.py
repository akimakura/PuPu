import os

pytest_plugins = [
    "tests.unit_tests.fixtures.ai_prompts",
    "tests.unit_tests.fixtures.database",
    "tests.unit_tests.fixtures.model",
    "tests.unit_tests.fixtures.any_field",
    "tests.unit_tests.fixtures.measure",
    "tests.unit_tests.fixtures.dimension",
    "tests.unit_tests.fixtures.data_storage",
    "tests.unit_tests.fixtures.composite",
    "tests.unit_tests.fixtures.tenant",
    "tests.unit_tests.fixtures.composite_sql_generator",
]

os.environ["ENABLE_MASTER_SELECTION"] = "0"
os.environ["PV_DICTIONARIES_URL"] = "http://test.ru/"
os.environ["ENABLE_KAFKA"] = "false"
os.environ["ENABLE_PV_DICTIONARIES"] = "false"
