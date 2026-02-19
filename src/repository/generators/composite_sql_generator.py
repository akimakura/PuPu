from typing import Any, Optional

from src.models.composite import JOIN_OPERATIONS, CompositeFieldRefObjectEnum, CompositeLinkTypeEnum
from src.models.consts import DATASOURCE_FIELD
from src.models.database import DatabaseTypeEnum
from src.models.model import Model as ModelModel
from src.repository.utils import get_database_object_names, get_object_filtred_by_model_name


class CompositeSqlGenerator:
    """Генератор sql выражения для создание Composite."""

    @classmethod
    def _generate_field_name(
        cls,
        datasources_info_dict: dict,
        field: dict,
        model: ModelModel,
        field_datasource_name: Optional[str] = None,
    ) -> str:
        """
        Генерирует SQL-выражение для физического имени поля источника данных.

        Args:
            datasources_info_dict (dict): Словарь с информацией о всех доступных источниках данных.
            field (dict): Метаданные поля, включая имя, datasource_links и sql_name.
            model (ModelModel): Модель.
            field_datasource_name (Optional[str]): Имя конкретного источника данных,
                связанного с полем. Defaults to None.

        Returns:
            str: SQL-выражение в формате 'schema.datasource.field as alias' или NULL as alias.
        """
        # Проверка специального случая для поля DATASOURCE_FIELD.
        # Если поле с именем DATASOURCE_FIELD,
        # то возвращаем "'семантическое имя источника данных' as DATASOURCE_FIELD"
        if field["name"] == DATASOURCE_FIELD:
            return f"'{field_datasource_name}' as {DATASOURCE_FIELD}"

        # Создание словаря datasource_links_dict,
        # где ключом является имя источника данных, а значением - словарь datasource_link
        # (Пример: {"источник1": {"datasource": "источник1", "datasource_field": "поле1"}})
        # Эта структура используется для удобного поиска связи поля с источником данных.
        datasource_links_dict = {}
        for datasource_link in field["datasource_links"]:
            datasource_links_dict[datasource_link["datasource"]] = datasource_link

        # Если поле не связано с источником данных, то возвращаем NULL as алиас.
        if (not field_datasource_name or not datasource_links_dict.get(field_datasource_name)) and field.get(
            "sql_name"
        ):
            return f"NULL as {field['sql_name']}"
        elif not field_datasource_name or not datasource_links_dict.get(field_datasource_name):
            return f"NULL as {field['name']}"

        datasource = datasources_info_dict[field_datasource_name]
        schema_name, datasource_name = cls._get_datasource_schema_name_and_datasource_name(datasource, model)
        # Если связано с источником данных, то  присваиваем field_name физическое название поля в datasource.
        if datasource["type"] != CompositeFieldRefObjectEnum.VIEW:
            field_name = (
                datasource["fields"][datasource_links_dict[field_datasource_name]["datasource_field"]].sql_name
                if datasource["fields"][datasource_links_dict[field_datasource_name]["datasource_field"]].sql_name
                is not None
                else datasource["fields"][datasource_links_dict[field_datasource_name]["datasource_field"]].name
            )
        else:
            field_name = datasource_links_dict[field_datasource_name]["datasource_field"]
        if field.get("sql_name") is None:
            return f"`{schema_name}`.`{datasource_name}`.`{field_name}` as `{field['name']}`"
        return f"`{schema_name}`.`{datasource_name}`.`{field_name}` as `{field['sql_name']}`"

    @classmethod
    def _get_datasource_schema_name_and_datasource_name(cls, datasource: dict, model: ModelModel) -> tuple[str, str]:
        """Возвращает схему и имя источника данных."""
        if datasource["type"] == CompositeFieldRefObjectEnum.COMPOSITE:
            for database_object in datasource["source"].database_objects:
                model_names = [model.name for model in database_object.models]
                if model.name in model_names:
                    datasource_name = database_object.name
                    schema_name = database_object.schema_name
        elif datasource["type"] == CompositeFieldRefObjectEnum.DATASTORAGE:
            database_objects = get_object_filtred_by_model_name(datasource["source"].database_objects, model.name, True)
            database_object_names = get_database_object_names(database_objects)
            datasource_name = database_object_names.table_name
            schema_name = database_object_names.table_schema
            if database_object_names.distributed_name:
                datasource_name = database_object_names.distributed_name
                schema_name = database_object_names.distributed_schema
            elif database_object_names.dictionary_name:
                datasource_name = database_object_names.dictionary_name
                schema_name = database_object_names.dictionary_schema
        elif datasource["type"] == CompositeFieldRefObjectEnum.VIEW:
            datasource_name = datasource["name"]
            schema_name = datasource["schema_name"]
        else:
            raise ValueError("Unknown datasource type")
        return schema_name, datasource_name

    @classmethod
    def _generate_composite_union_sql_expression(
        cls,
        datasources_info_dict: dict,
        fields: list[dict],
        datasources: list[dict],
        model: ModelModel,
    ) -> str:
        """Генерация SQL выражения типа "SELECT .. UNION SELECT ... UNION ..."."""
        sql_expressions = {}
        for datasource in datasources:
            if datasource["name"] not in sql_expressions:
                sql_expressions[datasource["name"]] = "SELECT "
        for field in fields:
            for datasource in datasources:
                field_name = cls._generate_field_name(datasources_info_dict, field, model, datasource["name"])
                sql_expressions[datasource["name"]] += f"{field_name}, "
        for datasource in datasources:
            schema_name, datasource_name = cls._get_datasource_schema_name_and_datasource_name(
                datasources_info_dict[datasource["name"]], model
            )
            sql_expressions[datasource["name"]] = (
                sql_expressions[datasource["name"]][:-2] + F" FROM `{schema_name}`.`{datasource_name}`"
            )
        return " UNION ALL ".join(sql_expressions.values())

    @classmethod
    def _get_field_and_datasource_from_datasources_info(
        cls, datasources_info_dict: dict, field: dict
    ) -> tuple[dict, Any]:
        """Возвращает источник данных и привязанное поле из словаря datasources_info_dict."""
        datasource = datasources_info_dict[field["datasource"]]
        field = datasource["fields"][field["datasource_field"]]
        return datasource, field

    @classmethod
    def _generate_composite_join_sql_expression(
        cls,
        link_type: CompositeLinkTypeEnum,
        datasources_info_dict: dict,
        fields: list[dict],
        datasources: list[dict],
        links_fields: list[dict],
        model: ModelModel,
    ) -> str:
        """Генерация SQL выражения типа "SELECT .. JOIN ... ON ....AND.."""
        sql_expression = "SELECT "
        for field in fields:
            datasource_links = field.get("datasource_links", [{}])
            if not datasource_links:
                datasource_links = [{}]
            datasource_link = datasource_links[0]
            field_name = cls._generate_field_name(
                datasources_info_dict, field, model, datasource_link.get("datasource")
            )
            sql_expression += f"{field_name}, "
        sql_expression = sql_expression[:-2]
        datasource = datasources_info_dict[datasources[0]["name"]]
        schema_name, datasource_name = cls._get_datasource_schema_name_and_datasource_name(datasource, model)
        sql_expression = sql_expression + F" FROM `{schema_name}`.`{datasource_name}`"
        if link_type == CompositeLinkTypeEnum.SELECT:
            return sql_expression
        if len(links_fields) == 0:
            raise ValueError("At least one LinkField must be specified for the JOIN operation")
        global_if_distributed = " "
        join_expressions = ""
        for index, link_field in enumerate(links_fields):
            left_link_field = link_field["left"]
            right_link_field = link_field["right"]
            left_datasource, left_field = cls._get_field_and_datasource_from_datasources_info(
                datasources_info_dict, left_link_field
            )
            right_datasource, right_field = cls._get_field_and_datasource_from_datasources_info(
                datasources_info_dict, right_link_field
            )
            left_field_name = left_field.sql_name if left_field.sql_name else left_field.name
            right_field_name = right_field.sql_name if right_field.sql_name else right_field.name
            left_schema_name, left_datasource_name = cls._get_datasource_schema_name_and_datasource_name(
                left_datasource, model
            )
            right_schema_name, right_datasource_name = cls._get_datasource_schema_name_and_datasource_name(
                right_datasource, model
            )
            left_field_name = f"`{left_schema_name}`.`{left_datasource_name}`.`{left_field_name}`"
            right_field_name = f"`{right_schema_name}`.`{right_datasource_name}`.`{right_field_name}`"

            if index == 0:
                join_expressions += f" `{right_schema_name}`.`{right_datasource_name}` ON"
            join_expressions += f" {left_field_name} = {right_field_name}"
            if len(links_fields) - 1 > index:
                join_expressions += " AND"
            if (
                left_datasource["type"] == CompositeFieldRefObjectEnum.DATASTORAGE
                and right_datasource["type"] == CompositeFieldRefObjectEnum.DATASTORAGE
            ):
                left_database_objects = get_object_filtred_by_model_name(
                    left_datasource["source"].database_objects, model.name, True
                )
                right_database_objects = get_object_filtred_by_model_name(
                    right_datasource["source"].database_objects, model.name, True
                )
                left_database_object_names = get_database_object_names(left_database_objects)
                right_database_object_names = get_database_object_names(right_database_objects)
                if left_database_object_names.distributed_name and right_database_object_names.distributed_name:
                    global_if_distributed = " GLOBAL "
        sql_expression += global_if_distributed + f"{JOIN_OPERATIONS[link_type]}" + join_expressions
        return sql_expression

    @classmethod
    def generate_composite_sql_expression_by_create_parameters(
        cls,
        link_type: CompositeLinkTypeEnum,
        datasources_info_dict: dict,
        fields: list[dict],
        datasources: list[dict],
        links_fields: list[dict],
        model: ModelModel,
    ) -> str:
        """
        Генерирует составное SQL-выражение на основе параметров создания композитной связи.

        Args:
            link_type (CompositeLinkTypeEnum): Тип связи (UNION/JOIN), определяющий логику генерации SQL.
            datasources_info_dict (dict): Словарь с метаданными источников данных.
            fields (list[dict]): Список словарей с описанием полей для включения в выражение.
            datasources (list[dict]): Список источников данных (таблиц или сценариев).
            links_fields (list[dict]): Список словарей с правилами связывания полей (для JOIN).
            model (ModelModel): Модель, содержащая параметры базы данных (тип СУБД и т.д.).

        Returns:
            str: Сгенерированное SQL-выражение в виде строки. Возвращает пустую строку, если
                среди источников присутствует тип CE_SCENARIO.
        """
        for datasource in datasources:
            if datasource["type"] == CompositeFieldRefObjectEnum.CE_SCENARIO:
                return ""
        if link_type == CompositeLinkTypeEnum.UNION:
            sql_expression = cls._generate_composite_union_sql_expression(
                datasources_info_dict, fields, datasources, model
            )
        else:
            sql_expression = cls._generate_composite_join_sql_expression(
                link_type, datasources_info_dict, fields, datasources, links_fields, model
            )
        if model.database and model.database.type in (DatabaseTypeEnum.POSTGRESQL, DatabaseTypeEnum.GREENPLUM):
            sql_expression = sql_expression.replace("`", '"')
        return sql_expression
