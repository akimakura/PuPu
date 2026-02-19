from typing import Any, Optional

from src.config import settings
from src.integrations.modelling_tools_api.codegen import (
    CompositeGet as Composite,
    Datasource,
    DataStorage,
    V1Api,
)
from src.models.composite import CompositeFieldRefObjectEnum, CompositeLinkTypeEnum
from src.models.consts import DATASOURCE_FIELD
from src.models.database import DatabaseTypeEnum
from src.repository.utils import get_database_object_names
from src.utils.text_exceptions import DATASTORAGE_NOT_FOUND
from src.utils.validators import get_not_null_or_raise

JOIN_OPERATIONS: dict[CompositeLinkTypeEnum, str] = {
    CompositeLinkTypeEnum.INNER_JOIN: "INNER JOIN",
    CompositeLinkTypeEnum.LEFT_JOIN: "LEFT JOIN",
}


class CompositeSQLGenerator:
    def __init__(self, model_name: str, tenant_id: str, mt_api_v1_client: V1Api, database_type: DatabaseTypeEnum):
        self.model_name = model_name
        self.tenant_id = tenant_id
        self.mt_api_v1_client = mt_api_v1_client
        self.database_type = database_type

    async def _update_datasources_field_dict_by_composite(
        self, result_datasources: dict[str, dict[str, Any]], datasource: Datasource
    ) -> None:
        """
        Обновляет словарь источников данных (`result_datasources`) на основе переданного объекта `datasource` типа COMPOSITE.

        Args:
            result_datasources (dict): Словарь источников данных, где ключ — имя источника, значение — объект типа dict с полями.
            datasource (Datasource): Объект-источник данных с информацией о полях, типе, имени и схеме базы данных.

        Exceptions:
            ValueError: Если источник данных имеет пустое название или схема отсутствует у типа 'VIEW'.

        Notes:
            Метод добавляет/обновляет элемент в результативный словарь, проверяя обязательность названия и схемы.
        """
        datasource_name: str = get_not_null_or_raise(datasource.name, log_attr_name="name", log_obj_name="datasource")
        composite = await self.mt_api_v1_client.get_composite_by_model_name_and_composite_name(
            tenant_name=self.tenant_id,
            model_name=self.model_name,
            composite_name=datasource_name,
            _request_timeout=settings.MT_API_TIMEOUT,
        )
        if composite is None:
            raise ValueError(
                f"Composite with tenant_id={self.tenant_id}, model_name={self.model_name} and name={datasource_name} not found."
            )
        result_datasources[datasource_name] = {
            "fields": {},
            "source": composite,
            "type": CompositeFieldRefObjectEnum.COMPOSITE,
        }
        for composite_field in composite.fields:
            result_datasources[datasource_name]["fields"][composite_field.name] = composite_field

    async def _update_datasources_field_dict_by_datastorage(
        self, result_datasources: dict[str, dict[str, Any]], datasource: Datasource
    ) -> None:
        """
        Обновляет словарь источников данных (`result_datasources`) на основе переданного объекта `datasource` типа DATASTORAGE.

        Args:
            result_datasources (dict): Словарь источников данных, где ключ — имя источника, значение — объект типа dict с полями.
            datasource (Datasource): Объект-источник данных с информацией о полях, типе, имени и схеме базы данных.

        Exceptions:
            ValueError: Если источник данных имеет пустое название или схема отсутствует у типа 'VIEW'.

        Notes:
            Метод добавляет/обновляет элемент в результативный словарь, проверяя обязательность названия и схемы.
        """
        datasource_name: str = get_not_null_or_raise(datasource.name, log_attr_name="name", log_obj_name="datasource")
        datastorage: DataStorage = await self.mt_api_v1_client.get_data_storage_by_model_name_and_data_storage_name(
            tenant_name=self.tenant_id,
            model_name=self.model_name,
            data_storage_name=datasource_name,
            _request_timeout=settings.MT_API_TIMEOUT,
        )
        datastorage = get_not_null_or_raise(
            datastorage,
            custom_raise_text=DATASTORAGE_NOT_FOUND.format(self.tenant_id, self.model_name, datasource_name),
        )
        result_datasources[datasource_name] = {
            "fields": {},
            "type": CompositeFieldRefObjectEnum.DATASTORAGE,
            "source": datastorage,
        }
        for dso_field in datastorage.fields:
            result_datasources[datasource_name]["fields"][dso_field.name] = dso_field

    async def _update_datasources_field_dict_by_view_or_scenario(
        self, result_datasources: dict[str, dict[str, Any]], datasource: Datasource
    ) -> None:
        """
        Обновляет словарь источников данных (`result_datasources`) на основе переданного объекта `datasource` типа VIEW или SCENARIO.

        Args:
            result_datasources (dict): Словарь источников данных, где ключ — имя источника, значение — объект типа dict с полями.
            datasource (Datasource): Объект-источник данных с информацией о полях, типе, имени и схеме базы данных.

        Exceptions:
            ValueError: Если источник данных имеет пустое название или схема отсутствует у типа 'VIEW'.

        Notes:
            Метод добавляет/обновляет элемент в результативный словарь, проверяя обязательность названия и схемы.
        """
        datasource_schema_name: str | None = datasource.schema_name
        datasource_name: str = get_not_null_or_raise(datasource.name, log_attr_name="name", log_obj_name="datasource")
        if datasource.type == CompositeFieldRefObjectEnum.VIEW:
            datasource_schema_name = get_not_null_or_raise(
                datasource.schema_name, log_attr_name="schema_name", log_obj_name="datasource"
            )
        result_datasources[datasource_name] = {
            "fields": {},
            "type": datasource.type,
            "name": datasource_name,
            "schema_name": datasource_schema_name,
        }

    async def _get_datasources_field_dict(self, datasources: list[Datasource]) -> dict[str, dict[str, Any]]:
        """
        Формирует словарь (map), содержащий поля DataSource модели Pydantic.

        Структура необходима для единоразового запроса всех объектов из базы данных.
        Метод обрабатывает различные типы источников данных (`Composite`, `DataStorage`,
        `VIEW` или `CE_SCENARIO`) и формирует итоговый словарь для дальнейшего использования.

        Args:
            datasources (list[Datasource]): Список объектов Datasource разных типов.

        Returns:
            dict[str, dict[str, Any]]: Словарь с полями каждого источника данных,
                                      сгруппированными по типу объекта.

        Example:
            {
                "datasource1": {
                    "type": "COMPOSITE",
                    "source": Composite(),
                    "fields" : {
                        "field1": CompositeField()
                    }
                },
                ...
            }
        """
        result_datasources: dict[str, dict[str, Any]] = {}
        for datasource in datasources:
            if datasource.type == CompositeFieldRefObjectEnum.COMPOSITE:
                await self._update_datasources_field_dict_by_composite(result_datasources, datasource)
            elif datasource.type == CompositeFieldRefObjectEnum.DATASTORAGE:
                await self._update_datasources_field_dict_by_datastorage(result_datasources, datasource)
            elif datasource.type in (CompositeFieldRefObjectEnum.VIEW, CompositeFieldRefObjectEnum.CE_SCENARIO):
                await self._update_datasources_field_dict_by_view_or_scenario(result_datasources, datasource)
        return result_datasources

    def _generate_field_name(
        self,
        datasources_info_dict: dict[str, Any],
        field: dict[str, Any],
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
        schema_name, datasource_name = self._get_datasource_schema_name_and_datasource_name(datasource)
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

    def _get_datasource_schema_name_and_datasource_name(self, datasource: dict[str, Any]) -> tuple[str, str]:
        """Возвращает схему и имя источника данных."""
        if datasource["type"] == CompositeFieldRefObjectEnum.COMPOSITE:
            datasource_name = datasource["source"].db_objects[0].name
            schema_name = datasource["source"].db_objects[0].schema_name
        elif datasource["type"] == CompositeFieldRefObjectEnum.DATASTORAGE:
            database_objects = datasource["source"].db_objects
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
        if schema_name is None or datasource_name is None:
            raise ValueError(f"Schema name and datasource name is required for VIEW datasource: {datasource}")
        return schema_name, datasource_name

    def _generate_composite_union_sql_expression(
        self,
        datasources_info_dict: dict[str, Any],
        fields: list[dict[str, Any]],
        datasources: list[dict[str, Any]],
    ) -> str:
        """Генерация SQL выражения типа "SELECT .. UNION SELECT ... UNION ..."."""
        sql_expressions = {}
        for datasource in datasources:
            if datasource["name"] not in sql_expressions:
                sql_expressions[datasource["name"]] = "SELECT "
        for field in fields:
            for datasource in datasources:
                field_name = self._generate_field_name(datasources_info_dict, field, datasource["name"])
                sql_expressions[datasource["name"]] += f"{field_name}, "
        for datasource in datasources:
            schema_name, datasource_name = self._get_datasource_schema_name_and_datasource_name(
                datasources_info_dict[datasource["name"]]
            )
            sql_expressions[datasource["name"]] = (
                sql_expressions[datasource["name"]][:-2] + f" FROM `{schema_name}`.`{datasource_name}`"
            )
        return " UNION ALL ".join(sql_expressions.values())

    def _get_field_and_datasource_from_datasources_info(
        self, datasources_info_dict: dict[str, Any], field: dict[str, Any]
    ) -> tuple[dict[str, Any], Any]:
        """Возвращает источник данных и привязанное поле из словаря datasources_info_dict."""
        datasource = datasources_info_dict[field["datasource"]]
        field = datasource["fields"][field["datasource_field"]]
        return datasource, field

    def _generate_composite_join_sql_expression(
        self,
        link_type: CompositeLinkTypeEnum,
        datasources_info_dict: dict[str, Any],
        fields: list[dict[str, Any]],
        datasources: list[dict[str, Any]],
        links_fields: list[dict[str, Any]],
    ) -> str:
        """Генерация SQL выражения типа "SELECT .. JOIN ... ON ....AND.."""
        sql_expression = "SELECT "
        for field in fields:
            datasource_links = field.get("datasource_links", [{}])
            if not datasource_links:
                datasource_links = [{}]
            datasource_link = datasource_links[0]
            field_name = self._generate_field_name(datasources_info_dict, field, datasource_link.get("datasource"))
            sql_expression += f"{field_name}, "
        sql_expression = sql_expression[:-2]
        datasource = datasources_info_dict[datasources[0]["name"]]
        schema_name, datasource_name = self._get_datasource_schema_name_and_datasource_name(datasource)
        sql_expression = sql_expression + f" FROM `{schema_name}`.`{datasource_name}`"
        if link_type == CompositeLinkTypeEnum.SELECT:
            return sql_expression
        if len(links_fields) == 0:
            raise ValueError("At least one LinkField must be specified for the JOIN operation")
        global_if_distributed = " "
        join_expressions = ""
        for index, link_field in enumerate(links_fields):
            left_link_field = link_field["left"]
            right_link_field = link_field["right"]
            left_datasource, left_field = self._get_field_and_datasource_from_datasources_info(
                datasources_info_dict, left_link_field
            )
            right_datasource, right_field = self._get_field_and_datasource_from_datasources_info(
                datasources_info_dict, right_link_field
            )
            left_field_name = left_field.sql_name if left_field.sql_name else left_field.name
            right_field_name = right_field.sql_name if right_field.sql_name else right_field.name
            left_schema_name, left_datasource_name = self._get_datasource_schema_name_and_datasource_name(
                left_datasource
            )
            right_schema_name, right_datasource_name = self._get_datasource_schema_name_and_datasource_name(
                right_datasource
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
                left_database_objects = left_datasource["source"].db_objects
                right_database_objects = right_datasource["source"].db_objects
                left_database_object_names = get_database_object_names(left_database_objects)
                right_database_object_names = get_database_object_names(right_database_objects)
                if left_database_object_names.distributed_name and right_database_object_names.distributed_name:
                    global_if_distributed = " GLOBAL "
        sql_expression += global_if_distributed + f"{JOIN_OPERATIONS[link_type]}" + join_expressions
        return sql_expression

    def generate_composite_sql_expression_by_create_parameters(
        self,
        link_type: CompositeLinkTypeEnum,
        datasources_info_dict: dict[str, Any],
        fields: list[dict[str, Any]],
        datasources: list[dict[str, Any]],
        links_fields: list[dict[str, Any]],
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
            sql_expression = self._generate_composite_union_sql_expression(
                datasources_info_dict,
                fields,
                datasources,
            )
        else:
            sql_expression = self._generate_composite_join_sql_expression(
                link_type, datasources_info_dict, fields, datasources, links_fields
            )
        if self.database_type in (DatabaseTypeEnum.POSTGRESQL, DatabaseTypeEnum.GREENPLUM):
            sql_expression = sql_expression.replace("`", '"')
        return sql_expression

    def validate_link_field(self, datasources_info_dict: dict[str, Any], link_field: dict[str, Any]) -> None:
        """Проверка поля linkFields на валидность."""
        if datasources_info_dict.get(link_field["datasource"]) is None:
            raise ValueError(
                f"""The datasource {link_field["datasource"]} for field {link_field["datasource_field"]} is not described"""
            )
        if datasources_info_dict[link_field["datasource"]]["fields"].get(
            link_field["datasource_field"]
        ) is None and datasources_info_dict[link_field["datasource"]]["type"] not in (
            CompositeFieldRefObjectEnum.VIEW,
            CompositeFieldRefObjectEnum.CE_SCENARIO,
        ):
            raise ValueError(
                f"""The datasource {link_field["datasource"]} does not contain field {link_field["datasource_field"]}"""
            )

    def validate_fields_and_link_fields(
        self,
        link_type: str,
        datasources_info_dict: dict[str, Any],
        fields: list[dict[str, Any]],
        links_fields: list[dict[str, Any]],
    ) -> None:
        """Проверка полей linkFields и fields на валидность."""
        count_scenario_datasource = 0
        for _, datasource_info in datasources_info_dict.items():
            if datasource_info["type"] == CompositeFieldRefObjectEnum.CE_SCENARIO:
                count_scenario_datasource += 1
            if datasource_info["type"] == CompositeFieldRefObjectEnum.VIEW and (
                len(datasources_info_dict) > 1 or link_type != CompositeLinkTypeEnum.SELECT
            ):
                raise ValueError(
                    "For the datasource type 'VIEW', it is possible to use only the 'SELECT' operation with a single Datasource."
                )
        if count_scenario_datasource > 0 and count_scenario_datasource != len(datasources_info_dict):
            raise ValueError("Either all datasources must be of type CE_SCENARIO, or none of them")
        for field in fields:
            for datasource_link in field["datasource_links"]:
                self.validate_link_field(datasources_info_dict, datasource_link)

        for link_field in links_fields:
            self.validate_link_field(datasources_info_dict, link_field["left"])
            self.validate_link_field(datasources_info_dict, link_field["right"])
        return None

    async def generate_composite_sql_expression_by_composite(self, composite: Composite) -> str:
        """Генерация sql композита."""
        if composite.link_fields is None:
            composite.link_fields = []
        fields = [field.model_dump(mode="json") for field in composite.fields]
        datasources = [datasource.model_dump(mode="json") for datasource in composite.datasources]
        link_fields = [link_field.model_dump(mode="json") for link_field in composite.link_fields]
        link_type = composite.link_type
        datasources_info_dict = await self._get_datasources_field_dict(composite.datasources)
        self.validate_fields_and_link_fields(link_type, datasources_info_dict, fields, link_fields)
        return self.generate_composite_sql_expression_by_create_parameters(
            CompositeLinkTypeEnum(link_type),
            datasources_info_dict,
            fields,
            datasources,
            link_fields,
        )
