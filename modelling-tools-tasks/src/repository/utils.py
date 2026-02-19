from src.integrations.modelling_tools_api.codegen import DbObject
from src.models.database_object import DatabaseObjectNames, DbObjectTypeEnum


def get_database_object_names(
    database_objects: list[DbObject],
) -> DatabaseObjectNames:
    """Возвращает имена всех типов таблиц в database_objects"""
    table_name = None
    table_schema = None
    dictionary_name = None
    dictionary_schema = None
    distributed_name = None
    distributed_schema = None
    table_type = DbObjectTypeEnum.TABLE
    for database_object in database_objects:
        if (
            database_object.object_type == DbObjectTypeEnum.TABLE
            or database_object.object_type == DbObjectTypeEnum.REPLICATED_TABLE
        ):
            table_name = database_object.name
            table_schema = database_object.schema_name
            table_type = database_object.object_type
        elif database_object.object_type == DbObjectTypeEnum.DISTRIBUTED_TABLE:
            distributed_name = database_object.name
            distributed_schema = database_object.schema_name
            if table_type != DbObjectTypeEnum.DICTIONARY:
                table_type = DbObjectTypeEnum.DISTRIBUTED_TABLE
        elif database_object.object_type == DbObjectTypeEnum.DICTIONARY:
            dictionary_name = database_object.name
            dictionary_schema = database_object.schema_name
            table_type = DbObjectTypeEnum.DICTIONARY
    return DatabaseObjectNames(
        table_name=table_name,
        table_schema=table_schema,
        distributed_name=distributed_name,
        distributed_schema=distributed_schema,
        dictionary_name=dictionary_name,
        dictionary_schema=dictionary_schema,
        type=DbObjectTypeEnum(table_type),
    )
