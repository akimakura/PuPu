import socket
from collections import defaultdict
from typing import Any, Optional, Sequence, Tuple, Type

from sqlalchemy import select
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload
from sqlalchemy.sql.selectable import Select

from src.config import settings
from src.db.any_field import AnyField, AnyFieldLabel
from src.db.composite import CompositeField, CompositeFieldLabel
from src.db.data_storage import DataStorageField, DataStorageFieldLabel
from src.db.database_object import DatabaseObject
from src.db.dimension import Dimension
from src.db.measure import Measure
from src.db.model import Model
from src.models.consts import TEXT_TO_LENGTH
from src.models.database_object import DatabaseObject as DatabaseObjectModel, DatabaseObjectNames, DbObjectTypeEnum
from src.models.field import BaseFieldTypeEnum
from src.models.label import LabelType, Language
from src.models.model import Model as ModelModel
from src.models.request_params import Pagination, SortDirectionEnum


def check_exists_object_in_models(object_with_models: Any, model_names: list[str]) -> bool:
    """
    Проверяет, существует ли указанный объект хотя бы в одной из перечисленных моделей.

    Args:
        object_with_models (Any): Объект, связанный с моделями (например, измерение или метрику), имеющий поле "models".
        model_names (list[str]): Список названий моделей, среди которых производится проверка.

    Returns:
        bool: True, если объект присутствует хотя бы в одной указанной модели, иначе False.
    """
    result_models = {model.name for model in object_with_models.models}
    absent_models = list(set(model_names) - result_models)
    exceptions = []
    for model_name in absent_models:
        exceptions.append(
            f"{object_with_models.__class__.__qualname__} with tenant_id={object_with_models.tenant_id} "
            + f"and name={object_with_models.name} is not represented in the model_name={model_name}."
        )
    if exceptions:
        raise NoResultFound("; ".join(exceptions))
    return True


async def get_dimension_orm_model_by_session(
    session: AsyncSession, tenant_id: str, name: str, model_names: Optional[list[str]] = None
) -> Dimension:
    """Получить объект SQLAlchemy Dimension"""
    query = (
        select(Dimension)
        .options(joinedload(Dimension.dimension))
        .where(
            Dimension.name == name,
            Dimension.tenant_id == tenant_id,
        )
    )
    result = (await session.execute(query)).scalars().one_or_none()
    if not result:
        raise NoResultFound(f"Dimension with tenant_id={tenant_id} and dimension_name={name} not found.")
    if model_names:
        check_exists_object_in_models(result, model_names)
    return result


async def get_list_dimension_orm_by_session(
    session: AsyncSession,
    tenant_id: str,
    model_name: Optional[str] = None,
    names: Optional[list[str]] = None,
    pagination: Optional[Pagination] = None,
) -> Sequence[Dimension]:
    if names is None:
        query = (
            select(Dimension)
            .options(joinedload(Dimension.dimension))
            .where(
                Dimension.tenant_id == tenant_id,
            )
        )
    else:
        query = (
            select(Dimension)
            .options(joinedload(Dimension.dimension))
            .where(
                Dimension.tenant_id == tenant_id,
                Dimension.name.in_(names),
            )
        )
    query = query.where(Dimension.models.any(Model.name == model_name)) if model_name else query
    query = get_select_query_with_offset_limit_order(query, Dimension.name, pagination)
    result = (await session.execute(query)).scalars().all()
    return result


async def get_measure_orm_model_by_session(
    session: AsyncSession, tenant_id: str, name: str, model_names: Optional[list[str]] = None
) -> Measure:
    """Получить объект SQLAlchemy Measure"""
    query = select(Measure).where(
        Measure.name == name,
        Measure.tenant_id == tenant_id,
    )
    result = (await session.execute(query)).scalars().one_or_none()
    if not result:
        raise NoResultFound(f"Measure with tenant_id={tenant_id} and name={name} not found.")
    if model_names:
        check_exists_object_in_models(result, model_names)
    return result


async def get_list_of_measures_orm_by_session(
    session: AsyncSession,
    tenant_id: str,
) -> Sequence[Measure]:
    """
    Асинхронный метод для получения списка мер (`Measure`) из базы данных через ORM-сессию.

    Args:
        session (AsyncSession): Активная асинхронная сессия для взаимодействия с БД.
        tenant_id (str): Идентификатор арендатора, чьи меры извлекаются.
    Returns:
        Sequence[Measure]: Последовательность объектов мер (`Measure`), удовлетворяющих условиям запроса.
    """
    query = select(Measure).where(
        Measure.tenant_id == tenant_id,
    )
    result = (await session.execute(query)).scalars().unique().all()
    return result


def add_missing_labels(
    labels: list[dict[str, Any]], name: str, append_short: bool = True, append_long: bool = True
) -> None:
    """
    Добавить label SHORT и LONG атрибутом name, если они отсутствуют.
    """
    missing_labels: dict[LabelType, dict[Any, Any]] = {
        LabelType.SHORT: {
            Language.EN: None,
            Language.RU: None,
            "other": None,
        },
        LabelType.LONG: {
            Language.EN: None,
            Language.RU: None,
            "other": None,
        },
    }
    for label in labels:
        if label["type"] == LabelType.SHORT and label["language"] == Language.RU:
            missing_labels[LabelType.SHORT][Language.RU] = label
        elif label["type"] == LabelType.SHORT and label["language"] == Language.EN:
            missing_labels[LabelType.SHORT][Language.EN] = label
        elif label["type"] == LabelType.SHORT:
            missing_labels[LabelType.SHORT]["other"] = label
        elif label["type"] == LabelType.LONG and label["language"] == Language.RU:
            missing_labels[LabelType.LONG][Language.RU] = label
        elif label["type"] == LabelType.LONG and label["language"] == Language.EN:
            missing_labels[LabelType.LONG][Language.EN] = label
        elif label["type"] == LabelType.LONG:
            missing_labels[LabelType.LONG]["other"] = label
    short_text = (
        missing_labels[LabelType.SHORT][Language.RU]
        or missing_labels[LabelType.SHORT][Language.EN]
        or missing_labels[LabelType.SHORT]["other"]
    )
    long_text = (
        missing_labels[LabelType.LONG][Language.RU]
        or missing_labels[LabelType.LONG][Language.EN]
        or missing_labels[LabelType.LONG]["other"]
    )
    if append_short and not short_text and long_text:
        short_text = {
            "language": long_text["language"],
            "type": LabelType.SHORT,
            "text": long_text["text"][: TEXT_TO_LENGTH[LabelType.SHORT]],
        }
        labels.append(short_text)
    elif append_short and not short_text:
        short_text = {"language": Language.EN, "type": LabelType.SHORT, "text": name}
        labels.append(short_text)
    if append_long and not long_text and short_text:
        long_text = {
            "language": short_text["language"],
            "type": LabelType.LONG,
            "text": short_text["text"],
        }
        labels.append(long_text)
    elif append_long and not long_text:
        long_text = {"language": Language.EN, "type": LabelType.LONG, "text": name}
        labels.append(long_text)
    return None


def convert_labels_list_to_orm(labels: list[dict[str, Any]], model: Any) -> list[Any]:
    """
    Конвертирует label из формата list[dict] в list[ModelLabel],
    где ModelLabel - orm представление label для конкретной модели.
    """
    return [model(**label) for label in labels]


def convert_anyfield_dict_to_orm(
    any_field: dict,
) -> AnyField:
    """Конвертирует AnyField из словаря в модель SQLalchemy AnyField"""
    any_field["labels"] = convert_labels_list_to_orm(any_field.pop("labels", []), AnyFieldLabel)
    return AnyField(**any_field)


async def convert_ref_type_to_orm(
    session: AsyncSession,
    tenant_id: str,
    model_names: Optional[list[str]],
    ref_type: dict,
) -> AnyField | Measure | Dimension:
    """Конвертирует словарь ref_type в AnyField, Measure или Dimension"""
    field_type = ref_type["ref_object_type"]
    ref_object: AnyField | Measure | Dimension
    if field_type == BaseFieldTypeEnum.MEASURE:
        ref_object = await get_measure_orm_model_by_session(
            session=session, tenant_id=tenant_id, name=ref_type["ref_object"], model_names=model_names
        )
        return ref_object
    if field_type == BaseFieldTypeEnum.DIMENSION:
        ref_object = await get_dimension_orm_model_by_session(
            session=session, tenant_id=tenant_id, name=ref_type["ref_object"], model_names=model_names
        )
        return ref_object
    if field_type == BaseFieldTypeEnum.ANYFIELD:
        ref_object = ref_type.get("ref_object")
        if not isinstance(ref_object, dict):
            raise ValueError("ref_object for ANYFIELD must be object")
        if "name" not in ref_object:
            raise ValueError("ref_object for ANYFIELD must contain name")
        ref_object.setdefault("labels", [])
        add_missing_labels(ref_object["labels"], ref_object["name"], append_long=False)
        return convert_anyfield_dict_to_orm(ref_object)
    raise ValueError(f"Unknown type: {field_type}")


async def convert_field_to_orm(
    session: AsyncSession,
    field: dict,
    tenant_id: str,
    model_names: Optional[list[str]],
    model_type: Type[CompositeField] | Type[DataStorageField],
) -> DataStorageField | CompositeField:
    """
    Конвертирует dict поле в composite или data_storage в CompositeField или DatastorageField
    """
    model_label_type = DataStorageFieldLabel if model_type == DataStorageField else CompositeFieldLabel
    dimension: Optional[Dimension] = None
    measure: Optional[Measure] = None
    ref_type = field.pop("ref_type")
    field.pop("sql_column_type", None)
    object_field = await convert_ref_type_to_orm(session, tenant_id, model_names, ref_type)
    field["labels"] = convert_labels_list_to_orm(field.pop("labels", []), model_label_type)
    field["field_type"] = ref_type["ref_object_type"]
    # SQLAlchemy column defaults are applied on flush/insert, while we validate ORM object earlier.
    # Ensure explicit booleans for new DataStorageField objects.
    if model_type == DataStorageField:
        field["is_key"] = False if field.get("is_key") is None else field.get("is_key")
        field["is_sharding_key"] = False if field.get("is_sharding_key") is None else field.get("is_sharding_key")
        field["is_tech_field"] = False if field.get("is_tech_field") is None else field.get("is_tech_field")
    if field["field_type"] == BaseFieldTypeEnum.MEASURE and isinstance(object_field, Measure):
        field["measure_id"] = object_field.id
        measure = object_field
    elif field["field_type"] == BaseFieldTypeEnum.DIMENSION and isinstance(object_field, Dimension):
        field["dimension_id"] = object_field.id
        dimension = object_field
    elif field["field_type"] == BaseFieldTypeEnum.ANYFIELD and isinstance(object_field, AnyField):
        field["any_field"] = object_field
    elif object_field is not None:
        raise ValueError("object_field has unknown type.")
    model_field = model_type(**field)
    model_field.dimension = dimension
    model_field.measure = measure
    return model_field


def get_ip_address_by_dns_name(dns_name: str) -> Optional[str]:
    """
    Получить ip адресс по имени домена
    """
    try:
        ip_address = socket.gethostbyname(dns_name)
    except Exception:  # noqa
        ip_address = "unknown"
    return ip_address


def get_database_object_names(
    database_objects: list[DatabaseObjectModel] | list[DatabaseObject],
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
        if database_object.type == DbObjectTypeEnum.TABLE or database_object.type == DbObjectTypeEnum.REPLICATED_TABLE:
            table_name = database_object.name
            table_schema = database_object.schema_name
            table_type = database_object.type
        elif database_object.type == DbObjectTypeEnum.DISTRIBUTED_TABLE:
            distributed_name = database_object.name
            distributed_schema = database_object.schema_name
            if table_type != DbObjectTypeEnum.DICTIONARY:
                table_type = DbObjectTypeEnum.DISTRIBUTED_TABLE
        elif database_object.type == DbObjectTypeEnum.DICTIONARY:
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
        type=table_type,
    )


def get_select_query_with_offset_limit_order(
    query: Select[Tuple[Any]],
    order_field: Any,
    pagination: Optional[Pagination] = None,
) -> Select[Tuple[Any]]:
    """Получить запрос, дообогащенный order_by, offset и limit."""
    if not pagination:
        return query
    order_field = order_field.desc() if pagination.sort_direction == SortDirectionEnum.desc else order_field
    if pagination.sort_direction:
        query = query.order_by(order_field)
    if pagination.offset is not None and pagination.limit is not None:
        query = query.offset(pagination.offset).limit(pagination.limit)
    return query


def get_field_type_with_length(field: DataStorageField) -> tuple[Optional[int], Optional[int], str]:
    """
    Возвращает Тип поля, его длину и точность.
    """
    precision = None
    scale = None
    field_type = None
    if field.field_type == BaseFieldTypeEnum.DIMENSION and field.dimension:
        field_type = field.dimension.type
        precision = field.dimension.precision
    elif field.field_type == BaseFieldTypeEnum.ANYFIELD and field.any_field:
        field_type = field.any_field.type
        precision = field.any_field.precision
        scale = field.any_field.scale
    elif field.field_type == BaseFieldTypeEnum.MEASURE and field.measure:
        field_type = field.measure.type
        precision = field.measure.precision
        scale = field.measure.scale
    if field_type is None:
        raise ValueError(
            f"""Field {field.name}
            (type={field.field_type}, data_storage_id={field.data_storage_id})
            cannot be converted to a type"""
        )
    return precision, scale, field_type


def get_object_filtred_by_model_name(objs: list[Any], model_name: str, is_equal_model_name: bool = False) -> list[Any]:
    """Возвращает список объектов, отфильтрованный по имени модели.

    Если is_equal_model_name=True, возвращаются объекты, у которых есть хотя бы одна модель с именем model_name.
    Если is_equal_model_name=False, возвращаются объекты, у которых **нет ни одной** модели с именем model_name.
    """
    if is_equal_model_name:
        return [obj for obj in objs if any(model.name == model_name for model in obj.models)]
    else:
        return [obj for obj in objs if all(model.name != model_name for model in obj.models)]


def get_filtred_database_object_by_data_storage(
    object_with_db_objects: Any, model_name: str
) -> list[DatabaseObjectModel]:
    """Возвращает отфильтрованные по модели database_object из объекта, содержащего db_object."""
    if object_with_db_objects is None:
        return []
    db_objects = get_object_filtred_by_model_name(object_with_db_objects.database_objects, model_name, True)
    return [DatabaseObjectModel.model_validate(db_object) for db_object in db_objects]


def get_database_schema_database_object_mapping(obj_with_database_objects: Any) -> tuple[dict, dict]:
    original_databases_database_objects_dict: dict[tuple[str, str], list[DatabaseObject]] = defaultdict(list)
    appended_database_objects: dict[tuple[str, str], set[str]] = defaultdict(set)
    for original_database_object in obj_with_database_objects.database_objects:
        for database_object_model in original_database_object.models:
            database_object_model_model = ModelModel.model_validate(database_object_model)
            database_object_database = database_object_model_model.database
            if database_object_database is None:
                raise ValueError("The model must contain a database")
            schema_name = database_object_model_model.schema_name
            if original_database_object.name in appended_database_objects[(database_object_database.name, schema_name)]:
                continue
            appended_database_objects[(database_object_database.name, schema_name)].add(original_database_object.name)
            original_databases_database_objects_dict[(database_object_database.name, schema_name)].append(
                original_database_object
            )
    return original_databases_database_objects_dict, appended_database_objects


def get_and_compare_model_name_by_priority(current_model_name: str, new_model_name: str) -> str:
    priority_list = settings.DIMENSION_OWNER_PRIORITY
    try:
        current_index = priority_list.index(current_model_name)
        current_in_priority = True
    except ValueError:
        current_in_priority = False

    try:
        new_index = priority_list.index(new_model_name)
        new_in_priority = True
    except ValueError:
        new_in_priority = False
    if current_in_priority and new_in_priority:
        if new_index < current_index:
            return new_model_name
        else:
            return current_model_name
    elif (current_in_priority and not new_in_priority) or (not current_in_priority and not new_in_priority):
        return current_model_name
    else:
        return new_model_name


def is_ignore_dimension(model_name: str, name: str, is_virtual: bool) -> bool:
    """Проверяет, следует ли игнорировать измерение при взаимодействии с ним."""
    if not settings.ENABLE_IGNORING_DIMENSIONS:
        return False
    return model_name in settings.MODELS_BLACKLIST and name not in settings.DIMENSIONS_WHITELIST and not is_virtual
