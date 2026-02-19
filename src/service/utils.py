from typing import Any, Optional

from fastapi import UploadFile
from pandas import DataFrame, Series, read_csv, read_excel

from src.models.consts import REPLACE_VALUE_DATAFRAME_DICT
from src.models.label import Label, LabelType, Language
from src.models.request_params import ContentTypeHeaderEnum


def get_updated_fields_object(original_object: dict, updated_object: dict) -> dict:
    """
    Возвращает поля, которые были обновлены для лога аудита.
    """
    updated_fields: dict = {}
    for field, value in updated_object.items():
        updated_fields[field] = value
    for field, value in original_object.items():
        if field in updated_fields:
            updated_fields[f"PREV_{field}"] = value
    return updated_fields


def read_upload_file_as_dataframe(file: UploadFile, columns: tuple = (), dtype: Optional[dict] = None) -> DataFrame:
    """
    Прочитать файл как датафрейм.

    Args:
        file (UploadFile): исходный файл
        columns (tuple): список колонок в исходном файле
        dtype (Optional[dict]): словарь колонок вида {"field_name": "field_type"} для корректного маппинга типов.

    Returns:
        DataFrame: DataFrame, содержащий информацию из файла
    """
    if file.content_type == ContentTypeHeaderEnum.CSV:
        df = read_csv(  # type: ignore
            file.file,
            delimiter=",",
        )
        df.columns = df.columns.str.strip()
        df = df[list(columns)]
    elif file.content_type == ContentTypeHeaderEnum.XLSX:
        df = read_excel(  # type: ignore
            file.file,
        )
        df.columns = df.columns.str.strip()
        df = df[list(columns)]
    else:
        raise ValueError(f"Unknown file content_type. filename={file.filename}, type={file.content_type}")
    if dtype:
        for column, data_type in dtype.items():
            if df[column].dtype == "object":
                df[column] = df[column].str.strip()
            df[column] = df[column].replace(REPLACE_VALUE_DATAFRAME_DICT)
            df[column] = df[column].astype(data_type)
            df[column] = df[column].replace(REPLACE_VALUE_DATAFRAME_DICT)
    return df


def labels_by_row(row: Series, file_enum_type: Any) -> list[Label]:
    """
    Достает из строки Dataframe список лейблов.

    Args:
        row (Series): строка DataFrame
        file_enum_type (Any): любой Enum, описывающий имена колонок DataFrame содержащий SHORT_LABEL и LONG_LABEL.

    Returns:
        list[Label]: список лейблов
    """
    labels = []
    label_types: tuple = (
        (file_enum_type.SHORT_LABEL, LabelType.SHORT),
        (file_enum_type.LONG_LABEL, LabelType.LONG),
    )
    for label_type in label_types:
        if row[label_type[0]]:
            labels.append(
                Label(
                    language=Language.RU,
                    type=label_type[1],
                    text=row[label_type[0]],
                )
            )
    return labels


def get_short_label_or_any_label(object_with_labels: Any) -> Label:
    """
    Возвращает первый SHORT label из object_with_labels.labels, если его нет, то возвращает любой label.

    Args:
        object_with_labels (Any): объект с полем labels

    Returns:
        Label: первый SHORT label или любой label
    """
    if object_with_labels.labels:
        for label in object_with_labels.labels:
            if label.type == LabelType.SHORT:
                return label
        return object_with_labels.labels[0]
    else:
        raise ValueError("Object has no labels")


def get_diff_lst(lst1: list[Any], lst2: list[Any]) -> list[Any]:
    set_lst2 = set(lst2)
    result = []
    for lst_item in lst1:
        if lst_item not in set_lst2:
            result.append(lst_item)
    return result
