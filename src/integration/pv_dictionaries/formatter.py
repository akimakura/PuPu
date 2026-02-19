"""Конвертация объектов семантического слоя в формат pv dictionary"""

from typing import Optional
from xml.dom.minidom import Document, Element

from py_common_lib.logger import EPMPYLogger

from src.config import settings
from src.db.data_storage import DataStorage, DataStorageField, DataStorageFieldLabel
from src.db.dimension import Dimension, DimensionAttribute, DimensionAttributeLabel, DimensionLabel
from src.integration.pv_dictionaries.consts import REFERENCE
from src.integration.pv_dictionaries.models import PVAttribute, PVAttributeType, PVLabels
from src.models.any_field import AnyFieldTypeEnum
from src.models.consts import (
    DATA_TYPES,
    DATEFROM,
    DATETO,
    DEFAULT_DATE_FROM,
    DEFAULT_DATE_TO,
    DEFAULT_TYPE_VALUES,
    LANGUAGE_FIELD,
    NOT_KEYS,
    PV_DICTIONARIES,
)
from src.models.data_storage import DataStorageEnum, DataStorageField as DataStorageFieldModel
from src.models.dimension import DimensionTextFieldEnum, DimensionTypeEnum
from src.models.field import BaseFieldTypeEnum
from src.models.label import Label, LabelType, Language
from src.repository.utils import get_field_type_with_length
from src.utils.validators import snake_to_camel

logger = EPMPYLogger(__name__)


class PVDictionaryFormatter:
    """Конвертация объектов семантического слоя в формат pv dictionary."""

    def __init__(
        self,
        dimension: Dimension,
        pv_dictionary: dict,
        ref_dimension_map: dict[str, Dimension] | None = None,
        with_attrs: bool = True,
        with_texts: bool = True,
    ) -> None:
        if not ref_dimension_map:
            self.ref_dimension_map = {}
        else:
            self.ref_dimension_map = ref_dimension_map
        self.dimension = dimension
        self.with_attrs = with_attrs
        self.with_texts = with_texts
        self.pv_dictionary = pv_dictionary
        self.root = Document()

    def _get_pv_dictionary_name(self) -> str:
        """Возвращает имя справочника из поддерживаемых ключей."""
        dictionary_name = self.pv_dictionary.get("dictionary_name") or self.pv_dictionary.get("object_name")
        if not dictionary_name:
            raise KeyError("Expected dictionary_name or object_name in pv_dictionary")
        return str(dictionary_name)

    @staticmethod
    def _convert_labels_to_pv_labels(
        labels: list[DimensionLabel] | list[DimensionAttributeLabel] | list[DataStorageFieldLabel] | list[Label],
    ) -> PVLabels:
        """Конвертация леблов семантического слоя в PVLabels."""
        pv_labels = PVLabels()
        for label in labels:
            if label.language == Language.RU and label.type == LabelType.SHORT:
                pv_labels.ru_short = label.text
            elif label.language == Language.RU and label.type == LabelType.LONG:
                pv_labels.ru_long = label.text
            elif label.language == Language.EN and label.type == LabelType.SHORT:
                pv_labels.en_short = label.text
            elif label.language == Language.EN and label.type == LabelType.LONG:
                pv_labels.en_long = label.text
            elif label.type == LabelType.SHORT:
                pv_labels.short = label.text
            elif label.type == LabelType.LONG:
                pv_labels.long = label.text
            else:
                pv_labels.other = label.text
        return pv_labels

    def _get_label_by_dimension(self) -> Optional[str]:
        """Получить лейбл текущего dimension."""

        labels = self._convert_labels_to_pv_labels(self.dimension.labels)
        return (
            labels.ru_short
            or labels.ru_long
            or labels.en_short
            or labels.en_long
            or labels.short
            or labels.long
            or labels.other
            or self._get_pv_dictionary_name()
        )

    def _get_description(self) -> Optional[str]:
        """Получить описание dimension."""
        labels = self._convert_labels_to_pv_labels(self.dimension.labels)
        return labels.ru_long or labels.en_long or labels.long

    def _get_field_params(self, field: DataStorageField) -> tuple[str, int, Optional[int]]:
        """Получить параметры поля на языке pv dictionary."""
        if (
            field.field_type == BaseFieldTypeEnum.DIMENSION
            and field.dimension
            and (not field.dimension.is_virtual or field.dimension.name in self.ref_dimension_map)
            and field.name != self.dimension.name
        ):
            return REFERENCE, field.dimension.precision, None
        elif (
            field.field_type == BaseFieldTypeEnum.DIMENSION and field.dimension and field.name == self.dimension.name
        ) or (field.field_type == BaseFieldTypeEnum.DIMENSION and field.dimension and field.dimension.is_virtual):
            return DATA_TYPES[PV_DICTIONARIES][field.dimension.type], field.dimension.precision, None
        elif field.field_type == BaseFieldTypeEnum.ANYFIELD and field.any_field:
            return DATA_TYPES[PV_DICTIONARIES][field.any_field.type], field.any_field.precision, field.any_field.scale
        elif field.field_type == BaseFieldTypeEnum.MEASURE and field.measure:
            return DATA_TYPES[PV_DICTIONARIES][field.measure.type], field.measure.precision, field.measure.scale
        raise ValueError("Неизвестный тип поля")

    def _get_regexp(self, field: DataStorageField | DimensionAttribute) -> str:
        """Получить регулярное выражение для поля."""
        if self.dimension.case_sensitive:
            return "^[А-ЯЁа-яёA-Za-z0-9_#-:;+.,]+$"
        return "^[А-ЯЁA-Z0-9_#-:;+.,]+$"

    def _convert_data_storage_field_to_pv_attribute(self, field: DataStorageField) -> PVAttribute:
        """Получить тип поля на языке pv dictionary."""
        field_model = DataStorageFieldModel.model_validate(field)
        labels = self._convert_labels_to_pv_labels(field_model.labels)
        field_type, precision, scale = self._get_field_params(field)
        label = labels.ru_short or labels.en_short or labels.short or field.name
        description = labels.ru_long or labels.en_long or labels.long
        dictionary = None
        if field_type == REFERENCE and field.dimension and field.dimension.dimension_id:
            ref_dimension = self.ref_dimension_map.get(field.dimension.name)
            if ref_dimension is None:
                print_ref_dimension_dict = {key: value.name for key, value in self.ref_dimension_map.items()}
                raise ValueError(
                    f"Last ref dimension for {field.dimension.name} not found. last ref dimensions (not virtuals): {print_ref_dimension_dict}"
                )
            if ref_dimension.pv_dictionary is None:
                raise ValueError(
                    f"Dimension {ref_dimension.name} referenced by dimension {field.dimension.name} does not have a pv_dictionary field"
                )
            dictionary = ref_dimension.pv_dictionary.object_name
        elif field_type == REFERENCE and field.dimension and field.dimension.pv_dictionary:
            dictionary = field.dimension.pv_dictionary.object_name
        elif field_type == REFERENCE and field.dimension and not field.dimension.pv_dictionary:
            raise ValueError(f"Dimension {field.dimension.name} does not have a pv_dictionary field")
        return PVAttribute(
            is_key=field.is_key if field.name not in NOT_KEYS else False,
            name=snake_to_camel(field.name),
            label=label,
            type=(
                DATA_TYPES[PV_DICTIONARIES][AnyFieldTypeEnum.STRING]
                if field.name == self.dimension.name and field.is_key
                else field_type
            ),
            key="ID" if field_type == REFERENCE else None,
            dictionary=dictionary,
            description=description,
            precision=precision,
            scale=scale if scale else 0,
            regex=self._get_regexp(field),
        )

    def _get_attributes_by_data_storage(self, data_storage: Optional[DataStorage]) -> dict[str, PVAttribute]:
        """Получить атрибуты data_storage в формате PVAttribute."""
        pv_attributes: dict[str, PVAttribute] = {}
        if not data_storage:
            return pv_attributes
        for field in data_storage.fields:
            if field.name in {DATEFROM, DATETO} or field.is_tech_field:
                continue
            pv_attribute = self._convert_data_storage_field_to_pv_attribute(field)
            if field.name == self.dimension.name:
                pv_attribute.pv_attribute_type = PVAttributeType.DIMENSION_KEY
            elif data_storage.type == DataStorageEnum.DIMENSION_TEXTS:
                pv_attribute.pv_attribute_type = PVAttributeType.TEXT
            elif data_storage.type == DataStorageEnum.DIMENSION_ATTRIBUTES:
                pv_attribute.pv_attribute_type = PVAttributeType.ATTRIBUTE
            pv_attributes[field.name] = pv_attribute
        return pv_attributes

    @staticmethod
    def _get_default_value(field: DataStorageField) -> Optional[int | str]:
        """Получить стандартное значение для поля с определенным типом данных."""
        _, _, field_type = get_field_type_with_length(field)
        if field.name == DATEFROM and field_type == DimensionTypeEnum.DATE:
            return DEFAULT_DATE_FROM
        elif field.name == DATETO and field_type == DimensionTypeEnum.DATE:
            return DEFAULT_DATE_TO
        elif not field.is_key:
            return DEFAULT_TYPE_VALUES[PV_DICTIONARIES][field_type]
        return None

    def _set_time_dependency_to_attributes(self, attributes: dict[str, PVAttribute]) -> None:
        """Добавить поле time_dependency в attributes."""
        if self.with_attrs:
            for dimension_attribute in self.dimension.attributes:
                if dimension_attribute.time_dependency:
                    attributes[dimension_attribute.name].time_dependency = True
        if self.with_texts:
            for _, attribute in attributes.items():
                if self.dimension.texts_time_dependency and attribute.pv_attribute_type == PVAttributeType.TEXT:
                    attribute.time_dependency = True

    def _get_attributes(self) -> dict[str, PVAttribute]:
        """Получить все атрибуты dimension в формате PVAttribute."""
        values_data_storage = self.dimension.values_table
        texts_data_storage = self.dimension.text_table
        attributes_data_storage = self.dimension.attributes_table
        attributes = self._get_attributes_by_data_storage(values_data_storage)
        if self.with_texts:
            attributes.update(self._get_attributes_by_data_storage(texts_data_storage))
        if self.with_attrs:
            attributes.update(self._get_attributes_by_data_storage(attributes_data_storage))
        self._set_time_dependency_to_attributes(attributes)
        if not self.dimension.attributes_time_dependency:
            attributes.pop(DATEFROM, None)
            attributes.pop(DATETO, None)
        return attributes

    def _get_label_and_description_by_attribute(self, attribute: DimensionAttribute) -> tuple[str, Optional[str]]:
        """Получить лейбл и описание атрибута."""
        labels = self._convert_labels_to_pv_labels(attribute.labels)
        description = labels.ru_long or labels.en_long or labels.long
        label = labels.ru_short or labels.en_short or labels.short or attribute.name
        return label, description

    def _generate_id_element(self) -> Element:
        """Создает атрибут ID."""
        id_element = self.root.createElement("Long")
        id_element.setAttribute("name", "ID")
        id_element.setAttribute("label", "ID")
        id_element.setAttribute("minEntries", "1")
        id_element.setAttribute("unique", "true")
        return id_element

    def replace_bad_label(self, label: str) -> str:
        return label.replace('"', "")

    def _set_name_and_label_to_attribute(self, attribute_element: Element, attribute: PVAttribute) -> None:
        """Добавляет в атрибут поля name и label."""
        attribute_element.setAttribute("name", attribute.name)
        attribute_element.setAttribute("label", self.replace_bad_label(attribute.label))
        return None

    def _set_max_length_and_markdown_to_attribute(self, attribute_element: Element, attribute: PVAttribute) -> None:
        """Добавляет максимальную длину и markdown в строковый атрибут."""
        if attribute.type == DATA_TYPES[PV_DICTIONARIES][AnyFieldTypeEnum.STRING]:
            attribute_element.setAttribute("maxLength", str(attribute.precision))
            attribute_element.setAttribute("markdown", "false")
            if attribute.name not in {
                DimensionTextFieldEnum.SHORT_TEXT,
                DimensionTextFieldEnum.LONG_TEXT,
                DimensionTextFieldEnum.MEDIUM_TEXT,
            }:
                attribute_element.setAttribute("regex", attribute.regex)
        return None

    def _set_precision_to_attribute(self, attribute_element: Element, attribute: PVAttribute) -> None:
        """Добавляет precision в decimal атрибут."""
        if attribute.type == DATA_TYPES[PV_DICTIONARIES][AnyFieldTypeEnum.DECIMAL]:
            attribute_element.setAttribute("precision", str(attribute.scale))
        return None

    def _set_is_default_value_now_to_attribute(self, attribute_element: Element, attribute: PVAttribute) -> None:
        """Добавляет isDefaultValueNow в date атрибут."""
        if attribute.type == DATA_TYPES[PV_DICTIONARIES][AnyFieldTypeEnum.DATE]:
            attribute_element.setAttribute("isDefaultValueNow", "false")
        return None

    def _set_description_to_attribute(self, attribute_element: Element, attribute: PVAttribute) -> None:
        """Добавляет описание в атрибут, если оно есть."""
        if attribute.description:
            attribute_element.setAttribute("description", attribute.description)

    def _set_key_and_dictionary(self, attribute_element: Element, attribute: PVAttribute) -> None:
        """Добавляет для Reference атрибута поля key и dictionary."""
        if attribute.type == REFERENCE and attribute.key and attribute.dictionary:
            attribute_element.setAttribute("key", attribute.key)
            attribute_element.setAttribute("dictionary", attribute.dictionary)
        return None

    def _set_min_max_value(self, attribute_element: Element, attribute: PVAttribute) -> None:
        """Добавляет для числовых атрибутов поля minValue и maxValue."""
        if attribute.type in {
            DATA_TYPES[PV_DICTIONARIES][AnyFieldTypeEnum.DECIMAL],
            DATA_TYPES[PV_DICTIONARIES][AnyFieldTypeEnum.INTEGER],
            DATA_TYPES[PV_DICTIONARIES][AnyFieldTypeEnum.FLOAT],
        }:
            min_value = "-" + "9" * attribute.precision
            max_value = "9" * attribute.precision
            if attribute.scale:
                min_value += "." + "0" * (attribute.scale - 1) + "1"
                max_value += "." + "9" * attribute.scale
            attribute_element.setAttribute("minValue", min_value)
            attribute_element.setAttribute("maxValue", max_value)
        return None

    def _set_min_entries(self, attribute_element: Element, attribute: PVAttribute) -> None:
        """Добавляет поле minEntries в атрибуты datefrom, dateto, language  или в ключевые."""
        if attribute.is_key or attribute.name in {DATEFROM, DATETO, LANGUAGE_FIELD}:
            attribute_element.setAttribute("minEntries", "1")
        return None

    def _set_unique_to_attribute(self, attribute_element: Element, attribute: PVAttribute) -> None:
        """Добавляет unique для бизнес-ключа."""
        if attribute.pv_attribute_type == PVAttributeType.DIMENSION_KEY:
            attribute_element.setAttribute("unique", "true")
        return None

    def add_key_to_key_element_by_attribute(self, key_element: Element, attribute: PVAttribute) -> None:
        """Добавляет атрибут в элемент <Keys>, если он ключ."""
        if attribute.is_key:
            self.add_key_to_key_element_by_name(key_element, attribute.name)
        return None

    def add_key_to_key_element_by_name(self, key_element: Element, name: str) -> None:
        """Добавляет атрибут в элемент <Keys>, если он ключ."""
        key_attribute_element = self.root.createElement("KeyAttribute")
        key_attribute_element.setAttribute("name", name)
        key_element.appendChild(key_attribute_element)
        return None

    def _get_date_attribute(self, name: str, label: str, default_value: str) -> Element:
        """
        Создает и настраивает элемент даты с заданными атрибутами.

        Args:
            name (str): Имя атрибута, используемое в XML-структуре.
            label (str): Метка для отображения пользователю.
            default_value (str): Значение по умолчанию в формате 'dd.MM.yyyy'.

        Returns:
            Element: Настраиваемый XML-элемент типа Date со следующими атрибутами:
                - format: Формат даты (dd.MM.yyyy).
                - defaultValue: Указанное значение по умолчанию.
                - isDefaultValueNow: Установлено в 'false' — значение не равно текущей дате.
                - name: Имя атрибута.
                - label: Описание атрибута.
                - minEntries: Минимальное количество обязательных записей (1).
        """
        date_attribute = self.root.createElement("Date")
        date_attribute.setAttribute("format", "dd.MM.yyyy")
        date_attribute.setAttribute("defaultValue", default_value)
        date_attribute.setAttribute("isDefaultValueNow", "false")
        date_attribute.setAttribute("name", name)
        date_attribute.setAttribute("label", label)
        date_attribute.setAttribute("minEntries", "1")
        return date_attribute

    def _get_time_depended_composite(self, name: str, label: Optional[str] = None) -> Element:
        """
        Создаёт XML-элемент TimeDependentComposite с атрибутами и дочерними элементами.

        Args:
            name (str): Обязательное имя композита.
            label (Optional[str]): Опциональная метка для отображения.
                Если не указана, используется значение `name`.

        Returns:
            Element: Сформированный XML-элемент TimeDependentComposite.
        """
        time_depended_composite = self.root.createElement("TimeDependentComposite")
        time_depended_composite.setAttribute("begin", DATEFROM.upper())
        time_depended_composite.setAttribute("end", DATETO.upper())
        time_depended_composite.setAttribute("name", name)
        time_depended_composite.setAttribute("label", label if label else name)
        time_depended_composite.setAttribute("multiple", "true")
        time_depended_composite.setAttribute("maxEntries", "32766")
        date_from_attribute = self._get_date_attribute(
            DATEFROM.upper(), label="Дата С", default_value="1900-01-01T00:00:00"
        )
        date_to_attribute = self._get_date_attribute(
            DATETO.upper(), label="Дата По", default_value="2299-12-31T23:59:59"
        )
        time_depended_composite.appendChild(date_from_attribute)
        time_depended_composite.appendChild(date_to_attribute)
        return time_depended_composite

    def _set_attributes_and_keys_to_dictionary(self, dictionary: Element) -> None:
        """Поместить элементы Keys и Attributes в элемент Dictionary."""
        attributes = self._get_attributes()
        attributes_element = self.root.createElement("Attributes")
        id_element = self._generate_id_element()
        keys_element = self.root.createElement("Keys")
        key_element = self.root.createElement("Key")
        primary_key_element = self.root.createElement("PrimaryKey")
        key_attribute_element = self.root.createElement("KeyAttribute")
        key_attribute_element.setAttribute("name", "ID")
        primary_key_element.appendChild(key_attribute_element)
        keys_element.appendChild(primary_key_element)
        attributes_element.appendChild(id_element)
        attributes_time_depended_composite = self._get_time_depended_composite("TdAttribute", "Атрибуты")
        texts_time_depended_composite = self._get_time_depended_composite("TdText", "Тексты")
        for _, attribute in attributes.items():
            attribute_element = self.root.createElement(attribute.type)
            self._set_name_and_label_to_attribute(attribute_element, attribute)
            self._set_max_length_and_markdown_to_attribute(attribute_element, attribute)
            self._set_precision_to_attribute(attribute_element, attribute)
            self._set_is_default_value_now_to_attribute(attribute_element, attribute)
            self._set_description_to_attribute(attribute_element, attribute)
            self._set_key_and_dictionary(attribute_element, attribute)
            self._set_min_max_value(attribute_element, attribute)
            self._set_min_entries(attribute_element, attribute)
            self._set_unique_to_attribute(attribute_element, attribute)
            self.add_key_to_key_element_by_attribute(key_element, attribute)
            if (
                self.dimension.texts_time_dependency
                and attribute.time_dependency
                and attribute.pv_attribute_type == PVAttributeType.TEXT
            ):
                texts_time_depended_composite.appendChild(attribute_element)
            elif (
                self.dimension.attributes_time_dependency
                and attribute.time_dependency
                and attribute.pv_attribute_type == PVAttributeType.ATTRIBUTE
            ):
                attributes_time_depended_composite.appendChild(attribute_element)
            else:
                attributes_element.appendChild(attribute_element)
        if self.dimension.texts_time_dependency and self.with_texts:
            self.add_key_to_key_element_by_name(key_element, "TdText/DATETO")
            attributes_element.appendChild(texts_time_depended_composite)
        if self.dimension.attributes_time_dependency and self.with_attrs:
            self.add_key_to_key_element_by_name(key_element, "TdAttribute/DATETO")
            attributes_element.appendChild(attributes_time_depended_composite)
        if (
            self.dimension.texts_language_dependency
            and self.dimension.texts_time_dependency
            and LANGUAGE_FIELD in attributes
            and self.with_texts
        ):
            self.add_key_to_key_element_by_name(key_element, "TdText/language")
        if len(key_element.childNodes) > 0:
            keys_element.appendChild(key_element)
        dictionary.appendChild(attributes_element)
        dictionary.appendChild(keys_element)

    def _set_configurations_to_dictionary(self, dictionary: Element) -> None:
        """Поместить элемент Configurations в элемент Dictionary."""
        configurations = self.root.createElement("Configurations")
        attributes = {
            "recordType": self._get_pv_dictionary_name(),
            "recordTypeDisplay": self._get_label_by_dimension() or self._get_pv_dictionary_name(),
            "versionType": "unversioned" if not settings.PV_DICTIONARIES_VERSIONED_DICTIONARY else "byRecord",
            "exportable": "false",
            "isPublic": "true",
            "secondHand": "false",
            "displayAttribute": snake_to_camel(self.dimension.name),
            "notEditedAttributes": "ID",
            "notComparableAttributes": "ID",
        }
        if self.dimension.attributes_time_dependency and self.with_attrs:
            attributes["invisibleAttributes"] = "TdAttribute/isActive"

        for attribute_name, attribute_value in attributes.items():
            config = self.root.createElement("Config")
            config.setAttribute("name", attribute_name)
            config.setAttribute("value", attribute_value)
            configurations.appendChild(config)
        dictionary.appendChild(configurations)

    def _set_dictionary_to_domain(self, domain: Element) -> None:
        """Поместить элемент Dictionary в элемент Domain."""
        dictionary = self.root.createElement("Dictionary")
        dictionary.setAttribute("name", self._get_pv_dictionary_name())
        dictionary.setAttribute(
            "label",
            self._get_label_by_dimension() or self._get_pv_dictionary_name(),
        )
        description = self._get_description()
        if description:
            dictionary.setAttribute("description", description)
        dictionary.setAttribute("owner", ".")
        dictionary.setAttribute("division", ".")
        dictionary.setAttribute("version", settings.PV_DICTIONARIES_DICTIONARY_CREATE_VERSION)
        self._set_configurations_to_dictionary(dictionary)
        self._set_attributes_and_keys_to_dictionary(dictionary)
        domain.appendChild(dictionary)

    def _set_domain_to_model(self, model: Element) -> None:
        """Поместить элемент Domain в элемент Model."""
        domain = self.root.createElement("Domain")
        domain.setAttribute("name", self.pv_dictionary["domain_name"])
        domain.setAttribute("label", self.pv_dictionary["domain_label"])
        self._set_dictionary_to_domain(domain)
        model.appendChild(domain)

    def _set_model_to_document(self) -> None:
        """Поместить элемент Model в корень документа."""
        model = self.root.createElement("Model")
        model.setAttribute("version", settings.PV_DICTIONARIES_MODEL_VERSION)
        model.setAttribute("xmlns", settings.PV_DICTIONARIES_MODEL_XMLNS)
        self._set_domain_to_model(model)
        self.root.appendChild(model)

    def _create_xml_document(self) -> Document:
        """Создать xml документ из dimension."""
        self.root = Document()
        self._set_model_to_document()
        return self.root

    def get_xml_create_document(self) -> bytes:
        """Создать xml документ из dimension в формате bytes."""
        root = self._create_xml_document()
        xml_str = root.toprettyxml(encoding="UTF-8", standalone=True)
        return xml_str
