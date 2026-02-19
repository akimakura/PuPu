from enum import StrEnum


class CompositeFieldRefObjectEnum(StrEnum):
    """
    Перечисление, представляющее типы объектов, связанных с составными полями.

    Каждое значение перечисления является строковым идентификатором, используемым для
    обозначения категории объекта в системе. Используется в качестве ограничения или
    фильтрации при работе с составными структурами данных.

    Attibutes:
        DATASTORAGE: Объект, представляющий хранилище данных.
        COMPOSITE: Составной объект, состоящий из нескольких компонентов.
        VIEW: Представление данных (например, визуализация или отчет).
        CE_SCENARIO: Сценарий обработки данных в контексте бизнес-процесса (Нужен для Calculation Engine).
    """

    DATASTORAGE = "DATASTORAGE"
    COMPOSITE = "COMPOSITE"
    VIEW = "VIEW"
    CE_SCENARIO = "CE_SCENARIO"


class CompositeLinkTypeEnum(StrEnum):
    LEFT_JOIN = "LEFT_JOIN"
    INNER_JOIN = "INNER_JOIN"
    UNION = "UNION"
    SELECT = "SELECT"
