import typing

if typing.TYPE_CHECKING:
    from src.models.tenant import SemanticObjects


class SemanticObjectRelationException(Exception):
    def __init__(self, semantic_objects: "SemanticObjects") -> None:
        message = ["На удаляемый объект ссылаются другие объекты. Пожалуйста, удалите сначала их."]
        if semantic_objects.measures:
            measure_objects = ["Measure объекты: "]
            measure_objects.extend(semantic_objects.measures)
            message.extend(measure_objects)

        if semantic_objects.dimensions:
            dimension_objects = ["Dimension объекты: "]
            dimension_objects.extend(semantic_objects.dimensions)
            message.extend(dimension_objects)

        if semantic_objects.composites:
            composite_objects = ["Composite объекты: "]
            composite_objects.extend(semantic_objects.composites)
            message.extend(composite_objects)

        if semantic_objects.data_storages:
            data_storage_objects = ["DataStorage объекты: "]
            data_storage_objects.extend(semantic_objects.data_storages)
            message.extend(data_storage_objects)
        super().__init__("\n".join(message))


class ViewDependentColumnChangeException(Exception):
    """Raised when ALTER affects datastorage columns referenced by views."""
