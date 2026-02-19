"""Тут задаются все БД модели."""

from sqlalchemy.orm import configure_mappers

from src.db.any_field import AnyField, AnyFieldLabel
from src.db.composite import (
    Composite,
    CompositeDatasource,
    CompositeField,
    CompositeFieldLabel,
    CompositeLabel,
    CompositeLinkFields,
    CompositeModelRelation,
)
from src.db.data_storage import (
    DataStorage,
    DataStorageField,
    DataStorageFieldLabel,
    DataStorageLabel,
    DataStorageModelRelation,
)
from src.db.database import Connection, Database, DatabaseLabel, Port
from src.db.database_object import DatabaseObject, DatabaseObjectModelRelation, DataBaseObjectSpecificAttribute
from src.db.dimension import (
    Dimension,
    DimensionAttribute,
    DimensionAttributeLabel,
    DimensionLabel,
    HierarchyBaseDimension,
    TextLink,
)
from src.db.hierarchy import HierarchyLabel, HierarchyMeta, HierarchyModelRelation, HierarchyStructureType
from src.db.measure import DimensionFilter, Measure, MeasureLabel
from src.db.meta import Base, metadata
from src.db.model import Model, ModelLabel
from src.db.tenant import Tenant, TenantLabel

configure_mappers()
