from enum import StrEnum


class PermissionEnum(StrEnum):
    DIMENSION_VIEW = "Semantic.Dimension.View"
    DIMENSION_CREATE = "Semantic.Dimension.Create"
    DIMENSION_DELETE = "Semantic.Dimension.Delete"
    DIMENSION_EDIT = "Semantic.Dimension.Edit"
    MEASURE_VIEW = "Semantic.Measure.View"
    MEASURE_CREATE = "Semantic.Measure.Create"
    MEASURE_DELETE = "Semantic.Measure.Delete"
    MEASURE_EDIT = "Semantic.Measure.Edit"
    DATASTORAGE_VIEW = "Semantic.DataStorage.View"
    DATASTORAGE_CREATE = "Semantic.DataStorage.Create"
    DATASTORAGE_DELETE = "Semantic.DataStorage.Delete"
    DATASTORAGE_EDIT = "Semantic.DataStorage.Edit"
    COMPOSITE_VIEW = "Semantic.Composite.View"
    COMPOSITE_CREATE = "Semantic.Composite.Create"
    COMPOSITE_DELETE = "Semantic.Composite.Delete"
    COMPOSITE_EDIT = "Semantic.Composite.Edit"
    MODEL_VIEW = "Semantic.Model.View"
    MODEL_CREATE = "Semantic.Model.Create"
    MODEL_DELETE = "Semantic.Model.Delete"
    MODEL_EDIT = "Semantic.Model.Edit"
    DATABASE_VIEW = "Semantic.Database.View"
    DATABASE_CREATE = "Semantic.Database.Create"
    DATABASE_DELETE = "Semantic.Database.Delete"
    DATABASE_EDIT = "Semantic.Database.Edit"
    HIERARCHY_VIEW = "Semantic.Hierarchy.View"
    HIERARCHY_CREATE = "Semantic.Hierarchy.Create"
    HIERARCHY_DELETE = "Semantic.Hierarchy.Delete"
    HIERARCHY_EDIT = "Semantic.Hierarchy.Edit"
    TENANT_VIEW = "Semantic.Tenant.View"
    TENANT_CREATE = "Semantic.Tenant.Create"
    TENANT_DELETE = "Semantic.Tenant.Delete"
    TENANT_EDIT = "Semantic.Tenant.Edit"


ALL_PERMISSIONS = [
    PermissionEnum.DIMENSION_VIEW,
    PermissionEnum.DIMENSION_CREATE,
    PermissionEnum.DIMENSION_DELETE,
    PermissionEnum.DIMENSION_EDIT,
    PermissionEnum.MEASURE_VIEW,
    PermissionEnum.MEASURE_CREATE,
    PermissionEnum.MEASURE_DELETE,
    PermissionEnum.MEASURE_EDIT,
    PermissionEnum.DATASTORAGE_VIEW,
    PermissionEnum.DATASTORAGE_CREATE,
    PermissionEnum.DATASTORAGE_DELETE,
    PermissionEnum.DATASTORAGE_EDIT,
    PermissionEnum.COMPOSITE_VIEW,
    PermissionEnum.COMPOSITE_CREATE,
    PermissionEnum.COMPOSITE_DELETE,
    PermissionEnum.COMPOSITE_EDIT,
    PermissionEnum.MODEL_VIEW,
    PermissionEnum.MODEL_CREATE,
    PermissionEnum.MODEL_DELETE,
    PermissionEnum.MODEL_EDIT,
    PermissionEnum.DATABASE_VIEW,
    PermissionEnum.DATABASE_CREATE,
    PermissionEnum.DATABASE_DELETE,
    PermissionEnum.DATABASE_EDIT,
    PermissionEnum.HIERARCHY_VIEW,
    PermissionEnum.HIERARCHY_CREATE,
    PermissionEnum.HIERARCHY_DELETE,
    PermissionEnum.HIERARCHY_EDIT,
    PermissionEnum.TENANT_VIEW,
    PermissionEnum.TENANT_CREATE,
    PermissionEnum.TENANT_DELETE,
    PermissionEnum.TENANT_EDIT,
]
