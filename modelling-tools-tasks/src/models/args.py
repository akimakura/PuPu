from pydantic import BaseModel, ConfigDict


class ArgsModel(BaseModel):
    run_migrate: bool
    tenant: str | None
    model: list[str] | None
    composite_delete: bool
    composite_create: bool
    datastorage_update: bool
    datastorage_create: bool
    with_delete_columns: bool
    with_delete_not_empty: bool
    recreate_dictionry: bool
    raw_sql: bool
    force_composites: bool
    force_datastorages: bool
    force_dimensions: bool
    model_config = ConfigDict(from_attributes=True)
