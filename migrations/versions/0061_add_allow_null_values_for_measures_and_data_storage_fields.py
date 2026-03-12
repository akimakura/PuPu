"""Add allow_null_values flags for measures, any_fields and data_storage_fields."""

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "0061_add_allow_null_values_for_measures_and_data_storage_fields"
down_revision = "0060_add_database_object_json_definition_and_relations"
branch_labels = None
depends_on = None


def _table_exists(table: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return table in inspector.get_table_names()


def _column_exists(table: str, column: str) -> bool:
    if not _table_exists(table):
        return False
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = [col.get("name") for col in inspector.get_columns(table)]
    return column in columns


def _add_boolean_not_null_column(table: str, column: str) -> None:
    if _column_exists(table, column):
        return
    op.add_column(
        table,
        sa.Column(column, sa.Boolean(), nullable=False, server_default=sa.text("false")),
    )
    op.alter_column(table, column, server_default=None)


def upgrade() -> None:
    for table_name, column_name in (
        ("measure", "allow_null_values"),
        ("measure_history", "allow_null_values"),
        ("any_field", "allow_null_values"),
        ("any_field_history", "allow_null_values"),
        ("data_storage_field", "allow_null_values_local"),
        ("data_storage_field_history", "allow_null_values_local"),
    ):
        _add_boolean_not_null_column(table_name, column_name)


def downgrade() -> None:
    for table_name, column_name in (
        ("data_storage_field_history", "allow_null_values_local"),
        ("data_storage_field", "allow_null_values_local"),
        ("any_field_history", "allow_null_values"),
        ("any_field", "allow_null_values"),
        ("measure_history", "allow_null_values"),
        ("measure", "allow_null_values"),
    ):
        if _column_exists(table_name, column_name):
            op.drop_column(table_name, column_name)
