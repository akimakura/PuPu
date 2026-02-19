"""Add json_definition to database_object and create database_object_relations."""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "0060_add_database_object_json_definition_and_relations"
down_revision = "0059_drop_composite_schema_name"
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


def upgrade() -> None:
    if not _column_exists("database_object", "json_definition"):
        op.add_column(
            "database_object",
            sa.Column("json_definition", postgresql.JSONB(), nullable=True),
        )
    if not _column_exists("database_object_history", "json_definition"):
        op.add_column(
            "database_object_history",
            sa.Column("json_definition", postgresql.JSONB(), nullable=True),
        )

    if not _table_exists("database_object_relations"):
        op.create_table(
            "database_object_relations",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("semantic_object_type", sa.String(), nullable=False),
            sa.Column("semantic_object_id", sa.Integer(), nullable=False),
            sa.Column("semantic_object_version", sa.Integer(), nullable=False),
            sa.Column(
                "database_object_id",
                sa.Integer(),
                sa.ForeignKey("database_object.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("database_object_version", sa.Integer(), nullable=False),
            sa.Column(
                "relation_type",
                sa.String(),
                nullable=True,
                server_default=sa.text("'PARENT'"),
            ),
            sa.Column(
                "timestamp",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.text("CURRENT_TIMESTAMP"),
            ),
            sa.Column("version", sa.Integer(), nullable=False),
        )


def downgrade() -> None:
    if _table_exists("database_object_relations"):
        op.drop_table("database_object_relations")

    if _column_exists("database_object_history", "json_definition"):
        op.drop_column("database_object_history", "json_definition")
    if _column_exists("database_object", "json_definition"):
        op.drop_column("database_object", "json_definition")
