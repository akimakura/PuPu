"""Drop composite.schema_name column."""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0059_drop_composite_schema_name"
down_revision = None
branch_labels = None
depends_on = None


def _column_exists(table: str, column: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = [col.get("name") for col in inspector.get_columns(table)]
    return column in columns


def upgrade() -> None:
    if _column_exists("composite", "schema_name"):
        op.drop_column("composite", "schema_name")


def downgrade() -> None:
    if not _column_exists("composite", "schema_name"):
        op.add_column(
            "composite",
            sa.Column("schema_name", sa.String(), nullable=True),
        )
