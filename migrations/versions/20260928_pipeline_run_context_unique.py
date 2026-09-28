"""enforce unique pipeline run context variable names

Revision ID: 20260928ctxuniq
Revises: c6d0d9df2db0
Create Date: 2026-09-28

"""
from alembic import op
import sqlalchemy as sa


revision = "20260928ctxuniq"
down_revision = "c6d0d9df2db0"
branch_labels = None
depends_on = None


INDEX_NAME = "uq_pipeline_run_context_data_pipeline_run_name"


def upgrade():
    connection = op.get_bind()
    rows = connection.execute(
        sa.text(
            "SELECT id, pipeline_run_id, name "
            "FROM pipeline_run_context_data ORDER BY id"
        )
    )
    seen = set()
    duplicate_ids = []
    for row in rows:
        key = (row.pipeline_run_id, row.name)
        if key in seen:
            duplicate_ids.append(row.id)
        else:
            seen.add(key)

    for duplicate_id in duplicate_ids:
        connection.execute(
            sa.text(
                "DELETE FROM pipeline_run_context_data WHERE id = :id"
            ),
            {"id": duplicate_id},
        )

    op.create_index(
        INDEX_NAME,
        "pipeline_run_context_data",
        ["pipeline_run_id", "name"],
        unique=True,
    )


def downgrade():
    op.drop_index(INDEX_NAME, table_name="pipeline_run_context_data")
