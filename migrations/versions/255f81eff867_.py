"""empty message

Revision ID: 255f81eff867
Revises: d3189cb5fe01
Create Date: 2018-03-14 13:27:34.654315

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = '255f81eff867'
down_revision = 'd3189cb5fe01'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('job',
                  sa.Column('exception_stack', mysql.LONGTEXT(), nullable=True))
    op.add_column('job',
                  sa.Column('name', sa.String(length=50), nullable=True))

    op.execute("""
        ALTER TABLE job
            MODIFY status_text LONGTEXT,
            MODIFY source_code LONGTEXT,
            MODIFY workflow_definition LONGTEXT""")

    op.execute("ALTER TABLE job_result MODIFY content LONGTEXT")
    op.execute("ALTER TABLE job_step_log MODIFY message LONGTEXT")
    op.execute('UPDATE job SET name = workflow_name')


def downgrade():
    op.drop_column('job', 'name')
    op.drop_column('job', 'exception_stack')

    op.execute("""
        ALTER TABLE job
            MODIFY status_text TEXT,
            MODIFY source_code TEXT,
            MODIFY workflow_definition TEXT""")
    op.execute("ALTER TABLE job_result MODIFY content TEXT")
    op.execute("ALTER TABLE job_step_log MODIFY message TEXT")
