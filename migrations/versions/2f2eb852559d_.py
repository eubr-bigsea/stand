"""empty message

Revision ID: 2f2eb852559d
Revises: 4eb6f2af4b72
Create Date: 2017-11-03 15:32:13.303940

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = '2f2eb852559d'
down_revision = '4eb6f2af4b72'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('job_step',
                  sa.Column('task_name', sa.String(length=200), nullable=True))
    op.add_column('job_step_log', sa.Column(
        'status',
        sa.Enum('COMPLETED', 'RUNNING',
                'INTERRUPTED', 'CANCELED',
                'WAITING', 'ERROR',
                'PENDING',
                name='StatusExecutionEnumType'),
        nullable=False))


def downgrade():
    op.drop_column('job_step_log', 'status')
    op.drop_column('job_step', 'task_name')
