"""Add columns task_name to job_step and status to job_step_log

Revision ID: 2f2eb852559d
Revises: 4eb6f2af4b72
Create Date: 2017-11-03 15:32:13.303940

"""
import sqlalchemy as sa
from alembic import op
from stand.migration_utils import is_sqlite

# revision identifiers, used by Alembic.
revision = '2f2eb852559d'
down_revision = '4eb6f2af4b72'
branch_labels = None
depends_on = None


def upgrade():
    if is_sqlite():
        with op.batch_alter_table('job_step') as batch_op:
            batch_op.add_column(sa.Column('task_name', sa.String(length=200), nullable=True))

        with op.batch_alter_table('job_step_log') as batch_op:
            batch_op.add_column(sa.Column(
            'status',
            sa.Enum('COMPLETED', 'RUNNING',
                    'INTERRUPTED', 'CANCELED',
                    'WAITING', 'ERROR',
                    'PENDING',
                    name='StatusExecutionEnumType'), nullable=False,
                    server_default='WAITING'))
    else:
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
    if is_sqlite():
        with op.batch_alter_table('job_step_log') as batch_op:
            batch_op.drop_column('status')
        with op.batch_alter_table('job_step') as batch_op:
            batch_op.drop_column('task_name')
    else:
        op.drop_column('job_step_log', 'status')
        op.drop_column('job_step', 'task_name')
