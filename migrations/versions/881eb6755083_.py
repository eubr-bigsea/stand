"""empty message

Revision ID: 881eb6755083
Revises: 989ff41dc23a
Create Date: 2017-09-11 16:46:28.607116

"""
import sqlalchemy as sa
from alembic import op
from stand.migration_utils import is_sqlite

# revision identifiers, used by Alembic.
revision = '881eb6755083'
down_revision = '989ff41dc23a'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('job', sa.Column('source_code', sa.Text(), nullable=True))


def downgrade():
    if is_sqlite():
        with op.batch_alter_table('job') as batch_op:
            batch_op.drop_column('source_code')
    else:
        op.drop_column('job', 'source_code')
