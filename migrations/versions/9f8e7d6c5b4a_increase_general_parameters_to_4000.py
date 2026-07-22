"""increase general_parameters size to 4000

Revision ID: 9f8e7d6c5b4a
Revises: 1099ab576c7e
Create Date: 2026-07-22 16:25:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql
from stand.migration_utils import is_sqlite

# revision identifiers, used by Alembic.
revision = '9f8e7d6c5b4a'
down_revision = '1099ab576c7e'
branch_labels = None
depends_on = None


def upgrade():
    if is_sqlite():
        with op.batch_alter_table('cluster') as batch_op:
            batch_op.alter_column('general_parameters',
                                  existing_type=sa.String(length=2000),
                                  type_=sa.String(length=4000),
                                  existing_nullable=True)
    else:
        op.alter_column('cluster', 'general_parameters',
               existing_type=mysql.VARCHAR(collation='utf8_unicode_ci', length=2000),
               type_=sa.String(length=4000),
               existing_nullable=True)


def downgrade():
    if is_sqlite():
        with op.batch_alter_table('cluster') as batch_op:
            batch_op.alter_column('general_parameters',
                                  existing_type=sa.String(length=4000),
                                  type_=sa.String(length=2000),
                                  existing_nullable=True)
    else:
        op.alter_column('cluster', 'general_parameters',
               existing_type=mysql.VARCHAR(collation='utf8_unicode_ci', length=4000),
               type_=sa.String(length=2000),
               existing_nullable=True)
