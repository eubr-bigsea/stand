"""empty message

Revision ID: 8c5a3ad38a4a
Revises: 82c208c2ff38
Create Date: 2020-08-11 17:42:17.558137

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = '8c5a3ad38a4a'
down_revision = '82c208c2ff38'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('cluster', 'auth_token',
               existing_type=mysql.VARCHAR(collation='utf8_unicode_ci', length=1000),
               nullable=True)
    op.alter_column('cluster', 'general_parameters',
               existing_type=mysql.VARCHAR(collation='utf8_unicode_ci', length=1000),
               nullable=True)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('cluster', 'general_parameters',
               existing_type=mysql.VARCHAR(collation='utf8_unicode_ci', length=1000),
               nullable=False)
    op.alter_column('cluster', 'auth_token',
               existing_type=mysql.VARCHAR(collation='utf8_unicode_ci', length=1000),
               nullable=False)
    # ### end Alembic commands ###
