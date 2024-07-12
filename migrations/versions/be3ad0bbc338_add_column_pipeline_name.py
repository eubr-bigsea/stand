"""Add column pipeline_name

Revision ID: be3ad0bbc338
Revises: 4fe0790b8423
Create Date: 2024-07-12 10:17:34.043475

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = 'be3ad0bbc338'
down_revision = '4fe0790b8423'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('pipeline_run', sa.Column('pipeline_name', sa.String(length=200), nullable=False))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('pipeline_run', 'pipeline_name')
