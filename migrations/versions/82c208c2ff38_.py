"""empty message

Revision ID: 82c208c2ff38
Revises: 35693eb8cdde
Create Date: 2020-08-10 18:54:03.784044

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '82c208c2ff38'
down_revision = '35693eb8cdde'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('job', sa.Column('type', sa.Enum('APP', 'NORMAL', name='JobTypeEnumType'), 
        nullable=False, server_default='NORMAL'))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('job', 'type')
    # ### end Alembic commands ###
