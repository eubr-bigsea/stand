"""empty message

Revision ID: 306f880b11c3
Revises: 255f81eff867
Create Date: 2018-08-03 15:07:44.354557

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = '306f880b11c3'
down_revision = '255f81eff867'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("UPDATE cluster \
            SET description='Spark local - 1 core / 1GB RAM', \
            address='local[*]',  \
            executor_cores=1,  \
            executor_memory='1G',  \
            executors=1  \
            WHERE id=1;")

def downgrade():
    op.execute("UPDATE cluster \
            SET description='Default cluster - change it', \
            address='localhost',  \
            executor_cores=0,  \
            executor_memory='',  \
            executors=0  \
            WHERE id=1;")
