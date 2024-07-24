"""populate Cluster table

Revision ID: 2f2e97f51d5f
Revises: c5cba727841a
Create Date: 2017-05-02 14:48:34.667079

"""
from alembic import op
from sqlalchemy import String, Integer
from sqlalchemy.sql import table, column, text

# revision identifiers, used by Alembic.
revision = '2f2e97f51d5f'
down_revision = 'c5cba727841a'
branch_labels = None
depends_on = None

cluster_table = table('cluster',
                      column("id", Integer),
                      column("name", String),
                      column('description', String),
                      column('enabled', String(200)),
                      column('type', String),
                      column('address', String),
                      )


def upgrade():
    op.bulk_insert(cluster_table,
                   [
                       {
                           'id': 1,
                           'name': 'Default cluster',
                           'description': 'Default cluster - change it',
                           'enabled': 1,
                           'type': 'SPARK_LOCAL',
                           'address': 'localhost'
                       }
                   ])


def downgrade():
    op.execute(text('DELETE FROM cluster WHERE id = 1'))
