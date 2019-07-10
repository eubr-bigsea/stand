"""empty message

Revision ID: bfd47ab5b430
Revises: 03dbc173d79a
Create Date: 2019-07-10 15:44:31.021080

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import String, Integer
from sqlalchemy.sql import table, column, text
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = 'bfd47ab5b430'
down_revision = '03dbc173d79a'
branch_labels = None
depends_on = None

cluster_table = table('cluster',
                      column("id", Integer),
                      column("name", String),
                      column('description', String),
                      column('enabled', String(200)),
                      column('type', String),
                      column('address', String),
                      column('executor_cores', Integer),
                      column('executor_memory', String),
                      column('executors', Integer),
                      column('general_parameters', String),
                      )

def upgrade():
    op.bulk_insert(cluster_table,
                   [
                       {
                           'id': 2,
                           'name': 'K8s cluster',
                           'description': 'Spark K8s - 1 core/1 GB RAM',
                           'enabled': '1',
                           'type': 'KUBERNETES',
                           'address': 'k8s://https://kubernetes.default.svc.cluster.local:443',
                           'executor_cores': '1',
                           'executor_memory': '1GB',
                           'executors': '1',
                           'general_parameters': '--conf spark.kubernetes.namespace=lemonade-dev --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-sa --conf spark.driver.host=juicer.lemonade-dev.svc.cluster.local --conf spark.driver.port=29413 --conf spark.kubernetes.container.image=lucasmsp/juicer:spark',
                       }
                   ])


def downgrade():
    op.execute(text('DELETE FROM cluster WHERE id = 2'))
