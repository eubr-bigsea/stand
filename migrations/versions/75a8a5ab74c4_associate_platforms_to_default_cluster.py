"""Associate platforms to default cluster

Revision ID: 75a8a5ab74c4
Revises: 8c5a3ad38a4a
Create Date: 2021-08-23 11:44:56.973235

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '75a8a5ab74c4'
down_revision = '8c5a3ad38a4a'
branch_labels = None
depends_on = None


def upgrade():
    op.get_bind().execute("""
        INSERT INTO cluster_platform(cluster_id, platform_id)
        SELECT 1, p.id FROM (SELECT 1 as id UNION SELECT 4 UNION SELECT 5) p 
        WHERE p.id IN (1, 4, 5) AND p.id NOT IN (
             SELECT platform_id FROM cluster_platform 
             WHERE cluster_id = 1);
    """)

def downgrade():
    op.get_bind().execute(
        'DELETE FROM cluster_platform WHERE cluster_id = 1 AND platform_id = 1')
    op.get_bind().execute(
        'DELETE FROM cluster_platform WHERE cluster_id = 1 AND platform_id = 4')
    op.get_bind().execute(
        'DELETE FROM cluster_platform WHERE cluster_id = 1 AND platform_id = 5')
