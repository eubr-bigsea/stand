"""new tables for pipeline

Revision ID: d7b52a7aaacc
Revises: 025b039de38c
Create Date: 2024-02-09 17:48:49.521058

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd7b52a7aaacc'
down_revision = '025b039de38c'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('pipeline_step_run_log',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('created', sa.DateTime(), nullable=False),
    sa.Column('action', sa.String(length=200), nullable=False),
    sa.Column('user_id', sa.Integer(), nullable=False),
    sa.Column('user_login', sa.String(length=200), nullable=False),
    sa.Column('user_name', sa.String(length=200), nullable=False),
    sa.Column('comment', sa.String(length=200), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('pipeline_run',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('start', sa.DateTime(), nullable=False),
    sa.Column('end', sa.DateTime(), nullable=False),
    sa.Column('pipeline_id', sa.Integer(), nullable=False),
    sa.Column('comment', sa.String(length=200), nullable=True),
    sa.Column('status', sa.Enum('COMPLETED', 'ERROR', 'INTERRUPTED', 'PENDING', 'RUNNING', 'WAITING', 'CANCELED', name='StatusExecutionEnumType'), nullable=False),
    sa.Column('final_status', sa.Enum('COMPLETED', 'ERROR', 'INTERRUPTED', 'PENDING', 'RUNNING', 'WAITING', 'CANCELED', name='StatusExecutionEnumType'), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_pipeline_run_end'), 'pipeline_run', ['end'], unique=False)
    op.create_index(op.f('ix_pipeline_run_pipeline_id'), 'pipeline_run', ['pipeline_id'], unique=False)
    op.create_index(op.f('ix_pipeline_run_start'), 'pipeline_run', ['start'], unique=False)
    op.create_table('pipeline_step_run',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('created', sa.DateTime(), nullable=False),
    sa.Column('updated', sa.DateTime(), nullable=False),
    sa.Column('workflow_id', sa.Integer(), nullable=False),
    sa.Column('retries', sa.Integer(), nullable=False),
    sa.Column('comment', sa.String(length=200), nullable=True),
    sa.Column('status', sa.Enum('COMPLETED', 'ERROR', 'INTERRUPTED', 'PENDING', 'RUNNING', 'WAITING', 'CANCELED', name='StatusExecutionEnumType'), nullable=False),
    sa.Column('final_status', sa.Enum('COMPLETED', 'ERROR', 'INTERRUPTED', 'PENDING', 'RUNNING', 'WAITING', 'CANCELED', name='StatusExecutionEnumType'), nullable=True),
    sa.Column('pipeline_run_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['pipeline_run_id'], ['pipeline_run.id'], name='fk_pipeline_step_run_pipeline_run_id'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_pipeline_step_run_pipeline_run_id'), 'pipeline_step_run', ['pipeline_run_id'], unique=False)
    op.create_index(op.f('ix_pipeline_step_run_updated'), 'pipeline_step_run', ['updated'], unique=False)
    op.create_index(op.f('ix_pipeline_step_run_workflow_id'), 'pipeline_step_run', ['workflow_id'], unique=False)
    op.create_table('job_pipeline_step_run',
    sa.Column('pipeline_step_run_id', sa.Integer(), nullable=False),
    sa.Column('job_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['job_id'], ['job.id'], ),
    sa.ForeignKeyConstraint(['pipeline_step_run_id'], ['pipeline_step_run.id'], ),
    info={'bind_key': None}
    )
    op.create_index(op.f('ix_job_pipeline_step_run_job_id'), 'job_pipeline_step_run', ['job_id'], unique=False)
    op.create_index(op.f('ix_job_pipeline_step_run_pipeline_step_run_id'), 'job_pipeline_step_run', ['pipeline_step_run_id'], unique=False)
    
    # op.alter_column('cluster', 'enabled',
    #            existing_type=sa.VARCHAR(length=200),
    #            type_=sa.Boolean(),
    #            existing_nullable=False)
    # op.alter_column('cluster', 'general_parameters',
    #            existing_type=sa.VARCHAR(length=1000),
    #            type_=sa.String(length=3000),
    #            existing_nullable=True,
    #            existing_server_default=sa.text("('')"))
    op.create_index(op.f('ix_cluster_access_cluster_id'), 'cluster_access', ['cluster_id'], unique=False)
    op.create_index(op.f('ix_cluster_configuration_cluster_id'), 'cluster_configuration', ['cluster_id'], unique=False)
    op.create_index(op.f('ix_cluster_flavor_cluster_id'), 'cluster_flavor', ['cluster_id'], unique=False)
    op.create_index(op.f('ix_cluster_platform_cluster_id'), 'cluster_platform', ['cluster_id'], unique=False)
    op.add_column('job', sa.Column('trigger_type', sa.Enum('MANUAL', 'TIME_SCHEDULE', 'MESSAGE', 'API', 'WATCH', 'OTHER', name='TriggerTypeEnumType'), server_default='MANUAL', nullable=False))
    # op.alter_column('job', 'type',
    #            existing_type=sa.VARCHAR(length=6),
    #            type_=sa.Enum('APP', 'BATCH', 'MODEL_BUILDER', 'NORMAL', name='JobTypeEnumType'),
    #            existing_nullable=False,
    #            existing_server_default=sa.text("'NORMAL'"))
    # op.alter_column('job', 'job_key',
    #            existing_type=sa.VARCHAR(length=200),
    #            nullable=False)
    op.create_index(op.f('ix_job_cluster_id'), 'job', ['cluster_id'], unique=False)
    op.create_index(op.f('ix_job_result_job_id'), 'job_result', ['job_id'], unique=False)
    op.create_index(op.f('ix_job_step_job_id'), 'job_step', ['job_id'], unique=False)
    op.create_index(op.f('ix_job_step_log_step_id'), 'job_step_log', ['step_id'], unique=False)
    op.create_index(op.f('ix_room_participant_room_id'), 'room_participant', ['room_id'], unique=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_room_participant_room_id'), table_name='room_participant')
    op.drop_index(op.f('ix_job_step_log_step_id'), table_name='job_step_log')
    op.drop_index(op.f('ix_job_step_job_id'), table_name='job_step')
    op.drop_index(op.f('ix_job_result_job_id'), table_name='job_result')
    op.drop_index(op.f('ix_job_cluster_id'), table_name='job')
    # op.alter_column('job', 'job_key',
    #            existing_type=sa.VARCHAR(length=200),
    #            nullable=True)
    # op.alter_column('job', 'type',
    #            existing_type=sa.Enum('APP', 'BATCH', 'MODEL_BUILDER', 'NORMAL', name='JobTypeEnumType'),
    #            type_=sa.VARCHAR(length=6),
    #            existing_nullable=False,
    #            existing_server_default=sa.text("'NORMAL'"))
    op.drop_column('job', 'trigger_type')
    op.drop_index(op.f('ix_cluster_platform_cluster_id'), table_name='cluster_platform')
    op.drop_index(op.f('ix_cluster_flavor_cluster_id'), table_name='cluster_flavor')
    op.drop_index(op.f('ix_cluster_configuration_cluster_id'), table_name='cluster_configuration')
    op.drop_index(op.f('ix_cluster_access_cluster_id'), table_name='cluster_access')
    # op.alter_column('cluster', 'general_parameters',
    #            existing_type=sa.String(length=3000),
    #            type_=sa.VARCHAR(length=1000),
    #            existing_nullable=True,
    #            existing_server_default=sa.text("('')"))
    # op.alter_column('cluster', 'enabled',
    #            existing_type=sa.Boolean(),
    #            type_=sa.VARCHAR(length=200),
    #            existing_nullable=False)
    op.drop_index(op.f('ix_job_pipeline_step_run_pipeline_step_run_id'), table_name='job_pipeline_step_run')
    op.drop_index(op.f('ix_job_pipeline_step_run_job_id'), table_name='job_pipeline_step_run')
    op.drop_table('job_pipeline_step_run')
    op.drop_index(op.f('ix_pipeline_step_run_workflow_id'), table_name='pipeline_step_run')
    op.drop_index(op.f('ix_pipeline_step_run_updated'), table_name='pipeline_step_run')
    op.drop_index(op.f('ix_pipeline_step_run_pipeline_run_id'), table_name='pipeline_step_run')
    op.drop_table('pipeline_step_run')
    op.drop_index(op.f('ix_pipeline_run_start'), table_name='pipeline_run')
    op.drop_index(op.f('ix_pipeline_run_pipeline_id'), table_name='pipeline_run')
    op.drop_index(op.f('ix_pipeline_run_end'), table_name='pipeline_run')
    op.drop_table('pipeline_run')
    op.drop_table('pipeline_step_run_log')
    # ### end Alembic commands ###
