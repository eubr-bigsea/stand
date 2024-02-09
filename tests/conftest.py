import os
from stand.factory import create_babel_i18n
import pytest
import datetime
import flask_migrate
from stand.app import create_app
from stand.models import (Job, ClusterType, Cluster,
                          JobType, StatusExecution, db)
from stand.services.redis_service import connect_redis_store

@pytest.fixture(scope='session')
def redis_store(app):
    with app.app_context():
        store = connect_redis_store(None, True)
        store.flushdb()
        return store

def get_clusters():
    v1 = Cluster(
        id=2, name="Cluster 1", description="Cluster 1",
        enabled=True, type=ClusterType.SPARK_LOCAL,
        address='local', executors=1, executor_cores=1,
        executor_memory='1G', general_parameters='{}')

    v2 = Cluster(
        id=3, name="Cluster 2", description="Cluster 2",
        enabled=True, type=ClusterType.KUBERNETES,
        address='kb8://', executors=2, executor_cores=2,
        executor_memory='2G', general_parameters='{}')
    v3 = Cluster(
        id=4, name="Cluster 3", description="Cluster 3",
        enabled=False, type=ClusterType.YARN,
        address='local', executors=1, executor_cores=1,
        executor_memory='1G', general_parameters='{}')

    return [v1, v2, v3]


def get_jobs():
    now = datetime.datetime.now()
    j1 = Job(
        id=1, created=now, name="Job 1 - Test", type=JobType.NORMAL,
        started=now, finished=now, status=StatusExecution.WAITING,
        status_text='Nothing', workflow_id=1,
        workflow_name='WF1', workflow_definition='{"id": 1}',
        user_id=1, user_login='admin', user_name='Admin',
        job_key='1111', cluster_id=1,
        steps=[], results=[]
    )
    j2 = Job(
        id=2, created=now, name="Job 2 - Test", type=JobType.NORMAL,
        started=now, finished=now, status=StatusExecution.WAITING,
        status_text='Nothing', workflow_id=2,
        workflow_name='WF2', workflow_definition='{"id": 2}',
        user_id=2, user_login='Someone', user_name='Someone',
        job_key='2222', cluster_id=1,
        steps=[], results=[]
    )
    j3 = Job(
        id=3, created=now, name="Job 3", type=JobType.NORMAL,
        started=now, finished=now, status=StatusExecution.WAITING,
        status_text='Nothing', workflow_id=1,
        workflow_name='WF3', workflow_definition='{"id": 1}',
        user_id=1, user_login='admin', user_name='Admin',
        job_key='3333', cluster_id=1,
        steps=[], results=[]
    )
    return [j1, j2, j3]


@pytest.fixture(scope='session')
def app():
    return create_app()

@pytest.fixture(scope='session')
def client(app):

    path = os.path.dirname(os.path.abspath(__name__))
    app.config['SQLALCHEMY_DATABASE_URI'] = f'sqlite:///{path}/test.db'
    app.config['TESTING'] = True
    # import pdb; pdb.set_trace()
    with app.test_client() as client:
        client.app = app
        create_babel_i18n(app)
        with app.app_context():
            if os.path.exists(os.path.join(path, 'test.db')):
                os.remove(os.path.join(path, 'test.db'))
            # flask_migrate.downgrade(revision="base")
            flask_migrate.upgrade(revision='head')
            for cluster in get_clusters():
                db.session.add(cluster)
            for job in get_jobs():
                db.session.add(job)
            client.secret = app.config['STAND_CONFIG']['secret']
            db.session.commit()
        yield client

@pytest.fixture(scope='session')
def get_pipelines():
    return [
        {
            'id': 1,
            'name': 'Pipeline 1',
            'enabled': True,
            'steps': [
                {
                    'id': 1,
                    'name': 'Step 1',
                    'workflow_id': 1,
                    'workflow_name': 'WF1',
                    'scheduling': {}
                },
                {
                    'id': 2,
                    'name': 'Step 2',
                    'workflow_id': 2,
                    'workflow_name': 'WF2',
                    'scheduling': {}
                }
            ]
        }
    ]