import json
from datetime import datetime
from functools import partial
import stand.util
from flask import url_for
from stand.models import Job, StatusExecution, JobException, db, StatusExecution


def job_stop_url(job_id): return f'/jobs/{job_id}/stop'


def job_lock_url(job_id): return f'/jobs/{job_id}/lock'
def job_unlock_url(job_id): return f'/jobs/{job_id}/unlock'


HEADERS = {'X-Auth-Token': '123456', 'Content-Type': 'application/json'}


def test_stop_job_workflow_running_success(client, app, redis_store):
    redis_store.flushdb()
    headers = {'X-Auth-Token': str(client.secret)}
    job_id = 2000
    with app.app_context():
        job = Job(id=job_id, workflow_id=1000, cluster_id=1, workflow_name='DEL',
                  user_id=1, user_name='AA', user_login='aa',
                  status=StatusExecution.RUNNING,
                  workflow_definition=json.dumps({'id': 1}))
        db.session.add(job)
        db.session.commit()

        redis_store.hset('job_{}'.format(job_id), 'status', job.status)

    response = client.post(job_stop_url(job_id=job_id), headers=headers)
    result = response.json

    assert result['data']['status'] == StatusExecution.CANCELED
    assert result['status'] == 'OK'
    assert response.status_code == 200

    assert redis_store.hget(
        f'job_{job_id}', 'status') == StatusExecution.CANCELED
    with app.app_context():
        db.session.delete(job)
        db.session.commit()


def test_stop_job_workflow_not_running_success(client, app):
    headers = {'X-Auth-Token': str(client.secret)}
    job_id = 11000
    with app.app_context():
        job = Job(id=job_id, workflow_id=11000, cluster_id=1, workflow_name='DEL',
                  user_id=1, user_name='AAX', user_login='aaa',
                  status=StatusExecution.COMPLETED,
                  workflow_definition=json.dumps({'id': 1}))
        db.session.add(job)
        db.session.commit()

    response = client.post(job_stop_url(job_id=job_id), headers=headers)
    result = response.json
    assert response.status_code == 400
    assert result['status'] == 'ERROR'
    assert result['code'] == 'ALREADY_FINISHED', result

    with app.app_context():
        db.session.delete(job)
        db.session.commit()


def test_stop_job_dont_exist_failure(client):
    data = {}
    headers = {'Content-Type': 'application/json'}
    headers.update(HEADERS)
    response = client.post(job_stop_url(job_id=1000), headers=headers,
                           data=json.dumps(data))
    result = response.json
    assert response.status_code == 404
    assert result['status'] == 'ERROR'


def test_stop_job_with_terminate_success(client):
    assert True


def test_stop_job_with_terminate_failure(client):
    assert True


def test_stop_partial_job_success(client):
    assert True


def test_stop_partial_job_not_running_failure(client):
    assert True


def test_start_partial_job_success(client):
    assert True


def test_start_partial_job_missing_id_task_failure(client):
    assert True


def test_fetch_result_data_job_sucess(client):
    assert True


def test_fetch_result_job_sucess(client):
    assert True


def test_fetch_all_result_job_sucess(client):
    """ Download """
    assert True


def test_lock_job_by_id_api_success(client, app, redis_store):
    job_id = 1732
    headers = {'X-Auth-Token': str(client.secret)}
    with app.app_context():
        job = Job(id=job_id, workflow_id=1000, cluster_id=1, workflow_name='DEL',
                  user_id=1, user_name='AA', user_login='aa',
                  status=StatusExecution.WAITING,
                  workflow_definition=json.dumps({'id': 1}))
        db.session.add(job)
        db.session.commit()

        data = {
            'user': {'id': 2142, 'name': 'Speed labs'},
            'computer': 'artemis.speed',
        }
        # Monkey patch
        locked_at = datetime(2010, 1, 20, 14, 12, 11)
        stand.util.get_now = lambda: locked_at
    response = client.post(job_lock_url(job_id=job_id),
                               headers=headers, json=data)
    result = response.json
    assert response.status_code == 200, response.json
    assert result['status'] == 'OK'

    queued = redis_store.hget('job_{}'.format(job_id), 'lock')
    assert queued != ''

    lock_info = json.loads(queued)
    assert lock_info['user']['id'] == data['user']['id']
    assert lock_info['user']['name'] == data['user']['name']
    assert lock_info['computer'] == data['computer']
    assert lock_info['date'] == locked_at.isoformat()
    
    with app.app_context():
        db.session.delete(job)
        db.session.commit()


def test_unlock_job_by_id_api_success(client):
    assert True


def test_lock_job_by_id_api_already_locked_failure(client, app, redis_store):
    job_id = 5000
    with app.app_context():
        job = Job(id=job_id, workflow_id=1000, cluster_id=1, workflow_name='DEL',
                  user_id=1, user_name='AA', user_login='BBB',
                  status=StatusExecution.WAITING,
                  workflow_definition=json.dumps({'id': 1}))
        db.session.add(job)
        db.session.commit()

    since = stand.util.get_now().isoformat()
    data1 = {
        'user': {'id': 200, 'name': 'Speed labs'},
        'computer': 'artemis.speed',
        'date': since
    }
    # Records lock information
    redis_store.hset('job_{}'.format(job_id), 'lock', json.dumps(data1))

    data2 = {
        'user': {'id': 300, 'name': 'BigSea'},
        'computer': 'eubra.bigsea',
    }
    headers = {'Content-Type': 'application/json'}
    headers.update(HEADERS)

    response = client.post(job_lock_url(job_id=job_id),
                           headers=headers, data=json.dumps(data2))

    result = response.json
    assert response.status_code == 409, response.json
    assert result['status'] == 'ERROR'
    assert result['message'] == ('Job {job} is locked by {user} '
                                 '@ {computer} since {date}').format(
        job=job_id, user=data1['user']['name'], computer=data1['computer'],
        date=since)
    with app.app_context():
        db.session.delete(job)
        db.session.commit()


def test_unlock_job_by_id_api_failure(client):
    assert True
