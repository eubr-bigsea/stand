import json
from datetime import datetime
from functools import partial

import mock
import stand.util
from flask import url_for
from stand.models import StatusExecution, JobException

job_stop_url = partial(url_for, endpoint='jobstopactionapi')
job_lock_url = partial(url_for, endpoint='joblockactionapi')
job_unlock_url = partial(url_for, endpoint='jobunlockactionapi')

HEADERS = {'X-Auth-Token': '123456', 'Content-Type': 'application/json'}


def test_stop_job_workflow_running_success(client, model_factories,
                                           redis_store):
    fake_job = model_factories.job_factory.create(
        id=444, status=StatusExecution.RUNNING)
    redis_store.hset('job_{}'.format(fake_job.id), 'status', fake_job.status)

    data = {}
    headers = {'Content-Type': 'application/json'}
    headers.update(HEADERS)
    response = client.post(job_stop_url(job_id=fake_job.id), headers=headers,
                           data=json.dumps(data))

    result = response.json

    assert result['data']['status'] == StatusExecution.CANCELED
    assert result['status'] == 'OK'
    assert response.status_code == 200

    assert redis_store.hget('job_{}'.format(fake_job.id),
                            'status') == StatusExecution.CANCELED


def test_stop_job_workflow_not_running_success(client, model_factories):
    fake_job = model_factories.job_factory.create(
        id=456, status=StatusExecution.COMPLETED)

    data = {}
    headers = {'Content-Type': 'application/json'}
    headers.update(HEADERS)
    response = client.post(job_stop_url(job_id=fake_job.id), headers=headers,
                           data=json.dumps(data))
    result = response.json
    assert response.status_code == 401
    assert result['status'] == 'ERROR'
    assert result['code'] == JobException.ALREADY_FINISHED


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


def test_lock_job_by_id_api_success(client, model_factories, redis_store):
    fake_job = model_factories.job_factory.create(
        id=456, status=StatusExecution.WAITING)

    data = {
        'user': {'id': 2142, 'name': 'Speed labs'},
        'computer': 'artemis.speed',
    }
    headers = {'Content-Type': 'application/json'}
    headers.update(HEADERS)

    # Monkey patch
    locked_at = datetime(2010, 1, 20, 14, 12, 11)
    with mock.patch('stand.util.get_now') as patched:
        patched.return_value = locked_at
        response = client.post(job_lock_url(job_id=fake_job.id),
                               headers=headers, data=json.dumps(data))
        result = response.json
        assert response.status_code == 200, response.json
        assert result['status'] == 'OK'

        queued = redis_store.hget('job_{}'.format(fake_job.id), 'lock')
        assert queued != ''

        lock_info = json.loads(queued)
        assert lock_info['user']['id'] == data['user']['id']
        assert lock_info['user']['name'] == data['user']['name']
        assert lock_info['computer'] == data['computer']
        assert lock_info['date'] == locked_at.isoformat()


def test_unlock_job_by_id_api_success(client, model_factories):
    assert True


def test_lock_job_by_id_api_already_locked_failure(client, model_factories,
                                                   redis_store):
    fake_job = model_factories.job_factory.create(
        id=999, status=StatusExecution.WAITING)

    since = stand.util.get_now().isoformat()
    data1 = {
        'user': {'id': 200, 'name': 'Speed labs'},
        'computer': 'artemis.speed',
        'date': since
    }
    # Records lock information
    redis_store.hset('job_{}'.format(fake_job.id), 'lock', json.dumps(data1))

    data2 = {
        'user': {'id': 300, 'name': 'BigSea'},
        'computer': 'eubra.bigsea',
    }
    headers = {'Content-Type': 'application/json'}
    headers.update(HEADERS)

    response = client.post(job_lock_url(job_id=fake_job.id),
                           headers=headers, data=json.dumps(data2))

    result = response.json
    assert response.status_code == 409, response.json
    assert result['status'] == 'ERROR'
    assert result['message'] == ('Job {job} is locked by {user} '
                                 '@ {computer} since {date}').format(
        job=fake_job.id, user=data1['user']['name'], computer=data1['computer'],
        date=since)


def test_unlock_job_by_id_api_failure(client, model_factories):
    assert True
