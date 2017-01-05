import json
from functools import partial

from flask import url_for
from stand.models import StatusExecution, JobException

job_stop_url = partial(url_for, endpoint='jobstopactionapi')

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
    assert response.status_code == 200
    assert result['data']['status'] == fake_job.status
    assert result['status'] == 'OK'
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


def test_stop_job_with_shutdown_success(client):
    assert False


def test_stop_job_with_shutdown_failure(client):
    assert False


def test_stop_partial_job_success(client):
    assert False


def test_stop_partial_job_not_running_failure(client):
    assert False


def test_start_partial_job_success(client):
    assert False


def test_start_partial_job_missing_id_task_failure(client):
    assert False


def test_fetch_result_data_job_sucess(client):
    assert False


def test_fetch_result_job_sucess(client):
    assert False


def test_fetch_all_result_job_sucess(client):
    """ Download """
    assert False
