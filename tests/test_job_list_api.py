# -*- coding: utf-8 -*-
import json
from functools import partial

from flask import url_for
from stand.models import StatusExecution
from stand.services.tahiti_service import TahitiService

job_list_url = partial(url_for, endpoint='joblistapi')
job_stop_url = partial(url_for, endpoint='jobstopactionapi')

HEADERS = {'X-Auth-Token': '123456'}


def test_list_jobs_api_unauthorized(client):
    jobs = client.get(job_list_url())
    assert (jobs.status_code == 401)


def test_list_empty_list_jobs_api(client):
    response = client.get(job_list_url(), headers=HEADERS)
    assert (response.status_code == 200)
    assert (type(response.json['data']) == list)
    assert (len(response.json['data']) == 0)


def test_list_2_jobs_api(client, model_factories):
    model_factories.job_factory.create(id=1001)
    model_factories.job_factory.create(id=1002)

    response = client.get(job_list_url(), headers=HEADERS)

    assert (response.status_code == 200)
    assert (type(response.json['data']) == list)
    assert (len(response.json['data']) == 2)


def test_list_job_return_expected_fields_in_json(client, model_factories):
    model_factories.job_factory.create(
        id=3001, cluster_id=1,
        workflow_definition=json.dumps({'id': 4444, 'name': 'WS'}))
    response = client.get(job_list_url(), headers=HEADERS)

    job = response.json['data'][0]
    assert (len(set(job.keys()) - {'status', 'created', 'started', 'workflow',
                                   'cluster', 'finished', 'user', 'id'}) == 0)

    assert (len(set(job['workflow'].keys()) - {'id', 'name'}) == 0)
    assert (len(set(job['cluster'].keys()) -
                {'id', 'name', 'address', 'type'}) == 0)
    assert (len(set(job['user'].keys()) - {'id', 'name', 'login'}) == 0)


def test_list_filter_jobs_by_workflow_returns_2_of_3(client, model_factories):
    workflow_id = 9001
    model_factories.job_factory.create(
        id=3001, cluster_id=1,
        workflow_id=workflow_id,
        workflow_definition=json.dumps({'id': workflow_id, 'name': 'WS'}))
    model_factories.job_factory.create(
        id=3002, cluster_id=1, workflow_id=workflow_id,
        workflow_definition=json.dumps({'id': workflow_id, 'name': 'WS'}))

    # This should not be returned
    model_factories.job_factory.create(
        id=3003, cluster_id=1, workflow_id=workflow_id - 1000,
        workflow_definition=json.dumps(
            {'id': workflow_id - 1000, 'name': 'WS'}))

    data = {
        'workflow_id': workflow_id
    }
    response = client.get(job_list_url(), headers=HEADERS, query_string=data)
    jobs = response.json['data']
    workflow_ids = [j['workflow']['id'] for j in jobs]
    assert len(workflow_ids) == 2
    assert sorted(workflow_ids) == [workflow_id, workflow_id]


def test_list_filter_jobs_by_user_returns_1_of_3(client, model_factories):
    user_id = 17
    model_factories.job_factory.create(id=3001, user_id=user_id)
    # These should not be returned
    model_factories.job_factory.create(id=3002, user_id=user_id + 1)
    model_factories.job_factory.create(id=3003, user_id=user_id * 4)

    data = {
        'user_id': user_id
    }
    response = client.get(job_list_url(), headers=HEADERS, query_string=data)
    jobs = response.json['data']
    users_id = [j['user']['id'] for j in jobs]
    assert len(users_id) == 1
    assert sorted(users_id) == [user_id]


def test_list_jobs_paged_return_page_1_no_more_pages(client, model_factories):
    model_factories.job_factory.create_batch(20)

    data = {
        'page': 1,
        'size': 40
    }
    response = client.get(job_list_url(), headers=HEADERS, query_string=data)
    jobs = response.json['data']
    pager = response.json['pagination']
    assert len(jobs) == 20
    assert pager['page'] == data['page']
    assert pager['size'] == data['size']
    assert pager['pages'] == 1


def test_list_jobs_paged_return_page_1_with_more_4_pages(client,
                                                         model_factories):
    model_factories.job_factory.create_batch(20)

    for i in xrange(1, 6):
        data = {
            'page': i,
            'size': 5
        }
        response = client.get(job_list_url(), headers=HEADERS,
                              query_string=data)

        if i < 5:
            jobs = response.json['data']
            pager = response.json['pagination']
            assert len(jobs) == data['size']
            assert pager['page'] == data['page']
            assert pager['size'] == data['size']
            assert pager['pages'] == 4
        else:
            assert response.status_code == 404


def test_list_jobs_paged_out_of_bounds_return_404(client, model_factories):
    model_factories.job_factory.create_batch(5)

    data = {
        'page': 20,
        'size': 5
    }
    response = client.get(job_list_url(), headers=HEADERS, query_string=data)
    assert response.status_code == 404


def test_create_job_ok_result_success(client, model_factories, redis_store):
    model_factories.cluster_factory.create(id=999, )
    workflow_id = 281
    data = {
        'user': {
            'id': 1,
            'login': 'turing',
            'name': 'Alan Turing'
        },
        'workflow': {
            'name': 'Titanic',
            'id': workflow_id,
            'platform': {
                'id': 1
            },
            'tasks': [
                {
                    'id': '2323aa-2323dac-as9987',
                    'forms': {},
                    'operation': {
                        'id': 1
                    }
                }
            ]
        },
        'cluster': {
            'id': 999
        },

    }
    headers = {'Content-Type': 'application/json'}
    headers.update(HEADERS)

    # Monkey patching remove API services
    def get_workflow(instance, workflow_id):
        return {'name': 'OK'}

    def get_cluster(instance, cluseter_id):
        return {"name": 'Teste'}

    setattr(TahitiService, 'get_workflow', get_workflow)
    setattr(TahitiService, 'get_cluster', get_cluster)

    response = client.post(job_list_url(), headers=headers,
                           data=json.dumps(data))

    assert response.status_code == 200, response.json
    job_id = response.json['data']['id']
    assert job_id is not None

    queued = redis_store.get('queue_start')[0]
    assert json.loads(queued)['workflow_id'] == \
           response.json['data']['workflow']['id']

    status = redis_store.hget('record_workflow_{}'.format(workflow_id),
                              'status')
    assert status == StatusExecution.WAITING


def test_create_job_nok_result_fail_missing_fields(client, model_factories):
    model_factories.cluster_factory.create(id=999)

    data = {

    }
    headers = {'Content-Type': 'application/json'}
    headers.update(HEADERS)
    response = client.post(job_list_url(), headers=headers,
                           data=json.dumps(data))
    result = response.json

    assert result['status'] == 'ERROR'
    assert result['message'] == 'Validation error'

    assert sorted(result['errors'].keys()) == sorted(
        ['cluster', 'user', 'workflow'])

    assert response.status_code == 401


def test_create_job_invalid_cluster_fail(client, model_factories, redis_store):
    model_factories.cluster_factory.create(id=999)
    redis_store.flushdb()

    data = {}
    headers = {'Content-Type': 'application/json'}
    headers.update(HEADERS)
    response = client.post(job_list_url(), headers=headers,
                           data=json.dumps(data))
    result = response.json
    assert response.status_code == 401
    assert result['status'] == 'ERROR'
    assert result['message'] == 'Validation error'

    assert sorted(result['errors'].keys()) == sorted(
        ['cluster', 'user', 'workflow'])
    content = redis_store.get('start')
    assert len(content) == 0


def test_create_job_workflow_running_another_job_fail(client, model_factories):
    workflow_id = 10000
    model_factories.job_factory.create(
        id=444, status=StatusExecution.RUNNING, workflow_id=workflow_id)

    data = {
        'user': {
            'id': 1,
            'login': 'turing',
            'name': 'Alan Turing'
        },
        'workflow': {
            'name': 'Titanic',
            'id': workflow_id,
            'platform': {
                'id': 1
            },
            'tasks': [
                {
                    'id': '2323aa-2323dac-as9987',
                    'forms': {},
                    'operation': {'id': 1}
                }
            ]
        },
        'cluster': {
            'id': 999
        },

    }
    headers = {'Content-Type': 'application/json'}
    headers.update(HEADERS)

    response = client.post(job_list_url(), headers=headers,
                           data=json.dumps(data))
    assert response.status_code == 401, response.json
    result = response.json
    assert result['status'] == 'ERROR'
    assert result['code'] == 'ALREADY_RUNNING'
    # assert len(connect_redis_store().get('start')) == 0
