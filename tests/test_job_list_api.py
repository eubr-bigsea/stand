# -*- coding: utf-8 -*-
import json
from functools import partial

from flask import url_for, current_app
from flask_babel import gettext
from stand.models import Job, StatusExecution, db

# partial(url_for, endpoint='joblistapi', external=False)
def job_list_url(): return '/jobs'
# partial(url_for, endpoint='jobstopactionapi', external=False)
def job_stop_url(): return '/jobs'
# partial(url_for, endpoint='jobdetailapi', external=False)
def job_detail_url(job_id): return f'/jobs/{job_id}'


HEADERS = {'X-Auth-Token': '123456'}


def test_list_jobs_api_unauthorized(client):
    jobs = client.get(job_list_url())
    assert (jobs.status_code == 401)


def test_list_empty_list_jobs_api(client):
    headers = {'X-Auth-Token': str(client.secret)}
    response = client.get(job_list_url(), headers=headers,
                          query_string={'name': 'Invalid'})
    assert (response.status_code == 200)
    assert (type(response.json['data']) == list)
    assert (len(response.json['data']) == 0)


def test_list_2_jobs_api(client):
    headers = {'X-Auth-Token': str(client.secret)}
    response = client.get(job_list_url(), headers=headers,
                          query_string={'name': 'test'})

    assert (response.status_code == 200)
    assert (type(response.json['data']) == list)
    assert (len(response.json['data']) == 2)


def test_list_job_return_expected_fields_in_json(client):
    headers = {'X-Auth-Token': str(client.secret)}
    response = client.get(job_list_url(), headers=headers)

    job = response.json['data'][0]
    fields = {'status', 'created', 'started', 'workflow', 'cluster',
              'finished', 'user', 'id', 'job_key', 'name', 'status_text', 'type'
              }
    assert len(set(job.keys()) - fields) == 0

    cluster_fields = {'description', 'executor_cores',
                      'executor_memory', 'executors', 'general_parameters',
                      'id', 'name', 'enabled', 'type'}
    assert len(set(job['workflow'].keys()) - fields) == 0

    assert len(set(job['cluster'].keys()) - cluster_fields) == 0
    assert len(set(job['user'].keys()) - {'id', 'name', 'login'}) == 0


def test_list_filter_jobs_by_workflow_returns_2_of_3(client):
    workflow_id = 1

    data = {
        'workflow_id': workflow_id
    }
    response = client.get(job_list_url(), headers=HEADERS, query_string=data)
    jobs = response.json['data']
    workflow_ids = [j['workflow']['id'] for j in jobs]
    assert len(workflow_ids) == 2
    assert sorted(workflow_ids) == [workflow_id, workflow_id]


def test_list_filter_jobs_by_user_returns_1_of_3(client):
    user_id = 2

    data = {
        'user_id': user_id
    }
    response = client.get(job_list_url(), headers=HEADERS, query_string=data)
    jobs = response.json['data']
    users_id = [j['user']['id'] for j in jobs]
    assert len(users_id) == 1
    assert sorted(users_id) == [user_id]


def test_list_jobs_paged_return_page_1_no_more_pages(client):

    data = {
        'page': 1,
        'size': 2
    }
    response = client.get(job_list_url(), headers=HEADERS, query_string=data)
    jobs = response.json['data']
    pager = response.json['pagination']
    assert len(jobs) == 2
    assert pager['page'] == data['page']
    assert pager['size'] == data['size']
    assert pager['pages'] == 2


def test_list_jobs_paged_out_of_bounds_return_404(client):
    data = {
        'page': 20,
        'size': 5
    }
    response = client.get(job_list_url(), headers=HEADERS, query_string=data)
    assert response.status_code == 404



def test_create_job_no_persist_ok_result_success(client, redis_store):
    redis_store.flushdb()
    workflow_id = 800
    headers = {'X-Auth-Token': str(client.secret)}
    data = {
        'user': {
            'id': 1,
            'login': 'turing',
            'name': 'Alan Turing'
        },
        'workflow': {
            'id': workflow_id,
            'enabled': True,
            'name': 'Titanic',
            'platform': {'id': 1},
            'tasks': [
                {
                    'id': '2323aa-2323dac-as9987',
                    'forms': {},
                    'operation': {'id': 1}
                }
            ]
        },
        'cluster': {'id': 1},
        'persist': False

    }
    response = client.post(job_list_url(), headers=headers, json=data)
    job_id = response.json['data']['id']

    assert response.status_code == 201, response.json
    assert job_id is not None

    queued = json.loads(redis_store.lrange('queue_start', 0, 1)[0])
    assert queued['workflow']['id'] == workflow_id
    assert 'workflow' not in response.json['data']
    assert queued['cluster']['id'] == data['cluster']['id']
    assert 'cluster' not in response.json['data']
    assert queued['app_configs']['persist'] == False

    status = redis_store.hget(f'record_workflow_{workflow_id}', 'status')
    assert status == StatusExecution.WAITING

def test_create_job_ok_result_success(client, redis_store):
    redis_store.flushdb()
    workflow_id = 555
    headers = {'X-Auth-Token': str(client.secret)}
    data = {
        'user': {
            'id': 1,
            'login': 'turing',
            'name': 'Alan Turing'
        },
        'workflow': {
            'id': workflow_id,
            'enabled': True,
            'name': 'Titanic',
            'platform': {'id': 1},
            'tasks': [
                {
                    'id': '2323aa-2323dac-as9987',
                    'forms': {},
                    'operation': {'id': 1}
                }
            ]
        },
        'cluster': {'id': 1},

    }

    # Monkey patching API services
    # def get_workflow(instance, workflow_id):
    #    return {'name': 'OK'}
    # def get_cluster(instance, cluseter_id):
    #    return {"name": 'Teste'}
    # setattr(TahitiService, 'get_workflow', get_workflow)
    response = client.post(job_list_url(), headers=headers, json=data)
    
    assert response.status_code == 201, response.json
    job_id = response.json['data']['id']
    assert job_id is not None

    queued = json.loads(redis_store.lrange('queue_start', 0, 1)[0])
    assert queued['workflow']['id'] == response.json['data']['workflow']['id']
    assert queued['cluster']['id'] == response.json['data']['cluster']['id']
    assert queued['app_configs']['persist'] == True

    status = redis_store.hget('record_workflow_{}'.format(workflow_id),
                              'status')
    assert status == StatusExecution.WAITING


def test_create_job_nok_result_fail_missing_fields(client):
    data = {}
    headers = {'X-Auth-Token': str(client.secret)}
    response = client.post(job_list_url(), headers=headers, json=data)
    result = response.json
    assert response.status_code == 400
    assert result['status'] == 'ERROR'
    assert result['message'] == gettext('Validation error')
    assert sorted(result['errors'].keys()) == sorted(['cluster', 'workflow'])


# def test_create_job_workflow_running_another_job_fail(client):
#     workflow_id = 10000

#     data = {
#         'user': {
#             'id': 1,
#             'login': 'turing',
#             'name': 'Alan Turing'
#         },
#         'workflow': {
#             'id': workflow_id,
#             'enabled': True,
#             'name': 'Titanic',
#             'platform': {
#                 'id': 1
#             },
#             'tasks': [
#                 {
#                     'id': '2323aa-2323dac-as9987',
#                     'forms': {},
#                     'operation': {'id': 1}
#                 }
#             ]
#         },
#         'cluster': {
#             'id': 999
#         },

#     }
#     headers = {'Content-Type': 'application/json'}
#     headers.update(HEADERS)

#     response = client.post(job_list_url(), headers=headers,
#                            data=json.dumps(data))
#     assert response.status_code == 401, response.json
#     result = response.json
#     assert result['status'] == 'ERROR'
#     assert result['code'] == 'ALREADY_RUNNING'
#     # assert len(connect_redis_store().get('start')) == 0


def test_create_job_invalid_cluster(client):
    workflow_id = 10000

    data = {
        'user': {
            'id': 1,
            'login': 'turing',
            'name': 'Alan Turing'
        },
        'workflow': {
            'id': workflow_id,
            'enabled': True,
            'name': 'Titanic',
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
    headers = {'X-Auth-Token': str(client.secret)}

    response = client.post(job_list_url(), headers=headers, json=data)
    assert response.status_code == 400, response.json
    result = response.json
    assert result['status'] == 'ERROR'
    assert list(result['errors'].keys()) == ['cluster']


def test_get_job_by_id_api_works(client):
    headers = {'X-Auth-Token': str(client.secret)}
    job_id = 1
    fake_job = Job.query.get(1)

    response = client.get(f'/jobs/{job_id}', headers=headers)
    job = response.json

    assert (fake_job.id == job['id'])
    assert (fake_job.name == job['name'])
    assert (fake_job.status == job['status'])


def test_get_job_by_id_api_not_found(client):
    response = client.get('/jobs/22', headers=HEADERS)
    assert (response.status_code == 404)


def test_delete_job_success(client, app):
    headers = {'X-Auth-Token': str(client.secret)}
    job_id = 1000
    with app.app_context():
        job = Job(id=job_id, workflow_id=1000, cluster_id=1, workflow_name='DEL',
            user_id=1, user_name='AA', user_login='aa')
        db.session.add(job)
        db.session.commit()

    response = client.delete(f'/jobs/{job_id}', headers=headers)
    assert (response.status_code == 204)
    with app.app_context():
        assert Job.query.get(job_id) is None
