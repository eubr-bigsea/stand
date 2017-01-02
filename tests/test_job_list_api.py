import json

from flask import url_for
from stand.models import StatusExecution
from functools import partial

from stand.schema import JobListResponseSchema

job_detail_url = partial(url_for, endpoint='jobdetailapi')
job_list_url = partial(url_for, endpoint='joblistapi')

HEADERS = {'X-Auth-Token': '123456'}


# def test_create_job(session):
#     now = datetime.datetime.now()
#     post = Job(
#         created=now, started=now, finished=None,
#         status=StatusExecution.COMPLETED,
#         workflow_id=1, workflow_name='Teste', workflow_definition=None,
#         user_id=1, user_login='Walter', user_name='Walter dos Santos Filho',
#         cluster_id=1)
#
#     session.add(post)
#     session.commit()
#
#     assert post.id > 0

# noinspection PyUnusedLocal
def test_init(session):
    """ Used to initialize session (needed to link session in Factory boy
     with pytest.
    """
    pass


def test_list_jobs_api_unauthorized(client):
    jobs = client.get(job_list_url())
    assert (jobs.status_code == 401)


def test_get_job_by_id_api_works(client, model_factories):
    fake_job = model_factories.job_factory.create(
        status=StatusExecution.CANCELED, id=22)

    response = client.get(job_detail_url(job_id=fake_job.id), headers=HEADERS)
    job = response.json

    assert (fake_job.id == job['id'])
    assert (fake_job.status == job['status'])


def test_get_job_by_id_api_not_found(client, job_factory):
    response = client.get(job_detail_url(job_id=22), headers=HEADERS)
    assert (response.status_code == 404)


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
        'workflow': workflow_id
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
        'user': user_id
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
