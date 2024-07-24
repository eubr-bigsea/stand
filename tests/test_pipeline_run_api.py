
from stand.models import PipelineRun, db
from flask import current_app


def test_pipeline_run_fail_not_authorized(client):
    tests = [
        lambda: client.get('/pipeline-runs', follow_redirects=True),
        lambda: client.get('/pipeline-runs/1', follow_redirects=True),
    ]
    for i, test in enumerate(tests):
        rv = test()
        assert 401 == rv.status_code, \
            f'Test {i}: Incorrect status code: {rv.status_code}'
        resp = rv.json
        assert resp['status'] == 'ERROR', f'Test {i}: Incorrect status'
        assert 'Thorn' in resp['message'], f'Test {i}: Incorrect message'


def test_pipeline_run_list_success(client):
    headers = {'X-Auth-Token': str(client.secret)}
    rv = client.get('/pipeline-runs', headers=headers, follow_redirects=True)
    assert 200 == rv.status_code, 'Incorrect status code'
    resp = rv.json
    assert resp['pagination']['total'] == PipelineRun.query.count(), \
        f"Wrong quantity: {resp['pagination']['total']}"

    with current_app.app_context():
        pipeline_run = PipelineRun.query.order_by(
            PipelineRun.id).first()
        assert resp['data'][0]['id'] == pipeline_run.id
        assert resp['data'][0].get('start') == (
            pipeline_run.start.isoformat() if pipeline_run.start else None)
        assert resp['data'][0].get('finish') == (
            pipeline_run.finish.isoformat() if pipeline_run.finish else None)
        assert resp['data'][0].get('pipeline_id') == (pipeline_run.pipeline_id)
        assert resp['data'][0].get('last_executed_step') == (
            pipeline_run.last_executed_step)
        assert resp['data'][0].get('comment') == (pipeline_run.comment)
        assert resp['data'][0].get('status') == (pipeline_run.status)
        assert resp['data'][0].get('final_status') == (
            pipeline_run.final_status)


def test_pipeline_run_list_all_success(client):
    headers = {'X-Auth-Token': str(client.secret)}
    params = {'all': 'true'}
    rv = client.get('/pipeline-runs', headers=headers, query_string=params,
                    follow_redirects=True)
    assert 200 == rv.status_code, 'Incorrect status code'


def test_pipeline_run_list_simple_sucess(client):
    headers = {'X-Auth-Token': str(client.secret)}
    params = {'simple': 'true'}

    rv = client.get('/pipeline-runs', headers=headers, query_string=params,
                    follow_redirects=True)
    assert 200 == rv.status_code, 'Incorrect status code'


def test_pipeline_run_get_success(client):
    headers = {'X-Auth-Token': str(client.secret)}
    pipeline_run_id = 1
    rv = client.get(f'/pipeline-runs/{pipeline_run_id}',
                    headers=headers, follow_redirects=True)
    assert 200 == rv.status_code, 'Incorrect status code'

    with current_app.app_context():
        pipeline_run = db.session.get(PipelineRun, 1)
        resp = rv.json
        assert resp['data'][0]['id'] == pipeline_run.id
        assert resp['data'][0].get('start') == (
            pipeline_run.start.isoformat() if pipeline_run.start else None)
        assert resp['data'][0].get('finish') == (
            pipeline_run.finish.isoformat() if pipeline_run.finish else None)
        assert resp['data'][0].get('pipeline_id') == (pipeline_run.pipeline_id)
        assert resp['data'][0].get('last_executed_step') == (
            pipeline_run.last_executed_step)
        assert resp['data'][0].get('comment') == (pipeline_run.comment)
        assert resp['data'][0].get('status') == (pipeline_run.status)
        assert resp['data'][0].get('final_status') == (
            pipeline_run.final_status)


def test_pipeline_run_not_found_failure(client):
    headers = {'X-Auth-Token': str(client.secret)}
    pipeline_run_id = 999
    rv = client.get(f'/pipeline-runs/{pipeline_run_id}',
                    headers=headers, follow_redirects=True)
    assert 404 == rv.status_code, f'Incorrect status code: {rv.status_code}'


def test_pipeline_run_delete_success(client, app):
    headers = {'X-Auth-Token': str(client.secret)}
    pipeline_run_id = 2

    rv = client.delete(f'/pipeline-runs/{pipeline_run_id}', headers=headers)
    assert rv.status_code == 204

