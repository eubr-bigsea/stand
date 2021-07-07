# -*- coding: utf-8 -*-
from stand.models import *
from flask import current_app


def test_cluster_fail_not_authorized(client):
    tests = [
        lambda: client.get('/clusters'),
        lambda: client.post('/clusters'),
        lambda: client.get('/clusters/1'),
        lambda: client.patch('/clusters/1'),
        lambda: client.delete('/clusters/1'),
    ]
    for i, test in enumerate(tests):
        rv = test()
        assert 401 == rv.status_code, \
            f'Test {i}: Incorrect status code: {rv.status_code}'
        resp = rv.json
        assert resp['status'] == 'ERROR', f'Test {i}: Incorrect status'
        assert 'Thorn' in resp['message'], f'Test {i}: Incorrect message'


def test_cluster_list_success(client):
    headers = {'X-Auth-Token': str(client.secret)}
    rv = client.get('/clusters', headers=headers)
    assert 200 == rv.status_code, 'Incorrect status code'
    resp = rv.json
    assert resp['pagination']['total'] == 4, \
        f"Wrong quantity: {resp['pagination']['total']}"

    with current_app.app_context():
        default_cluster = Cluster.query.order_by(Cluster.name).first()

    assert resp['data'][0]['id'] == default_cluster.id
    assert resp['data'][0]['name'] == default_cluster.name
    assert resp['data'][0]['enabled'] == default_cluster.enabled


def test_cluster_list_with_parameters_success(client):
    headers = {'X-Auth-Token': str(client.secret)}
    params = {'enabled': 'false', 'fields': 'id,name',
              'asc': 'false', 'query': 'cluster', 'sort': 'created'}

    rv = client.get('/clusters', headers=headers, query_string=params)
    assert 200 == rv.status_code, 'Incorrect status code'
    resp = rv.json
    assert resp['pagination']['total'] == 1, \
        f"Wrong quantity: {resp['pagination']['total']}"


def test_cluster_list_no_page_success(client):
    headers = {'X-Auth-Token': str(client.secret)}
    params = {'page': 'false'}

    rv = client.get('/clusters', headers=headers, query_string=params)
    assert 200 == rv.status_code, 'Incorrect status code'
    resp = rv.json
    assert len(resp['data']) == 4, 'Wrong quantity'\
        f"Wrong quantity: {resp['pagination']['total']}"


def test_cluster_post_missing_data(client):
    headers = {'X-Auth-Token': str(client.secret)}
    params = {}
    rv = client.post('/clusters', headers=headers, json=params)
    assert 400 == rv.status_code, 'Incorrect status code'
    resp = rv.json
    assert resp['status'] == 'ERROR', 'Wrong status'


def test_cluster_post_success(client):
    headers = {'X-Auth-Token': str(client.secret)}
    params = {'address': 'hdfs://server-hdfs/test',
              'description': 'A new cluster', 'enabled': True,
              'name': 'Test cluster', 'type': 'SPARK_LOCAL'}

    rv = client.post('/clusters', headers=headers, json=params)
    assert 201 == rv.status_code, f'Incorrect status code: {rv.status_code}'
    resp = rv.json
    # assert resp['status'] == 'OK', 'Wrong status'


def test_cluster_get_success(client):
    headers = {'X-Auth-Token': str(client.secret)}
    cluster_id = 1
    rv = client.get(f'/clusters/{cluster_id}', headers=headers)
    assert 200 == rv.status_code, 'Incorrect status code'

    with current_app.app_context():
        default_cluster = Cluster.query.get(1)
    resp = rv.json

    assert resp['data'][0]['id'] == default_cluster.id
    assert resp['data'][0]['type'] == default_cluster.type
    assert resp['data'][0]['address'] == default_cluster.address
    assert resp['data'][0]['name'] == default_cluster.name
    assert resp['data'][0]['enabled'] == default_cluster.enabled


def test_cluster_fail_not_found_error(client):
    headers = {'X-Auth-Token': str(client.secret)}
    cluster_id = 999
    rv = client.get(f'/clusters/{cluster_id}', headers=headers)
    assert 404 == rv.status_code, f'Incorrect status code: {rv.status_code}'


def test_cluster_delete_success(client):
    headers = {'X-Auth-Token': str(client.secret)}
    cluster_id = 9999

    rv = client.get(f'/clusters/{cluster_id}', headers=headers)
    assert rv.status_code == 404

    with current_app.app_context():
        cluster = Cluster(
            id=cluster_id, address='file:///tmp', type='SPARK_LOCAL', name='Deleted',
            description='Testing', enabled=True)
        db.session.add(cluster)
        db.session.commit()

    rv = client.delete(f'/clusters/{cluster_id}', headers=headers)
    assert 204 == rv.status_code, f'Incorrect status code: {rv.status_code}'

    with current_app.app_context():
        cluster = Cluster.query.get(cluster_id)
        assert cluster is None


def test_cluster_patch_success(client, app):
    headers = {'X-Auth-Token': str(client.secret)}
    cluster_id = 8888

    with app.app_context():
        cluster = Cluster(
            id=cluster_id, address='file:///tmp', type='SPARK_LOCAL', name='Updated',
            description='Testing', enabled=True)
        db.session.add(cluster)
        db.session.commit()

    update = {'address': 'hdfs://teste.com', 'name': 'Fixed'}
    rv = client.patch(f'/clusters/{cluster_id}', json=update, headers=headers)
    assert rv.status_code == 200

    with app.app_context():
        cluster = Cluster.query.get(cluster_id)
        assert cluster.name == update['name']
        assert cluster.address == update['address']
