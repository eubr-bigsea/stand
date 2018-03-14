from functools import partial

from flask import url_for
from stand.models import StatusExecution

job_detail_url = partial(url_for, endpoint='jobdetailapi')

HEADERS = {'X-Auth-Token': '123456'}


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
