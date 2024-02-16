from stand.scheduler.scheduler import get_pipelines
from mock import patch
from datetime import date, timedelta
import pytest

config = {
        'stand': {
            'auditing': False,
            'services':{
                'tahiti': {
                    'url': 'http://localhost:3333',
                    'auth_token': 1111111
                }
            },
            'pipeline': {
                'days': 7
            }
        }
    }


class FakeResponse():
    def __init__(self, status, text, args, kwargs):
        self.status_code = status
        self.text = text
        self.args = args
        self.kwargs = kwargs

    def json(self):
         return {'list': self.text}
    
def fake_req(status, text):
    def f(*args, **kwargs):
        return FakeResponse(status, text, args, kwargs)
    return f

services_config = config['stand']['services']

@patch('requests.get')
def test_get_pipelines_fail_http_error(mocked_get, pipelines):
    mocked_get.side_effect = fake_req(500, pipelines)
    with pytest.raises(Exception) as err:
        pipelines = get_pipelines(services_config['tahiti'], 
                              config['stand']['pipeline']['days'])
    assert str(err.value) == "Error 500 while getting pipelines"

@patch('requests.get')
def test_get_pipelines(mocked_get, pipelines):
    # mock requests.get request to Tahiti API
    mocked_get.side_effect = fake_req(200, pipelines)

    pipelines = get_pipelines(services_config['tahiti'], 
                              config['stand']['pipeline']['days'])
    assert len(pipelines) > 0
    pipeline_1 = pipelines[1]

    assert pipeline_1['name'] == "Pipeline 1"
    assert pipeline_1['enabled']

    reference = (date.today() - 
                 timedelta(days=config['stand']['pipeline']['days']))
    mocked_get.assert_called_with(
         f"{services_config['tahiti']['url']}/pipelines", 
         {'after': reference}, 
         headers={'X-Auth-Token': services_config['tahiti']['auth_token']})
    