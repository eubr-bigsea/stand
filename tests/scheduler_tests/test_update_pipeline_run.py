from collections import KeysView
from datetime import date, datetime, timedelta

import pytest
from mock import ANY, patch
from mock.mock import AsyncMock

from stand.models import PipelineRun, StatusExecution, PipelineStep
from stand.scheduler.utils import *
from stand.scheduler.update_pipeline_runs import *
from stand.scheduler.commands import *

config = {
    "stand": {
        "auditing": False,
        "services": {"tahiti": {"url": "http://localhost:3333", "auth_token": 1111111}},
        "pipeline": {"days": 7},
        "servers": {"database_url": "sqlite:///test.db"},
        "catchup": {"days": 1},
    }
}


class FakeResponse:
    def __init__(self, status, text, args, kwargs):
        self.status_code = status
        self.text = text
        self.args = args
        self.kwargs = kwargs

    def json(self):
        return {"list": self.text}


def fake_req(status, text):
    def f(*args, **kwargs):
        return FakeResponse(status, text, args, kwargs)

    return f


services_config = config["stand"]["services"]


@patch("requests.get")
def test_get_pipelines_fail_http_error(mocked_get, pipelines):
    mocked_get.side_effect = fake_req(500, pipelines)
    with pytest.raises(Exception) as err:
        pipelines = get_pipelines(
            services_config["tahiti"], config["stand"]["pipeline"]["days"]
        )
    assert str(err.value) == "Error 500 while getting pipelines"


@patch("requests.get")
def test_get_pipelines(mocked_get, pipelines):
    # mock requests.get request to Tahiti API
    mocked_get.side_effect = fake_req(200, pipelines)

    pipelines = get_pipelines(
        services_config["tahiti"], config["stand"]["pipeline"]["days"]
    )
    assert len(pipelines) > 0
    pipeline_1 = pipelines[1]

    assert pipeline_1["name"] == "Pipeline 1"
    assert pipeline_1["enabled"]

    reference = date.today() - timedelta(days=config["stand"]["pipeline"]["days"])
    mocked_get.assert_called_with(
        f"{services_config['tahiti']['url']}/pipelines",
        {"after": reference},
        headers={"X-Auth-Token": services_config["tahiti"]["auth_token"]},
    )


def test_update_pipeline_runs_new_run():
    """
    Test that the scheduler creates a run for a pipeline when it is enabled and
    run doesn't exist.
    """
    updated_pipelines = {
        0: {
            "id": 1,
            "name": "Pipeline 1",
            "enabled": True,
            "steps": [],
        }
    }
    # empty runs
    runs = []

    current_time = datetime.now()
 
    returned_commands = update_pipelines_runs(
        updated_pipelines=updated_pipelines,
        pipeline_runs=runs,
        current_time=current_time,
    )
   
    expected_commands = [CreatePipelineRun(pipeline=updated_pipelines[0])]
    for command in expected_commands:
        assert command in returned_commands
    


def test_update_pipeline_runs_create_because_interrupted_run_expired():
    """
    Test that the scheduler creates a new run when an INTERRUPTED run is
    already associated with the pipeline and has expired.
    """
    pipeline = {
        "id": 1002,
        "name": "Pipeline with expired interrupted run",
        "enabled": True,
    }

    updated_pipelines = {1002: pipeline}
    runs = [
        PipelineRun(
            pipeline_id=pipeline["id"],
            status=StatusExecution.INTERRUPTED,
            finish=datetime.now() - timedelta(days=8),
        ),
    ]

    returned_commands = update_pipelines_runs(
        updated_pipelines=updated_pipelines,
        pipeline_runs=runs,
        current_time=datetime.now(),
    )
    expected_commands = [CreatePipelineRun(pipeline=updated_pipelines[1002])]
    for command in expected_commands:
        assert command in returned_commands



def test_update_pipeline_runs_after_valid_period():
    """
    Test if the run state is changed to PENDING if it is not FINISHED and it
    is expired.
    """
    pipeline = {"id": 6, "name": "Pipeline with expired run", "enabled": True}

    updated_pipelines = {6: pipeline}

    runs = [
        PipelineRun(
            pipeline_id=pipeline["id"],
            status=StatusExecution.RUNNING,
            finish=datetime.now() - timedelta(1000),
        )
    ]

    retuned_commands = update_pipelines_runs(
        updated_pipelines=updated_pipelines,
        pipeline_runs=runs,
        current_time=datetime.now(),
    )
    expected_commands = []
    expected_commands.append(CreatePipelineRun(pipeline=updated_pipelines[6]))
    expected_commands.append(
        UpdatePipelineRunStatus(pipeline_run=runs[0], status=StatusExecution.PENDING)
    )
    for command in expected_commands:
        assert command in retuned_commands