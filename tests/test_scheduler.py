from collections import KeysView
from datetime import date, timedelta

import pytest
from mock import ANY, patch
from mock.mock import AsyncMock
from stand.models import PipelineRun, StatusExecution
from stand.scheduler.scheduler import (
    create_sql_alchemy_async_engine,
    get_pipelines,
    update_pipelines_runs,
)

config = {
    "stand": {
        "auditing": False,
        "services": {"tahiti": {"url": "http://localhost:3333", "auth_token": 1111111}},
        "pipeline": {"days": 7},
        "servers": {"database_url": "sqlite:///test.db"},
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


@pytest.mark.asyncio
@patch("requests.get")
@patch("stand.scheduler.scheduler.get_runs")
@patch("stand.scheduler.scheduler.create_pipeline_run")
async def test_update_pipeline_runs_new_run(
    mocked_create_pipeline_run, mocked_get_runs, mocked_get
):
    """
    Test that the scheduler creates a run for a pipeline when it is enabled.
    """
    pipeline = {
        "id": 1,
        "name": "Pipeline 1",
        "enabled": True,
        "steps": [],
    }
    mocked_get.side_effect = fake_req(200, [pipeline])
    # mocked_get_runs.side_effect = [[PipelineRun()]]
    mocked_get_runs.side_effect = [[]]  # Must be a list of results

    engine = create_sql_alchemy_async_engine(config)
    await update_pipelines_runs(config, engine)
    mocked_get_runs.assert_called_with(ANY, KeysView([pipeline["id"]]))

    mocked_create_pipeline_run.assert_called_with(ANY, pipeline, user={})


@pytest.mark.asyncio
@patch("requests.get")
@patch("stand.scheduler.scheduler.get_runs")
@patch("stand.scheduler.scheduler.create_pipeline_run")
@patch("stand.scheduler.scheduler.cancel_run")
async def test_update_pipeline_runs_disable_run(
    mocked_cancel_run: AsyncMock,
    mocked_create_pipeline_run: AsyncMock,
    mocked_get_runs: AsyncMock,
    mocked_get,
):
    """
    Test that the scheduler disables a run for a pipeline when it is disabled.
    """
    pipelines = [
        {"id": 2, "name": "Disabled pipeline", "enabled": False},
        {"id": 3, "name": "Disabled pipeline", "enabled": False},
    ]

    mocked_get.side_effect = fake_req(200, pipelines)
    runs = [
        PipelineRun(pipeline_id=pipelines[0]["id"], status=StatusExecution.PENDING),
        PipelineRun(pipeline_id=pipelines[1]["id"], status=StatusExecution.CANCELED),
    ]
    mocked_get_runs.side_effect = [runs]

    engine = create_sql_alchemy_async_engine(config)
    await update_pipelines_runs(config, engine)
    mocked_get_runs.assert_called_with(
        ANY, KeysView([pipelines[0]["id"], pipelines[1]["id"]])
    )

    mocked_create_pipeline_run.assert_not_called()
    mocked_cancel_run.assert_called_once_with(ANY, runs[0])
