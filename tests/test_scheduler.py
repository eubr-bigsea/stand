from collections import KeysView
from datetime import date, datetime, timedelta

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

    reference = date.today() - \
        timedelta(days=config["stand"]["pipeline"]["days"])
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
        mocked_create_pipeline_run, mocked_get_runs, mocked_get):
    """
    Test that the scheduler creates a run for a pipeline when it is enabled and 
    run doesn't exist.
    """
    pipeline = {
        "id": 1,
        "name": "Pipeline 1",
        "enabled": True,
        "steps": [],
    }
    mocked_get.side_effect = fake_req(200, [pipeline])
    mocked_get_runs.side_effect = [[]]  # Must be a list of results

    engine = create_sql_alchemy_async_engine(config)
    await update_pipelines_runs(config, engine)
    mocked_get_runs.assert_called_with(ANY, KeysView([pipeline["id"]]))

    mocked_create_pipeline_run.assert_called_with(ANY, pipeline, user={})


@pytest.mark.asyncio
@patch("requests.get")
@patch("stand.scheduler.scheduler.get_runs")
@patch("stand.scheduler.scheduler.create_pipeline_run")
async def test_update_pipeline_runs_ignore_because_interrupted_run(
        mocked_create_pipeline_run, mocked_get_runs, mocked_get):
    """
    Test that the scheduler refrains from creating a new run when 
    an INTERRUPTED run is already associated with the pipeline and 
    falls within its valid period.
    """
    pipeline = {
        "id": 1001,
        "name": "Pipeline with interrupted run",
        "enabled": True,
    }
    mocked_get.side_effect = fake_req(200, [pipeline])
    mocked_get_runs.side_effect = [
        [PipelineRun(pipeline_id=pipeline["id"],
                     status=StatusExecution.PENDING)]]

    engine = create_sql_alchemy_async_engine(config)
    await update_pipelines_runs(config, engine)
    mocked_get_runs.assert_called_with(ANY, KeysView([pipeline["id"]]))

    mocked_create_pipeline_run.assert_not_called()


@pytest.mark.asyncio
@patch("requests.get")
@patch("stand.scheduler.scheduler.get_runs")
@patch("stand.scheduler.scheduler.create_pipeline_run")
async def test_update_pipeline_runs_create_because_interrupted_run_expired(
        mocked_create_pipeline_run, mocked_get_runs, mocked_get):
    """
    Test that the scheduler creates a new run when an INTERRUPTED run is
    already associated with the pipeline and has expired.
    """
    pipeline = {
        "id": 1002,
        "name": "Pipeline with expired interrupted run",
        "enabled": True,
    }
    mocked_get.side_effect = fake_req(200, [pipeline])
    mocked_get_runs.side_effect = [
        [PipelineRun(pipeline_id=pipeline["id"],
                     status=StatusExecution.INTERRUPTED,
                     finish=datetime.now() - timedelta(days=8))]
    ]

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
        mocked_cancel_run: AsyncMock,    mocked_create_pipeline_run: AsyncMock,
        mocked_get_runs: AsyncMock, mocked_get):
    """
    Test that the scheduler disables an existing run for a pipeline when it 
    is disabled.
    """
    pipelines = [
        {"id": 2, "name": "Disabled pipeline", "enabled": False},
    ]

    mocked_get.side_effect = fake_req(200, pipelines)
    runs = [
        PipelineRun(pipeline_id=pipelines[0]["id"],
                    status=StatusExecution.PENDING),
    ]
    mocked_get_runs.side_effect = [runs]

    engine = create_sql_alchemy_async_engine(config)
    await update_pipelines_runs(config, engine)
    mocked_get_runs.assert_called_with(
        ANY, KeysView([pipelines[0]["id"]])
    )

    mocked_create_pipeline_run.assert_not_called()
    mocked_cancel_run.assert_called_once_with(ANY, runs[0])


@pytest.mark.asyncio
@patch("requests.get")
@patch("stand.scheduler.scheduler.get_runs")
@patch("stand.scheduler.scheduler.create_pipeline_run")
@patch("stand.scheduler.scheduler.cancel_run")
async def test_update_pipeline_runs_ignore_run_previously_canceled(
        mocked_cancel_run: AsyncMock, mocked_create_pipeline_run: AsyncMock,
        mocked_get_runs: AsyncMock, mocked_get):
    """
    Test that the scheduler ignores when a pipeline is disabled and there is 
    no run.
    """
    pipelines = [
        {"id": 2, "name": "Disabled pipeline", "enabled": False},
    ]

    mocked_get.side_effect = fake_req(200, pipelines)
    runs = [
        PipelineRun(pipeline_id=pipelines[0]["id"],
                    status=StatusExecution.CANCELED),
    ]
    mocked_get_runs.side_effect = [runs]

    engine = create_sql_alchemy_async_engine(config)
    await update_pipelines_runs(config, engine)
    mocked_get_runs.assert_called_with(
        ANY, KeysView([pipelines[0]["id"]])
    )

    mocked_create_pipeline_run.assert_not_called()
    mocked_cancel_run.assert_not_called()


@pytest.mark.asyncio
@patch("requests.get")
@patch("stand.scheduler.scheduler.get_runs")
@patch("stand.scheduler.scheduler.create_pipeline_run")
@patch("stand.scheduler.scheduler.change_run_state")
async def test_update_pipeline_runs_after_valid_period(
        mocked_change_run_state: AsyncMock,
        mocked_create_pipeline_run: AsyncMock,
        mocked_get_runs: AsyncMock, mocked_get):
    """
    Test if the run state is changed to PENDING if it is not FINISHED and it 
    is expired.
    """
    pipelines = [
        {"id": 6, "name": "Pipeline with expired run", "enabled": True},
    ]

    mocked_get.side_effect = fake_req(200, pipelines)
    runs = [
        PipelineRun(pipeline_id=pipelines[0]["id"],
                    status=StatusExecution.RUNNING,
                    finish=datetime.now() - timedelta(10)),
    ]
    mocked_get_runs.side_effect = [runs]

    engine = create_sql_alchemy_async_engine(config)
    await update_pipelines_runs(config, engine)
    mocked_get_runs.assert_called_with(
        ANY, KeysView([pipelines[0]["id"]])
    )

    mocked_create_pipeline_run.assert_called_once_with(
        ANY, pipelines[0], user={})
    mocked_change_run_state.assert_called_once_with(
        ANY, runs[0], StatusExecution.PENDING)
