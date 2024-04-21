from collections import KeysView
from datetime import date, datetime, timedelta

import pytest
from mock import ANY, patch
from mock.mock import AsyncMock

from stand.models import PipelineRun, StatusExecution ,PipelineStep
from stand.scheduler.scheduler import (
    create_sql_alchemy_async_engine,
    get_pipelines,
    update_pipelines_runs,
    get_next_scheduled_date,
    pipeline_run_should_be_created,
)

config = {
    "stand": {
        "auditing": False,
        "services": {"tahiti": {"url": "http://localhost:3333", "auth_token": 1111111}},
        "pipeline": {"days": 7},
        "servers": {"database_url": "sqlite:///test.db"},
        "catchup": {"days":1}
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


















def test_get_next_scheduled_date_of_step():
    
    """
    Test if the function correctly asserts the date
    of the next occurence
    """
    # because current_time is the 23rd day
    # and the expr only schedules for the 25th day
    # then, next occurence should be on the 25th of the same month
    expr ="0 0 25 * *"
    current_time = datetime(2024, 3, 23, 0, 0)
    
    next_occ = get_next_scheduled_date(cron_expr=expr,
    
                                               current_time=current_time)
    expected_time = datetime(2024,3, 25,0,0 )
    assert next_occ==expected_time
    
    # because current_time is the 27th day
    # and the expr only schedules for the 25th day
    # then next occurence should be on the 25th of the next month
    
    expr ="0 0 25 * *"
    current_time = datetime(2024, 3, 27, 0, 0)
    
    next_occ = get_next_scheduled_date(cron_expr=expr,
    
                                               current_time=current_time)
    expected_time = datetime(2024,4, 25,0,0 )
    assert next_occ==expected_time
    
    #testing similar cases for weekdays expressions instead of day of months
    expr ="* * * * 1" #every monday
    current_time = datetime(2024, 3, 24, 0, 0) #this is a sunday
    
    next_occ = get_next_scheduled_date(cron_expr=expr,
    
                                               current_time=current_time)
    expected_time = datetime(2024,3, 25,0,0 ) #this is a monday 
    assert next_occ==expected_time
    
    expr ="* * * * 1" #every monday
    current_time = datetime(2024, 3, 26, 0, 0) #this is a tuesday
    
    next_occ = get_next_scheduled_date(cron_expr=expr,
    
                                               current_time=current_time)
    expected_time = datetime(2024,4, 1,0,0 ) #this is next week monday
    
    assert next_occ==expected_time
    
    
    
def test_pipeline_run_should_be_created():
    """
    Test if the function can tell if current_time 
    is between first and last  step scheduled dates
    """
    #case 1 : current time creation of pipeline run
    #would make fist step trigger on next window
    first_step = PipelineStep(id="123", name="Step 1", 
                        order=1, 
                        scheduling="0 17 10 * *", ## trigger 17:00, day 10 , every month 
                        description="Description of Step 1", 
                        enabled=True)
    
    last_step = PipelineStep(id="124", name="Step 5", 
                        order=5, 
                        scheduling="0 17 15 * *", ## trigger 17:00, day 15 , every month 
                        description="Description of Step 5", 
                        enabled=True)
    
    date_time = datetime(2024, 3, 13, 15, 0) ## day 13
    # if a run was created durying the 13th day , first step wouldnt be triggered
    can_be_created =pipeline_run_should_be_created(first_step=first_step,
                                              last_step=last_step,
                                              current_time=date_time)

    assert can_be_created == False
    
     #case 2 : current time creation of pipeline run
     #would make fist step trigger on current window
    first_step = PipelineStep(id="123", name="Step 1", 
                        order=1, 
                        scheduling="0 17 10 * *", ## trigger 17:00, day 10 , every month 
                        description="Description of Step 1", 
                        enabled=True)
    
    last_step = PipelineStep(id="124", name="Step 5", 
                        order=5, 
                        scheduling="0 17 15 * *", ## trigger 17:00, day 15 , every month 
                        description="Description of Step 5", 
                        enabled=True)
    
    date_time = datetime(2024, 3, 8, 15, 0) ## day 8
    # a run with first step on day 10, can be created on day 8
    can_be_created =pipeline_run_should_be_created(first_step=first_step,
                                              last_step=last_step,
                                              current_time=date_time)

    assert can_be_created == True
    

    



    
    
    
  
    