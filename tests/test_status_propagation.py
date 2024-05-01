from collections import KeysView
from datetime import date, datetime, timedelta

import pytest
from mock import ANY, patch, MagicMock
from mock.mock import AsyncMock

from stand.models import PipelineRun, StatusExecution, PipelineStep
from stand.scheduler.utils import *
from stand.scheduler.status_control import *


@pytest.fixture
def mocked_functions():
    with patch(
        "stand.scheduler.status_control.get_latest_pipeline_step_run"
    ) as mocked_get_latest_pipeline_step_run, patch(
        "stand.scheduler.status_control.get_latest_job_from_pipeline_step_run"
    ) as mocked_get_latest_job_from_pipeline_step_run, patch(
        "stand.scheduler.status_control.update_pipeline_run_status"
    ) as mocked_update_pipeline_run_status, patch(
        "stand.scheduler.status_control.update_pipeline_step_run_status"
    ) as mocked_update_pipeline_step_run_status:

        yield (
            mocked_get_latest_pipeline_step_run,
            mocked_get_latest_job_from_pipeline_step_run,
            mocked_update_pipeline_run_status,
            mocked_update_pipeline_step_run_status,
        )


def test_completed_job_status_propagation(mocked_functions):
    """
    tests if a "completed" job  has its status
    correctly propagated to its associated step_run and pipeline_run
    """
    (
        mocked_get_latest_pipeline_step_run,
        mocked_get_latest_job_from_pipeline_step_run,
        mocked_update_pipeline_run_status,
        mocked_update_pipeline_step_run_status,
    ) = mocked_functions
    latest_job = Job(id=1, status=StatusExecution.COMPLETED)

    step_run = PipelineStepRun(id="1", status=StatusExecution.RUNNING)

    pipeline_run = PipelineRun(id="1", status=StatusExecution.RUNNING)

    mocked_get_latest_pipeline_step_run.return_value = step_run
    mocked_get_latest_job_from_pipeline_step_run.return_value = latest_job

    propagate_job_status(pipeline_run)

    mocked_update_pipeline_run_status.assert_called_with(
        pipeline_run, StatusExecution.WAITING
    )
    mocked_update_pipeline_step_run_status(pipeline_run, StatusExecution.COMPLETED)


def test_completed_job_status_propagation(mocked_functions):
    """
    tests if a job  thast started to run has its status
    correctly propagated to its associated "waiting"step_run and pipeline_run
    """
    (
        mocked_get_latest_pipeline_step_run,
        mocked_get_latest_job_from_pipeline_step_run,
        mocked_update_pipeline_run_status,
        mocked_update_pipeline_step_run_status,
    ) = mocked_functions

    latest_job = Job(id=1, status=StatusExecution.RUNNING)

    step_run = PipelineStepRun(id="1", status=StatusExecution.RUNNING)

    pipeline_run = PipelineRun(id="1", status=StatusExecution.WAITING)

    mocked_get_latest_pipeline_step_run.return_value = step_run
    mocked_get_latest_job_from_pipeline_step_run.return_value = latest_job

    propagate_job_status(pipeline_run)

    mocked_update_pipeline_run_status.assert_called_with(
        pipeline_run, StatusExecution.RUNNING
    )


def test_error_job_status_propagation(mocked_functions):
    """
    tests if a error status job has its status
    correctly propagated to its associated "waiting"step_run and pipeline_run
    """

    (
        mocked_get_latest_pipeline_step_run,
        mocked_get_latest_job_from_pipeline_step_run,
        mocked_update_pipeline_run_status,
        mocked_update_pipeline_step_run_status,
    ) = mocked_functions

    latest_job = Job(id=1, status=StatusExecution.ERROR)

    step_run = PipelineStepRun(id="1", status=StatusExecution.RUNNING)

    pipeline_run = PipelineRun(id="1", status=StatusExecution.WAITING)

    mocked_get_latest_pipeline_step_run.return_value = step_run
    mocked_get_latest_job_from_pipeline_step_run.return_value = latest_job

    propagate_job_status(pipeline_run)

    mocked_update_pipeline_run_status.assert_called_with(
        pipeline_run, StatusExecution.ERROR
    )
    mocked_update_pipeline_step_run_status(pipeline_run, StatusExecution.ERROR)
