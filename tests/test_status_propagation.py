from collections import KeysView
from datetime import date, datetime, timedelta

import pytest


from stand.models import PipelineRun, StatusExecution, PipelineStep
from stand.scheduler.utils import *
from stand.scheduler.status_control import *


def test_completed_job_status_propagation():
    """
    tests if a "completed" job  has its status
    correctly propagated to its associated step_run and pipeline_run
    """

    latest_job = Job(id=1, status=StatusExecution.COMPLETED)

    step_run = PipelineStepRun(id="1", status=StatusExecution.RUNNING)

    pipeline_run = PipelineRun(id="1", status=StatusExecution.RUNNING)

    result_commands = propagate_job_status(
        run=pipeline_run, active_step_run=step_run, latest_job=latest_job
    )
    expected_commands = []
    expected_commands.append(
        UpdatePipelineRunStatus(pipeline_run=pipeline_run, status=StatusExecution.WAITING)
    )
    expected_commands.append(
        UpdatePipelineStepRunStatus(
            pipeline_step_run=step_run, status=StatusExecution.COMPLETED
        )
    )
    for command in expected_commands:
        assert command in result_commands
    


def test_error_job_status_propagation():
    """
    tests if a error status job has its status
    correctly propagated to its associated "waiting"step_run and pipeline_run
    """

    latest_job = Job(id=1, status=StatusExecution.ERROR)

    step_run = PipelineStepRun(id="1", status=StatusExecution.RUNNING)

    pipeline_run = PipelineRun(id="1", status=StatusExecution.WAITING)

    result_commands = propagate_job_status(
        run=pipeline_run, active_step_run=step_run, latest_job=latest_job
    )

    expected_commands = []
    expected_commands.append(
        UpdatePipelineRunStatus(pipeline_run=pipeline_run, status=StatusExecution.ERROR)
    )
    expected_commands.append(
        UpdatePipelineStepRunStatus(
            pipeline_step_run=step_run, status=StatusExecution.ERROR
        )
    )

    for command in expected_commands:
        assert command in result_commands


def test_job_started_status_propagation():
    """
    tests if a job  thast started to run has its status
    correctly propagated to its associated "waiting"step_run and pipeline_run
    """

    latest_job = Job(id=1, status=StatusExecution.RUNNING)

    step_run = PipelineStepRun(id="1", status=StatusExecution.RUNNING)

    pipeline_run = PipelineRun(id="1", status=StatusExecution.WAITING)

    commands = propagate_job_status(
        run=pipeline_run, active_step_run=step_run, latest_job=latest_job
    )

    expected_command = UpdatePipelineRunStatus(
        pipeline_run=pipeline_run, status=StatusExecution.RUNNING
    )
    assert commands == expected_command
