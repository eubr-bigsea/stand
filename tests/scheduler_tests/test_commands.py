import typing
from datetime import date, datetime, timedelta
from typing import List
import pytest
import json
from stand.models import (
    Job,
    PipelineRun,
    PipelineStepRun,
    StatusExecution,
    Pipeline,
    PipelineStep,
    TriggerType,
)
from stand.scheduler.utils import *
from stand.scheduler.commands import *


@pytest.mark.parametrize(
    "pipeline,current_time,expected_time",
    [
        (
            {"execution_window": "monthly"},
            datetime(year=2024, month=12, day=31, hour=5, minute=0, second=0),
            datetime(year=2024, month=12, day=31, hour=23, minute=59, second=59),
        ),
        # bisexto
        (
            {"execution_window": "monthly"},
            datetime(year=2024, month=2, day=12, hour=15, minute=20, second=0),
            datetime(year=2024, month=2, day=29, hour=23, minute=59, second=59),
        ),
        (
            {"execution_window": "weekly"},
            datetime(year=2024, month=5, day=20, hour=15, minute=20, second=0),
            datetime(year=2024, month=5, day=25, hour=23, minute=59, second=59),
        ),
        (
            {"execution_window": "daily"},
            datetime(year=2024, month=5, day=20, hour=15, minute=20, second=0),
            datetime(year=2024, month=5, day=20, hour=23, minute=59, second=59),
        ),
    ],
)
def test_CreatePipelineRun_has_correct_end_time(pipeline, current_time, expected_time):
    """
    tests if the CreatePipelineRun command auxiliar functions
    for correctly generates the end time for the run
    """
    command = CreatePipelineRun(pipeline=pipeline)
    end_time = command.get_pipeline_run_end(current_time=current_time)
    assert end_time == expected_time


@pytest.mark.parametrize(
    "pipeline,current_time,expected_time",
    [
        (
            {"execution_window": "monthly"},
            datetime(year=2024, month=5, day=20, hour=5, minute=0, second=0),
            datetime(year=2024, month=5, day=1, hour=0, minute=0, second=0),
        ),
        (
            {"execution_window": "weekly"},  # starts sunday
            datetime(year=2024, month=5, day=20, hour=15, minute=20, second=0),
            datetime(year=2024, month=5, day=19, hour=0, minute=0, second=0),
        ),
        (
            {"execution_window": "daily"},
            datetime(year=2024, month=5, day=20, hour=15, minute=20, second=0),
            datetime(year=2024, month=5, day=20, hour=0, minute=0, second=0),
        ),
    ],
)
def test_CreatePipelineRun_has_correct_start_time(
    pipeline, current_time, expected_time
):
    """
    tests if the CreatePipelineRun command auxiliar functions
    for correctly generates the start time for the run
    """
    command = CreatePipelineRun(pipeline=pipeline)
    start_time = command.get_pipeline_run_start(current_time=current_time)
    assert start_time == expected_time


