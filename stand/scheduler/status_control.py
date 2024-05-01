from typing import List
from croniter import croniter
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import and_

from stand.models import (
    Job,
    PipelineRun,
    PipelineStepRun,
    StatusExecution,
    Pipeline,
    PipelineStep,
)
from stand.scheduler.utils import *


def propagate_job_status(run: PipelineRun):
    """
    Checks status of latest job and propagate it to
    its associated step run and pipeline run
    """

    active_step_run = get_latest_pipeline_step_run(run)

    if active_step_run is not None:

        latest_job = get_latest_job_from_pipeline_step_run(active_step_run)

        latest_job_status = latest_job.status

        # run was running and a step was completed
        if (
            run.status == StatusExecution.RUNNING
            and latest_job_status == StatusExecution.COMPLETED
        ):
            update_pipeline_run_status(run, StatusExecution.WAITING)
            update_pipeline_step_run_status(active_step_run, StatusExecution.COMPLETED)

        # run was waiting and a step was triggered
        elif (
            run.status == StatusExecution.WAITING
            and latest_job_status == StatusExecution.RUNNING
        ):
            update_pipeline_run_status(run, StatusExecution.RUNNING)

        # error in job during run
        elif (
            run.status == StatusExecution.RUNNING
            and latest_job_status == StatusExecution.ERROR
        ):
            update_pipeline_run_status(run, StatusExecution.ERROR)
            update_pipeline_step_run_status(active_step_run, StatusExecution.ERROR)

        # propagates other status without special interactions
        else:
            update_pipeline_run_status(run, latest_job_status)
            update_pipeline_step_run_status(active_step_run, latest_job_status)
