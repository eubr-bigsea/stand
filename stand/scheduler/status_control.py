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
from stand.scheduler.commands import *

def propagate_job_status(
    run: PipelineRun, active_step_run: PipelineStepRun, latest_job: Job
):
    """
    Checks status of latest job and propagate it to
    its associated step run and pipeline run
    """

    if active_step_run is not None:

        latest_job_status = latest_job.status

        # run was running and a step was completed
        if (
            run.status == StatusExecution.RUNNING
            and latest_job_status == StatusExecution.COMPLETED
        ):
            commands = []
            commands.append(
                UpdatePipelineRunStatus(
                    pipeline_run=run, status=StatusExecution.WAITING
                )
            )
            commands.append(
                UpdatePipelineStepRunStatus(
                    pipeline_step_run=active_step_run, status=StatusExecution.COMPLETED
                )
            )
            # job completed so last conmpleted step must be increased
            commands.append(
                ChangeLastCompletedStep(
                    pipeline_run=run,
                    new_last_completed_step=PipelineRun.last_executed_step + 1,
                )
            )
            return commands

        # run was waiting and a step was triggered
        elif (
            run.status == StatusExecution.WAITING
            and latest_job_status == StatusExecution.RUNNING
        ):
            command = UpdatePipelineRunStatus(
                pipeline_run=run, status=StatusExecution.RUNNING
            )
            return command

        # error in job during run
        elif (
            run.status == StatusExecution.RUNNING
            and latest_job_status == StatusExecution.ERROR
        ):
            commands = []
            commands.append(
                UpdatePipelineRunStatus(pipeline_run=run, status=StatusExecution.ERROR)
            )
            commands.append(
                UpdatePipelineStepRunStatus(
                    pipeline_step_run=active_step_run, status=StatusExecution
                )
            )
            return commands

        # propagates other status without special interactions
        else:
            commands = []
            commands.append(
                UpdatePipelineRunStatus(pipeline_run=run, status=latest_job_status)
            )
            commands.append(
                UpdatePipelineStepRunStatus(
                    pipeline_step_run=active_step_run, status=latest_job_status
                )
            )
            return commands
