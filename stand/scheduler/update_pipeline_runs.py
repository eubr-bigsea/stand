from stand.models import (
    Job,
    PipelineRun,
    PipelineStepRun,
    StatusExecution,
    Pipeline,
    PipelineStep,
)

# Basic code to start a scheduler using aiocron package
import asyncio
import os
import typing
from datetime import date, datetime, timedelta
from typing import List


from stand.scheduler.utils import *
from stand.scheduler.commands import *


def update_pipelines_runs(
    updated_pipelines: typing.Dict,
    pipeline_runs: typing.List,
    current_time: datetime,
):
    """Update pipelines' runs"""
    runs = dict([[r.pipeline_id, r] for r in pipeline_runs])

    commands = []
    for pipeline in updated_pipelines.values():

        run: PipelineRun = runs.get(pipeline["id"])

        if run:
            if (
                run.status == StatusExecution.RUNNING
                or run.status == StatusExecution.WAITING
            ):
                # run expired
                if run.finish < current_time:
                    # all steps completed
                    if run.last_executed_step == len(run.steps):
                        commands.append(
                            UpdatePipelineRunStatus(
                                pipeline_run=run, status=StatusExecution.COMPLETED
                            )
                        )
                        commands.append(CreatePipelineRun(pipeline=pipeline))
                    # not all steps completed
                    else:
                        commands.append(
                            UpdatePipelineRunStatus(
                                pipeline_run=run, status=StatusExecution.PENDING
                            )
                        )
                        commands.append(CreatePipelineRun(pipeline=pipeline))

                # Test if run is using latest pipeline data
                elif run.updated < pipeline["updated"]:
                    commands.append(
                        UpdatePipelineInfo(
                            pipeline_run=run, update_time=pipeline["update"]
                        )
                    )

            if run.status == StatusExecution.COMPLETED:
                if run.finish < current_time:
                    commands.append(CreatePipelineRun(pipeline=pipeline))

            elif run.status == StatusExecution.INTERRUPTED:
                if run.finish < current_time:
                    commands.append(CreatePipelineRun(pipeline=pipeline))

        else:

            commands.append(CreatePipelineRun(pipeline=pipeline))

    return commands
