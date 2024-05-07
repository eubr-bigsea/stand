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
import requests
import yaml
from croniter import croniter
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import and_

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
            if run.status == StatusExecution.RUNNING:

                if run.finish < current_time:
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

            elif run.status == StatusExecution.INTERRUPTED:
                if run.finish < current_time:
                    commands.append(CreatePipelineRun(pipeline=pipeline))

        else:
           
            commands.append(CreatePipelineRun(pipeline=pipeline))

    return commands
