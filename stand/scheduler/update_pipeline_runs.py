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


async def update_pipelines_runs(
    updated_pipelines: typing.Dict,
    pipeline_runs: typing.List,
    engine: AsyncEngine,
    current_time: datetime,
):
    """Update pipelines' runs"""

    async_session = build_session_maker(engine)

    async with async_session() as session:
        
        runs = dict([[r.pipeline_id, r] for r in pipeline_runs])
        
        for pipeline in updated_pipelines.values():
        
            run: PipelineRun = runs.get(pipeline["id"])
            if run:
                if run.status == StatusExecution.RUNNING:
                  
                    if run.finish < current_time:
                        
                        await update_pipeline_run_status(
                            session, run, StatusExecution.PENDING
                        )
                        await create_pipeline_run(session, pipeline, user={})
                        
                    # Test if run is using latest pipeline data
                    elif run.updated < pipeline["updated"]:
                        await update_run(session, pipeline["updated"], run)

                elif run.status == StatusExecution.INTERRUPTED:
                    if run.finish < current_time:
                        await create_pipeline_run(session, pipeline, user={})
            else:
                await create_pipeline_run(session, pipeline, user={})
