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
from sqlalchemy.ext.asyncio import (
    AsyncEngine, AsyncSession, create_async_engine)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import and_

from stand.models import (Job, PipelineRun, StatusExecution,Pipeline,PipelineStep)

from configuration import load
from stand.scheduler.utils import *
from stand.scheduler.status_control import *
from stand.scheduler.trigger_scheduled_jobs import *
from stand.scheduler.update_pipeline_runs import *

async def check_and_execute():
    engine = create_sql_alchemy_async_engine(config)
    session = build_session_maker(engine=engine)
    while True:
        current_time = datetime.now()
        remaining_seconds = (
            60 - current_time.second - (current_time.microsecond / 1_000_000)
        )
        await asyncio.sleep(remaining_seconds)  # Sleep until next minute
        
        updated_pipelines = get_pipelines(tahiti_config=config,days=7)
        active_pipeline_runs = get_runs(session=session, pipeline_ids= updated_pipelines.keys())
        
        
        await update_pipelines_runs(
        updated_pipelines=updated_pipelines,
        pipeline_runs=active_pipeline_runs,
        engine= engine,
        current_time=current_time)
        # TODO: pass session as arguments 
        for run in active_pipeline_runs :
            trigger_scheduled_pipeline_steps(pipeline_run=run,time=current_time)
            propagate_job_status(run=run)

async def main(engine):
    # await check_and_execute()
    async_session = build_session_maker(engine)

    async with async_session() as session:
        result = await session.execute(select(Job).filter(Job.id == 1).limit(1))
    # async with engine.begin() as conn:
    #    result = await conn.execute(select(Job).filter(Job.id == 1))
    # job: Job = result.one()
    job: Job = result.scalars().one()
    print(">>>>>>>>", job, type(job))
    await session.close()
    await engine.dispose()


if __name__ == "__main__":
    config = load()
    print(config["stand"]["servers"]["database_url"])
    engine = create_sql_alchemy_async_engine(config)
    asyncio.run(main(engine))
