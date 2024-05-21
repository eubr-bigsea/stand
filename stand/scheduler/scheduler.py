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

from stand.models import Job, PipelineRun, StatusExecution, Pipeline, PipelineStep

from stand.scheduler.utils import *
from stand.scheduler.status_control import *
from stand.scheduler.trigger_scheduled_jobs import *
from stand.scheduler.update_pipeline_runs import *



async def execute(session,current_time):
    
        #returned as pipeline_id:pipeline_in_json dict
        updated_pipelines = await get_pipelines(tahiti_config=config, days=7)
        #returned as list of active pipeline runs in json
        active_pipeline_runs = await get_runs(
            session=session, pipeline_ids=updated_pipelines.keys()
        )

        pipeline_runs_commands = update_pipelines_runs(
            updated_pipelines=updated_pipelines,
            pipeline_runs=active_pipeline_runs,
            current_time=current_time,
        )
        for command in pipeline_runs_commands:
            await command.execute(session)
            
        # must be called again bc pipeline_runs_commands can create new runs
        active_pipeline_runs = get_runs(
            session=session, pipeline_ids=updated_pipelines.keys()
        )
        for run in active_pipeline_runs:

            steps = [step for step in updated_pipelines[run["pipeline_id"]]["steps"]]

            trigger_commands = trigger_scheduled_pipeline_steps(
                pipeline_run=run, time=current_time, steps=steps
            )
            for command in trigger_commands:
                command.execute(session)
            #both the latest job and active step run needs to 
            #be called here because trigger_commands can alter these,
            #so they cant be called up top for the whole batch of runs
            latest_job = get_latest_job(run=run)
            active_step_run = get_active_step_run(run=run)
            
            #latest_job will always have a relationship to active_step_run
            propagate_commands = propagate_job_status(
                run=run, latest_job=latest_job, active_step_run=active_step_run
            )
            for command in propagate_commands:
                command.execute(session)
        
        #not used 
        return [pipeline_runs_commands,trigger_commands,propagate_commands]



async def check_and_execute():
    engine = create_sql_alchemy_async_engine(config)
    session = build_session_maker(engine=engine)
    while True:
        current_time = datetime.now()
        execute(session=session,current_time=current_time)
        
        #updating current_time to consider execute() execution time
        current_time = datetime.now()
        remaining_seconds = (
            60 - current_time.second - (current_time.microsecond / 1_000_000)
        )
        await asyncio.sleep(remaining_seconds)  # Sleep until next minute
   
       

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
    config = load_config()
    print(config["stand"]["servers"]["database_url"])
    engine = create_sql_alchemy_async_engine(config)
    asyncio.run(main(engine))
