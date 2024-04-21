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


def load_config():
    config_file = os.environ.get("STAND_CONFIG")
    with open(config_file, "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    return config


def create_sql_alchemy_async_engine(config: typing.Dict):
    url = config["stand"]["servers"]["database_url"]
    if "sqlite" in url:
        url = url.replace("sqlite", "sqlite+aiosqlite")
    return create_async_engine(url, echo=True, future=True)


def build_session_maker(engine: AsyncEngine):
    return sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def change_run_state(session: AsyncSession, run: PipelineRun,
                           state: StatusExecution) -> None:
    run: PipelineRun = session.merge(run)
    run.status = state
    await session.commit()



async def update_pipelines_runs(config: typing.Dict, engine: AsyncEngine):
    
    """Update pipelines' runs"""
    tahiti_config = config["stand"]["services"]["tahiti"]

    # Window is defined in config file (default = 7 days)
    days = config["stand"].get("pipeline").get("days", 7)
    updated_pipelines = get_pipelines(tahiti_config, days)
    now = datetime.now()

    async_session = build_session_maker(engine)
    async with async_session() as session:
        # FIXME: Add more filters
        # Add run for new pipelines
        # Create expected steps run for pipeline

        # Read current runs
        runs = dict(
            [
                [r.pipeline_id, r]
                for r in await get_runs(session, updated_pipelines.keys())
            ]
        )
       
        for pipeline in updated_pipelines.values():
         
            run: PipelineRun = runs.get(pipeline["id"])
            if run:
                if pipeline['enabled'] is False:
                    if run.status != StatusExecution.CANCELED:
                        # Remove run for disabled pipelines
                        await cancel_run(session, run)
                elif run.status == StatusExecution.RUNNING:   
                    if run.finish < now:
                        await change_run_state(session, run,
                                               StatusExecution.PENDING)
                        await create_pipeline_run(session, pipeline, user={})
                    # Test if run is using latest pipeline data
                    elif run.updated < pipeline["updated"]:
                        await update_run(session, pipeline['updated'], run)

                elif run.status == StatusExecution.PENDING:
                    pass
                
                elif (run.status == StatusExecution.CANCELED ):
                   
                    await create_pipeline_run(session, pipeline, user={})
                elif run.status == StatusExecution.INTERRUPTED:
                    if run.finish < now:
                        await create_pipeline_run(session, pipeline, user={})
            else:
                if pipeline['enabled']:
                    await create_pipeline_run(session, pipeline, user={})
            
    




async def update_run(session: AsyncSession, updated: datetime,
                     run: PipelineRun) -> None:
    """
    Update run with latest pipeline data
    """
    run.updated = updated
    run.status = StatusExecution.PENDING
    await session.commit()
    # Create expected steps run for pipeline


async def cancel_run(session, run):
    await session.delete(run)
    await session.commit()


async def create_pipeline_run(
        session: AsyncSession, pipeline: typing.Dict, user: typing.Dict) -> None:
    pass


def get_pipelines(tahiti_config: typing.Dict, days: int
                  ) -> typing.Dict[int, typing.Dict]:
    """Read pipelines from tahiti API.
    Don't need to read all pipelines, only those updated in the last window.
    """

    tahiti_api_url = tahiti_config["url"]
    reference = date.today() - timedelta(days=days)
    params = {"after": reference}
    headers = {"X-Auth-Token": tahiti_config["auth_token"]}
    resp = requests.get(f"{tahiti_api_url}/pipelines", params, headers=headers)
    if resp.status_code != 200:
        raise Exception(f"Error {resp.status_code} while getting pipelines")

    updated_pipelines = dict([[p["id"], p] for p in resp.json()["list"]])
    return updated_pipelines


async def get_canceled_runs(session):
    return await session.execute(
        select(PipelineRun).filter(
            PipelineRun.status == StatusExecution.CANCELED)
    ).fetchall()


async def get_runs(session, pipeline_ids):
    # Subquery to get the most recent run for each pipeline_id
    subquery = (
        select(PipelineRun.pipeline_id, func.max(
            PipelineRun.start).label("max_start"))
        .filter(PipelineRun.pipeline_id.in_(pipeline_ids))
        .group_by(PipelineRun.pipeline_id)
        .subquery()
    )
    # subquery = await session.execute(q)

    # Join the subquery with the PipelineRun table to get the full
    # PipelineRun entities
    q = (
        select(PipelineRun)
        .join(
            subquery,
            and_(
                PipelineRun.pipeline_id == subquery.c.pipeline_id,
                PipelineRun.start == subquery.c.max_start,
            ),
        )
        .order_by(PipelineRun.pipeline_id)
    )
    result = await session.execute(q)

    return result.fetchall()


def update_pipeline_steps_status():
    # Check if there is a Job associated to the step
    # Update the step status
    pass





def get_next_scheduled_date(cron_expr:str,
                                    current_time: datetime,
                                    )->datetime:
    """
    receives a cron_exp and current_time, 
    returns datetime of next occurence of the scheduling
    based on current_time argument
    """
  
    iter = croniter(cron_expr,current_time)
    next_occurrence = iter.get_next(datetime)
    
    return next_occurrence
    
    #bad name
def pipeline_run_should_be_created(current_time: datetime,
                                   first_step: PipelineStep,
                                   last_step :PipelineStep)-> bool:
    """
    Checks if a pipeline run should be created based on its
    associated pipeline step runs. If first_step isnt the first
    next scheduled one , then dont create it.
    """
    ## this function will probably get more complex
    first_step_expr = first_step.scheduling
    first_step_next_date= get_next_scheduled_date(cron_expr=first_step_expr,
                                                           current_time=current_time)
    last_step_expr = last_step.scheduling
    last_step_next_date = get_next_scheduled_date(cron_expr=last_step_expr,
                                                           current_time=current_time)
    #last step is scheduled first than last step
    #it implies  current time is betwween fist and last step
    if (first_step_next_date>last_step_next_date):
        return False
    else:
        return True
    
    



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
