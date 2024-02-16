# Basic code to start a scheduler using aiocron package
import asyncio
from datetime import datetime, timedelta, date
import os

from croniter import croniter
from sqlalchemy.sql import and_
import yaml

# from . import all_cron_executions
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

import typing
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import select
from stand.models import Job, PipelineRun, StatusExecution
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import AsyncSession
import requests


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


async def update_pipelines_runs(config: typing.Dict, engine: AsyncEngine):
    """Update pipelines' runs"""
    tahiti_config = config["stand"]["services"]["tahiti"]

    # Window is defined in config file (default = 7 days)
    days = config["stand"].get("pipeline").get("days", 7)
    updated_pipelines = get_pipelines(tahiti_config, days)

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

        running_statuses = [StatusExecution.PENDING, StatusExecution.RUNNING]
        for pipeline in updated_pipelines.values():
            if pipeline["id"] in runs:
                # Remove run for disabled pipelines
                run = runs[pipeline["id"]]
                if pipeline["enabled"] is False and run.status in running_statuses:
                    await cancel_run(session, run)
                continue
            else:
                # Create run for pipeline
                create_pipeline_run(session, pipeline, user={})
            pass
        # canceled = await get_canceled_runs(session)
        # Update run for running pipelines
    pass


async def cancel_run(session, run):
    await session.delete(run)
    await session.commit()


def create_pipeline_run(
    session: AsyncSession, pipeline: typing.Dict, user: typing.Dict
) -> None:
    pass


def get_pipelines(
    tahiti_config: typing.Dict, days: int
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
        select(PipelineRun).filter(PipelineRun.status == StatusExecution.CANCELED)
    ).fetchall()


async def get_runs(session, pipeline_ids):
    # Subquery to get the most recent run for each pipeline_id
    subquery = (
        select(PipelineRun.pipeline_id, func.max(PipelineRun.start).label("max_start"))
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


# Define your functions here
async def function1():
    print("Executing function 1")


async def function2():
    print("Executing function 2")


# Mapping crontab expressions to functions
crontab_mapping = {
    "* * * * *": function1,  # Every minute
    "*/2 * * * *": function2,  # Every two minutes
}


async def check_and_execute():
    while True:
        current_time = datetime.now()
        remaining_seconds = (
            60 - current_time.second - (current_time.microsecond / 1_000_000)
        )
        await asyncio.sleep(remaining_seconds)  # Sleep until next minute
        for crontab_exp, func in crontab_mapping.items():
            matches = croniter.match(crontab_exp, current_time)
            print(crontab_exp, current_time, matches)
            if matches:
                asyncio.create_task(func())


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
