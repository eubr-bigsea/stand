# Basic code to start a scheduler using aiocron package
import asyncio
import os
import typing
from datetime import date, datetime, timedelta

import requests
import yaml
from croniter import croniter
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import and_

from stand.models import Job, PipelineRun, PipelineStepRun, StatusExecution


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
    run.status = state
    await session.merge(run)
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
                        # FIXME: Test if all steps were executed and if so,
                        # marks run as FINISHED
                        await update_run(session, pipeline['updated'], run)

                elif run.status == StatusExecution.PENDING:
                    pass
                elif (run.status == StatusExecution.CANCELED and
                        pipeline["enabled"]):
                    # Create run for pipeline
                    await create_pipeline_run(session, pipeline, user={})
                elif run.status == StatusExecution.INTERRUPTED:
                    if run.finish < now:
                        await create_pipeline_run(session, pipeline, user={})
            else:
                # Create run for pipeline
                await create_pipeline_run(session, pipeline, user={})
            pass


async def update_pipelines_runs_from_jobs(config: typing.Dict, engine: AsyncEngine):
    """Update pipelines' runs from jobs results """
    async_session = build_session_maker(engine)
    async with async_session() as session:
        jobs = get_jobs(session)

        for job in jobs:
            if job.status in (StatusExecution.ERROR, StatusExecution.RUNNING):
                change_run_step_state(session,
                                      job.pipeline_step_run,
                                      job.status, job.status, True)
            elif job.status == StatusExecution.FINISHED:
                pass
                #job.pipeline_step_run.pipeline_run.steps
                # FIXME test if it's the last step of the pipeline
                # if so, change run state to FINISHED
                change_run_step_state(session,
                                      job.pipeline_step_run,
                                      job.status, job.status, True)
            elif job.status == StatusExecution.PENDING:
                pass


async def change_run_step_state(session: AsyncSession,
                                step_run: PipelineStepRun,
                                run_status: StatusExecution,
                                step_status: StatusExecution,
                                commit: bool = False) -> None:

    # Begin trans

    await change_run_state(session,
                           step_run.pipeline_run,
                           run_status)
    step_run.status = step_status
    session.add(step_run)
    if commit:
        await session.commit()


async def update_run(session: AsyncSession, updated: datetime,
                     run: PipelineRun, commit: bool = False) -> None:
    """
    Update run with latest pipeline data
    """
    run.updated = updated
    run.status = StatusExecution.PENDING
    if commit:
        await session.commit()
    # Create expected steps run for pipeline


async def get_jobs(session: AsyncSession) -> typing.List[Job]:
    return []


async def cancel_run(session, run):
    await session.delete(run)
    await session.commit()


async def create_pipeline_run(
        session: AsyncSession, pipeline: typing.Dict, user: typing.Dic, commit: bool = False) -> None:
    run: PipelineRun = PipelineRun(
        pipeline_id=pipeline["id"],
        updated=pipeline["updated"],
        user=user,
        status=StatusExecution.WAITING,
        final_status=StatusExecution.WAITING,
        start=datetime.now(),  # FIXME
        finish=datetime.now(),  # FIXME
        steps=[]  # FIXME
    )
    session.add(run)
    if commit:
        await session.commit()


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
        for crontab_exp, func_ in crontab_mapping.items():
            matches = croniter.match(crontab_exp, current_time)
            print(crontab_exp, current_time, matches)
            if matches:
                asyncio.create_task(func_())


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
