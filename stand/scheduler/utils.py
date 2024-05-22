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

from stand.services.tahiti_service import query_tahiti


async def get_latest_pipeline_step_run(session: AsyncSession, run: PipelineRun) -> PipelineStepRun:
    # Return the latest pipeline step run given a pipeline run
    query = select(PipelineStepRun).filter(PipelineStepRun.id == run.last_completed_step)
    return await session.execute(query).one()


async def get_latest_job_from_pipeline_step_run(session: AsyncSession, step_run: PipelineStepRun) -> Job:
    query = (select(Job, func.max(Job.finished).label("latest_job_finished_time"))
             .filter(Job.pipeline_step_run_id == step_run.id)
             .group_by(Job.pipeline_step_run_id))
    
    return await session.execute(query).one()


async def update_pipeline_step_run_status(session: AsyncSession, step_run: PipelineStepRun, status: StatusExecution):
    """
    Update StepRun with latest execution status
    """
    step_run.status = status
    session.flush()


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

async def update_run(session: AsyncSession, updated: datetime,
                     run: PipelineRun) -> None:
    """
    Update run with latest pipeline data
    """
    run.updated = updated
    run.status = StatusExecution.PENDING
    await session.commit()
    # Create expected steps run for pipeline


def get_pipelines(
    tahiti_config: typing.Dict, days: int
) -> typing.Dict[int, typing.Dict]:
    """Read pipelines from tahiti API.
    Don't need to read all pipelines, only those updated in the last window.
    """

    reference = date.today() - timedelta(days=days)
    params = {"after": reference}
    resp = query_tahiti(item_path='/pipelines', params=params) # return data in json
    updated_pipelines = dict([[p["id"], p] for p in resp.json()["list"]])
    return updated_pipelines


async def create_pipeline_run(
    session: AsyncSession, pipeline: typing.Dict, user: typing.Dict
) -> None:
    pipeline_run = PipelineRun(pipeline)
    await session.add(pipeline_run)


def create_sql_alchemy_async_engine(config: typing.Dict):
    url = config["stand"]["servers"]["database_url"]
    if "sqlite" in url:
        url = url.replace("sqlite", "sqlite+aiosqlite")
    return create_async_engine(url, echo=True, future=True)


async def update_pipeline_run_status(
    session: AsyncSession, run: PipelineRun, state: StatusExecution
) -> None:
    run: PipelineRun = session.merge(run)
    run.status = state
    await session.commit()


def build_session_maker(engine: AsyncEngine):
    return sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

async def get_job_status(session, job: Job) -> StatusExecution:
    # caso o objeto Job é passado (ele ja tem todas as infos dentro dele)
    return job.status
    # caso seja passado job_id ao inves do objeto Job
    query = select(Job).filter(Job.id == job_id)
    result = await session.execute(query).fisrt()
    return result.status

def get_pipeline_steps(pipeline: Pipeline):
    # objeto Pipeline já é passado com todas as infos
    return pipeline.steps
    # id da pipeline é passado
    resp = query_tahiti(item_path='/pipelines', item_id=pipeline_id) # return data in json
    return resp[0]


def create_step_run(session, pipeline_step: PipelineStep):
    pipeline_step_run = PipelineStepRun(pipeline_step)
    session.add(pipeline_step_run)


def increase_last_completed_step(session, pipeline_run: PipelineRun):
    pipeline_run.last_completed_step += 1
    session.flush()
