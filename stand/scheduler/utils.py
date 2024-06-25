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


async def get_canceled_runs(session):
    return await session.execute(
        select(PipelineRun).filter(PipelineRun.status == StatusExecution.CANCELED)
    ).fetchall()

async def get_latest_pipeline_step_run(session: AsyncSession, run: PipelineRun) -> PipelineStepRun:
    # Return the latest pipeline step run given a pipeline run
    query = select(PipelineStepRun).filter(PipelineStepRun.id == run.last_completed_step)
    return await session.execute(query).one()


async def get_latest_job_from_pipeline_step_run(session: AsyncSession, step_run: PipelineStepRun) -> Job:
    query = (select(Job, func.max(Job.finished).label("latest_job_finished_time"))
             .filter(Job.pipeline_step_run_id == step_run.id)
             .group_by(Job.pipeline_step_run_id))
    
    return await session.execute(query).one()

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
    print(updated_pipelines)
    return updated_pipelines




def create_sql_alchemy_async_engine(config: typing.Dict):
    url = config["stand"]["servers"]["database_url"]
    if "sqlite" in url:
        url = url.replace("sqlite", "sqlite+aiosqlite")
    return create_async_engine(url, echo=True, future=True)



def build_session_maker(engine: AsyncEngine):
    return sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


def load_config():
    config_file = os.environ.get("STAND_CONFIG")
    with open(config_file, "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    return config


def get_latest_job(run:PipelineRun)->Job:
    "name"
    pass

def get_active_step_run(run:PipelineRun)->Job:
    "name"
    pass