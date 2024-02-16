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
    config_file = os.environ.get('STAND_CONFIG')
    with open(config_file, 'r') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    return config


def create_sql_alchemy_async_engine(config: typing.Dict):
    url = config['stand']['servers']['database_url']
    if 'sqlite' in url:
        url = url.replace('sqlite', 'sqlite+aiosqlite')
    return create_async_engine(url,
                               echo=True,
                               future=True)

def build_session_maker(engine: AsyncEngine):
    return sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

async def update_pipelines_runs(config: typing.Dict, engine: AsyncEngine):
    """ Update pipelines' runs
    """
    tahiti_config = config['stand']['services']['tahiti']
    
    # Window is defined in config file (default = 7 days)
    days = config['stand'].get('pipeline').get('days', 7)
    updated_pipelines = get_pipelines(tahiti_config, days)
    
    async_session = build_session_maker(engine)
    async with async_session() as session:
        # FIXME: Add more filters
        # Add run for new pipelines
        # Create expected steps run for pipeline

        # Read current runs
        runs = await get_runs(session, updated_pipelines.keys())

        # Remove run for cancelled pipelines
        # canceled = await get_canceled_runs(session)
        # Update run for running pipelines
    pass

def get_pipelines(tahiti_config: typing.Dict, days: int) -> typing.Dict[int, typing.Dict]:
    """ Read pipelines from tahiti API.
    Don't need to read all pipelines, only those updated in the last window.
    """
    
    tahiti_api_url = tahiti_config['url']
    reference = date.today() - timedelta(days=days)
    params = {
        'after': reference
    }
    headers = {
        'X-Auth-Token': tahiti_config['auth_token']
    }
    resp = requests.get(f'{tahiti_api_url}/pipelines', params, headers=headers)
    if resp.status_code != 200:
        raise Exception(f'Error {resp.status_code} while getting pipelines')
    updated_pipelines = dict([[p['id'], p] for p in resp.json()['list']])
    return updated_pipelines

async def get_canceled_runs(session):
    return await session.execute(
            select(PipelineRun)
                .filter(PipelineRun.status == StatusExecution.CANCELED)).fetchall()

async def get_runs(session, pipeline_ids):
    # Subquery to get the most recent run for each pipeline_id
    subquery = (
        session.query(
            PipelineRun.pipeline_id,
            func.max(PipelineRun.start).label("max_start")
        )
        .group_by(PipelineRun.pipeline_id)
        .subquery()
    )

    # Join the subquery with the PipelineRun table to get the full 
    # PipelineRun entities
    query = (
        session.query(PipelineRun)
        .join(subquery, and_(
            PipelineRun.pipeline_id == subquery.c.pipeline_id,
            PipelineRun.start == subquery.c.max_start
        ))
        .order_by(PipelineRun.pipeline_id)
    )
    return await query.all()


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
    '* * * * *': function1,  # Every minute
    '*/2 * * * *': function2,  # Every two minutes
}


async def check_and_execute():
    while True:
        current_time = datetime.now()
        remaining_seconds = 60 - current_time.second - \
            (current_time.microsecond / 1_000_000)
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
    #job: Job = result.one()
    job: Job = result.scalars().one()
    print('>>>>>>>>', job, type(job))
    await session.close()
    await engine.dispose()

if __name__ == "__main__":
    config = load_config()
    print(config['stand']['servers']['database_url'])
    engine = create_sql_alchemy_async_engine(config)
    asyncio.run(main(engine))
