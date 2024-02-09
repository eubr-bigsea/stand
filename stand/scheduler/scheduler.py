# Basic code to start a scheduler using aiocron package
import asyncio
from datetime import datetime
import os

from croniter import croniter
import yaml
# from . import all_cron_executions
from sqlalchemy.ext.asyncio import create_async_engine

import typing
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import select
from stand.models import Job
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import AsyncSession


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


def update_pipelines_window():
    # Read pipelines
    # Read current windows

    # Remove window for cancelled pipelines
    # Add window for new pipelines
    # Update window for running pipelines

    # Create expected steps window for pipeline
    pass


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
    async_session = sessionmaker(
        engine, expire_on_commit=False, class_=AsyncSession
    )
    async with async_session() as session:
        result = await session.execute(select(Job).filter(Job.id == 1).limit(1))
    # async with engine.begin() as conn:
    #    result = await conn.execute(select(Job).filter(Job.id == 1))
    job: Job = result.one()
    print(type(job))
    await session.close()
    await engine.dispose()

if __name__ == "__main__":
    config = load_config()
    print(config['stand']['servers']['database_url'])
    engine = create_sql_alchemy_async_engine(config)
    asyncio.run(main(engine))
