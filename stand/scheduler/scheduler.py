import asyncio
from datetime import datetime

from stand.scheduler.status_control import propagate_job_status
from stand.scheduler.trigger_scheduled_jobs import (
    trigger_scheduled_pipeline_steps,
)
from stand.scheduler.update_pipeline_runs import update_pipelines_runs
from stand.scheduler.utils import (
    build_session_maker,
    create_sql_alchemy_async_engine,
    get_latest_job_from_pipeline_step_run,
    get_latest_pipeline_step_run,
    get_pipelines,
    get_runs,
)


async def execute(session, current_time):
    # returned as pipeline_id:pipeline_in_json dict
    updated_pipelines = await get_pipelines(tahiti_config=config, days=7)
    # returned as list of active pipeline runs in json
    active_pipeline_runs = await get_runs(
        session=session, pipeline_ids=updated_pipelines.keys()
    )

    update_pipeline_runs_commands = update_pipelines_runs(
        updated_pipelines=updated_pipelines,
        pipeline_runs=active_pipeline_runs,
        current_time=current_time,
    )
    for command in update_pipeline_runs_commands:
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
        # both the latest job and active step run needs to
        # be called here because trigger_commands can alter these,
        # so they cant be called up top for the whole batch of runs
        active_step_run = get_latest_pipeline_step_run(session=session, run=run)
        latest_job = get_latest_job_from_pipeline_step_run(
            session=session, step_run=active_step_run
        )

        # latest_job will always have a relationship to active_step_run
        propagate_commands = propagate_job_status(
            run=run, latest_job=latest_job, active_step_run=active_step_run
        )
        for command in propagate_commands:
            command.execute(session)

    # not used
    return [update_pipeline_runs_commands, trigger_commands, propagate_commands]


async def check_and_execute():
    engine = create_sql_alchemy_async_engine(config)
    session = build_session_maker(engine=engine)
    while True:
        current_time = datetime.now()
        execute(session=session, current_time=current_time)

        # updating current_time to consider execute() execution time
        current_time = datetime.now()
        remaining_seconds = (
            60 - current_time.second - (current_time.microsecond / 1_000_000)
        )
        await asyncio.sleep(remaining_seconds)  # Sleep until next minute


async def main(engine):
    await check_and_execute()


if __name__ == "__main__":
    config = {"stand": {"servers": {"database_url": "localhost:33602"}}}
    print(config["stand"]["servers"]["database_url"])
    engine = create_sql_alchemy_async_engine(config)
    print(engine)
