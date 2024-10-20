import asyncio
from datetime import datetime

from stand.scheduler.trigger_scheduled_jobs import (
    trigger_scheduled_pipeline_steps,
)
from stand.scheduler.update_pipeline_runs import get_pipeline_run_commands
from stand.scheduler.utils import (
    get_latest_pipeline_runs,
    get_pipeline_run,
    get_pipelines,
    load_config,
    pipeline_steps_have_valid_schedulings,
)


async def check_and_execute(config):
    while True:
        try:
            current_time = datetime.now()
            await execute(config, current_time=current_time)
        except Exception as e:
            print(f"an error occurred: {e}")

        current_time = datetime.now()
        remaining_seconds = (
            60 - current_time.second - (current_time.microsecond / 1_000_000)
        )
        print(datetime.now())
        await asyncio.sleep(remaining_seconds)  # Sleep until the next minute


# FIXME: Theres redundancy in the api call to get the pipelines
# first they are called in a batch then called individualy,bc the
# batch api doesnt return the step runs
async def execute(config, current_time=datetime.now()):
    # returned as pipeline_id:pipeline_in_json dict
    updated_pipelines = await get_pipelines(
        tahiti_config=config["stand"]["services"]["tahiti"], days=7
    )

    # only using pipelines with valid scheduling steps
    valid_schedule_pipelines = {}
    for id in updated_pipelines:
        if pipeline_steps_have_valid_schedulings(updated_pipelines[id]["steps"]):
            valid_schedule_pipelines[id] = updated_pipelines[id]

    # FIXME
    # this function doesnt return the pipeline steps correctly
    active_pipeline_runs = await get_latest_pipeline_runs(
        config["stand"]["services"]["stand"],
        pipeline_ids=valid_schedule_pipelines.keys(),
    )

    # FIXME
    # need to use the individual pipeline run step to get the steps runs
    valid_pipeline_runs = []
    for i in active_pipeline_runs:
        p = await get_pipeline_run(config["stand"]["services"]["stand"], i.id)
        valid_pipeline_runs.append(p)

    # managing states and creating pipelines
    update_pipeline_runs_commands = get_pipeline_run_commands(
        updated_pipelines=valid_schedule_pipelines,
        pipeline_runs=valid_pipeline_runs,
        current_time=current_time,
    )

    for command in update_pipeline_runs_commands:
        print(command)
        await command.execute(config)

    # must be called again bc pipeline_runs_commands can create new runs
    # TODO
    # instead of calling it again, check the commands and see what
    # pipeline was created from there ,or use the api and only get
    # the pipelines created less than 2 minutes ago.
    active_pipeline_runs = await get_latest_pipeline_runs(
        config["stand"]["services"]["stand"],
        pipeline_ids=valid_schedule_pipelines.keys(),
    )
    valid_pipeline_runs = []
    for i in active_pipeline_runs:
        p = await get_pipeline_run(config["stand"]["services"]["stand"], i.id)
        valid_pipeline_runs.append(p)
    trigger_commands = []

    # triggering stepruns
    for run in valid_pipeline_runs:
        step_runs = [step for step in run.steps]
        step_infos = valid_schedule_pipelines[run.pipeline_id]["steps"]

        new_command = trigger_scheduled_pipeline_steps(
            pipeline_run=run,
            time=current_time,
            steps=step_infos,
            step_runs=step_runs,
        )
        if new_command != None:
            trigger_commands.append(new_command)

    for command in trigger_commands:
        await command.execute(config)

    return [update_pipeline_runs_commands, trigger_commands]


async def main(config):
    today = datetime.today()
    # specific_time = today.replace(month=11,day=8,hour=9, minute=27, second=12, microsecond=12312)
    # await execute(config=config,current_time= specific_time)
    await check_and_execute(config=config)


if __name__ == "__main__":
    config = load_config()

    asyncio.run(main(config))
