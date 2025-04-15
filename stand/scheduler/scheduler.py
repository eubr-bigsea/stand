import asyncio
import logging

from datetime import datetime
from datetime import timezone
from stand.scheduler.trigger_scheduled_jobs import (
    trigger_scheduled_pipeline_steps,
)
from stand.models import (
    StatusExecution,
)
from stand.scheduler.update_pipeline_runs import get_pipeline_run_commands
from stand.scheduler.utils import (
    get_latest_pipeline_runs,
    get_pipeline_run,
    get_pipelines,
    load_config,
    pipeline_steps_have_valid_schedulings,
)

logger = logging.getLogger(__name__)


async def check_and_execute(config):
    while True:
        current_time = datetime.now(timezone.utc)
        logger.info("Checking scheduler. Now = %s", current_time.isoformat())
        try:
            await execute(config, current_time=current_time)
        except Exception as e:
            logger.exception(e)

        current_time = datetime.now(timezone.utc)
        remaining_seconds = (
            60 - current_time.second - (current_time.microsecond / 1_000_000)
        )
        await asyncio.sleep(remaining_seconds)  # Sleep until the next minute





async def execute(config, current_time=None):
    current_time = current_time or datetime.now(timezone.utc)

    # fetch pipelines and filter valid ones
    updated_pipelines = await get_pipelines(config["stand"]["services"]["tahiti"])
    valid_schedule_pipelines = filter_valid_schedule_pipelines(updated_pipelines)
    unvalid_schedule_pipelines = filter_non_valid_schedule_pipelines(updated_pipelines)

    if logger.isEnabledFor(logging.INFO):
        
        logger.info("fetched %s pipelines.", len(updated_pipelines))
        
    # fetch active pipeline runs with scheduled pipelines
    active_pipeline_runs = await fetch_active_pipeline_runs(
        config, valid_schedule_pipelines
    )
    if logger.isEnabledFor(logging.INFO):
        logger.info("fetched %s active and scheduled pipelines runs.", len(active_pipeline_runs))

    # update pipeline runs for scheduled pipelines
    update_pipeline_runs_commands = get_pipeline_run_commands(
        updated_pipelines=valid_schedule_pipelines,
        pipeline_runs=active_pipeline_runs,
        current_time=current_time,
    )
    await execute_commands(update_pipeline_runs_commands, config)

    # fetch active pipeline runs again (to account for new runs)
    active_pipeline_runs = await fetch_active_pipeline_runs(
        config, valid_schedule_pipelines
    )

    # trigger pipeline step commands for scheduled pipelines
    trigger_commands = prepare_trigger_commands(
        active_pipeline_runs, valid_schedule_pipelines, current_time, scheduled=True
    )
    await execute_commands(trigger_commands, config, step_logging=True)

    # fetching pipeline runs created by the API
    active_pipeline_runs = await fetch_active_pipeline_runs(
        config, unvalid_schedule_pipelines,latest_only=False
    )
    if logger.isEnabledFor(logging.INFO):
        logger.info("fetched %s active and API created pipelines runs.", len(active_pipeline_runs))

    # triggering pipeline steps for non scheduled pipelines (pipeline runs created by api)
    trigger_commands = prepare_trigger_commands(
        active_pipeline_runs, unvalid_schedule_pipelines, current_time, scheduled=False
    )
    await execute_commands(trigger_commands, config, step_logging=True)
    return [update_pipeline_runs_commands, trigger_commands]


def filter_valid_schedule_pipelines(updated_pipelines):
    """Filters pipelines with valid scheduling steps."""
    return {
        id: pipeline
        for id, pipeline in updated_pipelines.items()
        if (
            pipeline["run_creation_method"] == "scheduler"
            and pipeline["enabled"]
            and pipeline_steps_have_valid_schedulings(pipeline["steps"])
        )
    }


def filter_non_valid_schedule_pipelines(updated_pipelines):
    """Filters pipelines with non valid scheduling steps, that
    were crerated by anothe methods."""
    for id, pipeline in updated_pipelines.items():
        if pipeline["run_creation_method"] != "scheduler":
            print(pipeline["run_creation_method"])
    return {
        id: pipeline
        for id, pipeline in updated_pipelines.items()
        if (
            pipeline["enabled"]
            and not pipeline_steps_have_valid_schedulings(pipeline["steps"])
        )
    }


async def fetch_active_pipeline_runs(config, valid_schedule_pipelines,latest_only=True):
    """Fetches the latest pipeline runs for valid pipelines."""
    return await get_latest_pipeline_runs(
       
        config["stand"]["services"]["stand"],
        pipeline_ids=valid_schedule_pipelines.keys(),
        latest_only=latest_only
    )


def prepare_trigger_commands(
    active_pipeline_runs, valid_schedule_pipelines, current_time, scheduled
):
    """Prepares commands to trigger scheduled pipeline steps."""
    trigger_commands = []
    for run in active_pipeline_runs:
        step_infos = valid_schedule_pipelines[run.pipeline_id]["steps"]
        step_runs = [step for step in run.steps]
        if  run.status in(StatusExecution.COMPLETED, StatusExecution.CANCELED,StatusExecution.ERROR):
            continue
        if(len(step_infos)!=len(step_runs)):
            if logger.isEnabledFor(logging.INFO):
                log_message = (
                    f"Pipeline run {run.id} and its base Pipeline {run.pipeline_id} don't have the same number of steps.The pipeline's steps were changed after the run was created. This run will be ignored until a user manually complete it or cancel it"
                )
                logger.info(log_message)
            continue
      
        new_command = trigger_scheduled_pipeline_steps(
            pipeline_run=run,
            time=current_time,
            steps=step_infos,
            step_runs=step_runs,
            scheduled=scheduled,
        )
        if new_command:
            trigger_commands.append(new_command)
    return trigger_commands


async def execute_commands(commands, config, step_logging=False):
    """Executes a list of commands with optional logging for step execution."""
    for command in commands:
        if logger.isEnabledFor(logging.INFO):
            log_message = (
                f"Executing command {command.pipeline_step.id} for step."
                if step_logging
                else f"Executing command {command}."
            )
            logger.info(log_message)
        await command.execute(config)


async def main(config):
    await check_and_execute(config=config)


if __name__ == "__main__":
    config = load_config()
    logger.info("Starting Stand Scheduler")
    asyncio.run(main(config))
