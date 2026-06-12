import asyncio
import logging

from datetime import datetime
from datetime import timezone
from stand.scheduler.trigger_scheduled_jobs import (
    trigger_scheduled_pipeline_steps,
    get_step_is_user_triggered
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
    current_queue= []
    while True:
        current_time = datetime.now(timezone.utc)
        logger.info("Checking scheduler. Now = %s", current_time.isoformat())
        try:
            current_queue=  await execute(config,current_queue= current_queue,current_time=current_time)
        except Exception as e:
            logger.exception(e)

        current_time = datetime.now(timezone.utc)
        remaining_seconds = (
            60 - current_time.second - (current_time.microsecond / 1_000_000)
        )
        await asyncio.sleep(remaining_seconds)  # Sleep until the next minute





async def execute(config,current_queue, current_time=None,concurrent_jobs=2):
    current_time = current_time or datetime.now(timezone.utc)

    # fetch pipelines and filter valid ones
    updated_pipelines = await get_pipelines(config["stand"]["services"]["tahiti"])
    invalid_schedule_pipelines = filter_non_valid_schedule_pipelines(updated_pipelines)

    if logger.isEnabledFor(logging.INFO):
        
        logger.info("fetched %s pipelines.", len(updated_pipelines))
        

    # fetching pipeline runs created by the API (pipelines that arent scheduled)
    active_pipeline_runs = await fetch_active_pipeline_runs(
        config, invalid_schedule_pipelines,latest_only=False
    )
    
    new_queue = manage_pipeline_queue(all_runs=active_pipeline_runs,pipelines_info=invalid_schedule_pipelines)
    
    if logger.isEnabledFor(logging.INFO):
        logger.info("fetched %s active and API created pipelines runs.", len(active_pipeline_runs))

    # triggering pipeline steps for non scheduled pipelines (pipeline runs created by api)
    if len(new_queue)>0:
        trigger_commands = prepare_trigger_commands(
            new_queue[0:concurrent_jobs], invalid_schedule_pipelines, current_time, scheduled=False
        )
        await execute_commands(trigger_commands, config, step_logging=True)
    

    return []


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

def manage_pipeline_queue(all_runs,pipelines_info):
    
    queue=[]

    for run in all_runs:
        if run.id not in [r.id for r in queue ]:
            queue.append(run)
      
            
  
    #removing runs that are completed or with an error
    queue = [run for run in queue if run.status not in[StatusExecution.ERROR, StatusExecution.CANCELED,StatusExecution.COMPLETED]]
    
    #removing runs that had the last step already executed
    queue = [run for run in queue if run.last_executed_step  != len(pipelines_info[run.pipeline_id]["steps"])]
    
    #removing runs with all steps completed but that for some reason dont have "completed" as a status
    queue = [run for run in queue if not (len({step_run.status for step_run in run.steps})==1 and StatusExecution.COMPLETED  in {step_run.status for step_run in run.steps})]

    #kicking elements out if their next step needs user input
    new_queue =[]
    for run in queue:
        step_infos = pipelines_info[run.pipeline_id]["steps"]
        last_executed_step = run.last_executed_step 
        next_step = step_infos[last_executed_step]
        if "scheduling" in next_step and get_step_is_user_triggered(next_step["scheduling"]):
            continue
        else:
            new_queue.append(run)
    return new_queue

    
async def main(config):
    await check_and_execute(config=config)


if __name__ == "__main__":
    config = load_config()
    logger.info("Starting Stand Scheduler")
    asyncio.run(main(config))
