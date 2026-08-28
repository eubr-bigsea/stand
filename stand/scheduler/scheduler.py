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

from stand.scheduler.commands import UpdatePipelineRunStatus
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




async def execute(config,current_queue, current_time=None,concurrent_jobs=5):
    current_time = current_time or datetime.now(timezone.utc)

    # fetch pipelines and filter valid ones
    updated_pipelines = await get_pipelines(config["stand"]["services"]["tahiti"])
    pipelines = filter_api_created_runs(updated_pipelines)

    if logger.isEnabledFor(logging.INFO):
        
        logger.info("fetched %s pipelines.", len(updated_pipelines))
        

    # fetching pipeline runs created by the API (pipelines that arent scheduled)
    active_pipeline_runs = await fetch_active_pipeline_runs(
        config, pipelines,latest_only=False
    )
    
    new_queue = manage_pipeline_queue(all_runs=active_pipeline_runs,pipelines_info=pipelines)
    
    if logger.isEnabledFor(logging.INFO):
        logger.info("fetched %s active  pipelines runs.", len(active_pipeline_runs))

        logger.info( f"Current pipeline_run ids execution queue:[{[p.id for p in new_queue[0:5]]}]")
    # triggering pipeline steps for non scheduled pipelines (pipeline runs created by api)
    if len(new_queue)>0:
        trigger_commands = prepare_trigger_commands(
            new_queue[0:concurrent_jobs], pipelines, current_time, scheduled=False
        )
      
        await execute_commands(trigger_commands, config, step_logging=True)
    

    return []




def filter_api_created_runs(updated_pipelines):
    """Filters pipelines runs that were create by the API."""

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
                    f"Pipeline run {run.id} and its base Pipeline {run.pipeline_id} don't have the same number of steps. This run is being cancelled"
                )
                logger.info(log_message)
                cancel_pipeline_command = UpdatePipelineRunStatus(pipeline_run=run,status="CANCELED")
                trigger_commands.append(cancel_pipeline_command)
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
