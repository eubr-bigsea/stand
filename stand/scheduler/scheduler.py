import asyncio
from datetime import datetime

from stand.scheduler.status_control import propagate_job_status
from stand.scheduler.trigger_scheduled_jobs import (
    trigger_scheduled_pipeline_steps,
)
from stand.scheduler.update_pipeline_runs import get_pipeline_run_commands
from stand.scheduler.utils import (
    get_latest_job_from_pipeline_step_run,
    get_latest_pipeline_step_run,
    get_pipelines,
    get_latest_pipeline_runs,
    load_config,
    pipeline_steps_have_valid_schedulings
)




async def check_and_execute(config):
    while True:
        current_time = datetime.now()
        execute(config, current_time=current_time)

        # updating current_time to consider execute() execution time
        current_time = datetime.now()
        remaining_seconds = (
            60 - current_time.second - (current_time.microsecond / 1_000_000)
        )
        await asyncio.sleep(remaining_seconds)  # Sleep until next minute


async def execute(config, current_time):
    # returned as pipeline_id:pipeline_in_json dict
    updated_pipelines = await get_pipelines(tahiti_config=config['stand']['services']['tahiti'], days=7)
  
  
    #only using pipelines with valid scheduling steps
    valid_schedule_pipelines ={}
    for  id in updated_pipelines:
        if pipeline_steps_have_valid_schedulings(updated_pipelines[id]["steps"]):
        
            valid_schedule_pipelines[id] = updated_pipelines[id]
     
    active_pipeline_runs = await get_latest_pipeline_runs(
    config['stand']['services']['stand'],
    pipeline_ids=valid_schedule_pipelines.keys())

    
    print(len(valid_schedule_pipelines))
    print(len(active_pipeline_runs))
    update_pipeline_runs_commands = get_pipeline_run_commands(
        updated_pipelines= valid_schedule_pipelines,
        pipeline_runs=active_pipeline_runs,
        current_time=current_time,
    )
    
    
    for command in update_pipeline_runs_commands:
        await command.execute(config)
    # print(active_pipeline_runs)
    # # must be called again bc pipeline_runs_commands can create new runs
    # active_pipeline_runs = get_latest_pipeline_runs(
    #     config.get('services').get('stand'),
    #     pipeline_ids=updated_pipelines.keys())

    # for run in active_pipeline_runs:
    #     steps = [step for step in updated_pipelines[run["pipeline_id"]]["steps"]]

    #     trigger_commands = trigger_scheduled_pipeline_steps(
    #         pipeline_run=run, time=current_time, steps=steps
    #     )
    #     for command in trigger_commands:
    #         command.execute(config)
    #     # both the latest job and active step run needs to
    #     # be called here because trigger_commands can alter these,
    #     # so they cant be called up top for the whole batch of runs
    #     active_step_run = get_latest_pipeline_step_run(run=run)
    #     latest_job = get_latest_job_from_pipeline_step_run(
    #         config.get('services').get('stand'), step_run=active_step_run
    #     )

    #     # latest_job will always have a relationship to active_step_run
    #     propagate_commands = propagate_job_status(
    #         run=run, latest_job=latest_job, active_step_run=active_step_run
    #     )
    #     for command in propagate_commands:
    #         command.execute(config)

    # # not used
    # return [update_pipeline_runs_commands, trigger_commands, propagate_commands]

async def main(config):
    await execute(config=config, current_time=None)

if __name__ == "__main__":
    config = load_config()  
    print(config,"-----")
    asyncio.run(main(config))  