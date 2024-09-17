import typing
from datetime import date, datetime
import json
from stand.models import (
    PipelineRun,
    StatusExecution,
    PipelineStepRun,
    
)
from stand.scheduler.commands import (TriggerWorkflow,UpdatePipelineRunStatus)


def is_next_step_in_order(step: PipelineStepRun, pipeline_run: PipelineRun) -> bool:
    "returns if step is the next step in a pipeline run execution"
    if step["order"] == pipeline_run.last_executed_step + 1:
        return True
    else:
        return False


def step_has_associated_active_job():
    "name"
    pass


def time_match(scheduling, current_time: datetime) -> bool:
    """
    returns if datetime matchs with scheduling json format from a pipeline_step
    to the minute
    """
    parsed_scheduling = json.loads(scheduling)

    start_time = get_step_start_time(scheduling)
  
    # prevents matches before start_time
    if start_time > current_time:
        return False
   
                       
    function_dict = {
        "once": once_schedule,
        "daily": daily_schedule,
        "monthly": monthly_schedule,
    }
    frequency = parsed_scheduling["stepSchedule"]["frequency"]
    if frequency in function_dict:
        return function_dict[frequency](scheduling, current_time)
    else:
        return False


def once_schedule(scheduling, current_time: date) -> bool:
    start_time = get_step_start_time(scheduling)
    if start_time == current_time:
        return True
    else:
        return False


def daily_schedule(scheduling, current_time: datetime) -> bool:
    parsed_scheduling = json.loads(scheduling)
    start_time = get_step_start_time(scheduling)
    delta = abs(current_time - start_time)
    difference_in_minutes = delta.total_seconds() / 60
    frequency_in_days = parsed_scheduling["stepSchedule"]["intervalDays"]
    # diference  is divisible by frenquency , so match
    return difference_in_minutes % (int(frequency_in_days) * 1440) == 0


def monthly_schedule(scheduling, current_time: datetime) -> bool:
    
    parsed_scheduling = json.loads(scheduling)
    start_time = get_step_start_time(scheduling)
    current_month = str(current_time.month)
    current_day = str(current_time.day)
    current_hour = current_time.hour
    currrent_minute = current_time.minute
    scheduled_months = parsed_scheduling["stepSchedule"]["months"]
    scheduled_days = parsed_scheduling["stepSchedule"]["days"]
    scheduled_hour = start_time.hour
    scheduled_minute = start_time.minute


    if scheduled_hour == current_hour and scheduled_minute == currrent_minute:
        if current_month in scheduled_months and current_day in scheduled_days:
        
            return True
        
    return False


def get_step_is_immediate(scheduling) -> bool:
    parsed_scheduling = json.loads(scheduling)
    return parsed_scheduling["stepSchedule"]["executeImmediately"]


def get_step_start_time(scheduling) -> datetime:
   
    parsed_scheduling = json.loads(scheduling)
    start_datetime_str = parsed_scheduling["stepSchedule"]["startDateTime"]
    # print(start_datetime_str)
    start_datetime_obj = datetime.strptime(
        #FIXME
        start_datetime_str[:16], "%Y-%m-%dT%H:%M"
    )
    # print(start_datetime_obj)
    return start_datetime_obj


def trigger_scheduled_pipeline_steps(
    pipeline_run: PipelineRun, time: datetime, steps: typing.List,
    step_runs:typing.List
):
    for index,step in enumerate(steps):
  
        if not get_step_is_immediate(step["scheduling"]):
     
            if is_next_step_in_order(step, pipeline_run):
        
                if time_match(step["scheduling"], time):
                    if step["id"]==347:
                        print(step["scheduling"],time)
                    command = TriggerWorkflow(pipeline_step=step_runs[index])
                    return command
                else:
                    #only time match, execution out of order

                    command = UpdatePipelineRunStatus(
                        pipeline_run=pipeline_run, status=StatusExecution.PENDING
                    )
                    return command

        # steps that occur imediatelly after the last one
        if get_step_is_immediate(step["scheduling"]):
            if is_next_step_in_order(step, pipeline_run):
                command = TriggerWorkflow(pipeline_step=step)
                return command
    

