
from collections import KeysView
from datetime import date, datetime, timedelta

import pytest
from mock import ANY, patch,MagicMock
from mock.mock import AsyncMock

from stand.models import PipelineRun, StatusExecution ,PipelineStep
from stand.scheduler.utils import *
from stand.scheduler.trigger_scheduled_jobs  import *


@pytest.mark.parametrize("current_time, expected_result", [
    (datetime(year=2024, month=4, day=24, hour=11, minute=0, second=0), True),
    (datetime(year=2024, month=5, day=2, hour=11, minute=1, second=0), False)   
])
def test_time_match_once(current_time, expected_result):
    """
    tests if steps with "repeat once" option are matched correctly
    """
    #start time = 2024-04-24 , 11:00:00
    scheduling = "{\"stepSchedule\":{\"executeImmediately\":false,\"frequency\":\"once\",\
    \"startDateTime\":\"2024-04-24T11:00:00\",\"intervalDays\":null,\"intervalWeeks\":null,\
    \"weekDays\":[],\"months\":[],\"days\":[]}}"
   
    assert time_match(scheduling, current_time) ==  expected_result

@pytest.mark.parametrize("current_time, expected_result", [
    (datetime(year=2024, month=4, day=24, hour=11, minute=0, second=0), True),
    (datetime(year=2024, month=4, day=28, hour=11, minute=0, second=0), True),  
    (datetime(year=2024, month=5, day=2, hour=11, minute=0, second=0), True),   
    (datetime(year=2024, month=4, day=20, hour=11, minute=0, second=0), False),
    (datetime(year=2024, month=5, day=2, hour=12, minute=0, second=0), False),   
    (datetime(year=2024, month=5, day=2, hour=11, minute=1, second=0), False)   
])
def test_time_match_daily(current_time, expected_result):
    """
    tests if steps with interval in days are matched correctly
    """
    #start time = 2024-04-24 , 11:00:00
    scheduling ="{\"stepSchedule\":{\"executeImmediately\":false,\"frequency\":\"daily\",\
    \"startDateTime\":\"2024-04-24T11:00:00\",\"intervalDays\":\"4\",\"intervalWeeks\":null,\
    \"weekDays\":[],\"months\":[],\"days\":[]}}"

    assert time_match(scheduling, current_time) ==  expected_result


@pytest.mark.parametrize("current_time, expected_result", [
    (datetime(year=2024, month=3, day=24, hour=11, minute=0, second=0), False),
    (datetime(year=2024, month=4, day=24, hour=11, minute=0, second=0), True),  
    (datetime(year=2024, month=5, day=24, hour=11, minute=0, second=0), True),   
    (datetime(year=2024, month=6, day=24, hour=12, minute=0, second=0), False),
    (datetime(year=2024, month=7, day=24, hour=11, minute=1, second=0), False),   
    (datetime(year=2024, month=8, day=23, hour=11, minute=0, second=0), False)   
])
def test_time_match_monthly(current_time, expected_result):
    """
    tests if steps with interval in months are matched correctly
    """
      #start time = 2024-04-24 , 11:00:00
    scheduling ="{\"stepSchedule\":{\"executeImmediately\":false,\"frequency\":\"monthly\",\
    \"startDateTime\":\"2024-04-24T11:00:00\",\"intervalDays\":null,\"intervalWeeks\":null,\
    \"weekDays\":[],\"months\":[\"1\",\"2\",\"3\",\"4\",\"5\",\"6\",\"7\",\"8\",\"9\",\"10\",\"11\",\"12\"],\
    \"days\":[\"24\"]}}"

    assert time_match(scheduling, current_time) ==  expected_result


@pytest.mark.parametrize("execute_immediately,expected_result", [("true",True), ("false",False)])
def test_immediate_step_detected_correctly(execute_immediately,expected_result):
    """
    tests if  the trigger type "immediate" is detected correctly on
    a json scheduling
    """
   
    scheduling= "{\"stepSchedule\":{\"executeImmediately\":"+ execute_immediately+\
    ",\"frequency\":\"once\",\
    \"startDateTime\":\"2024-04-24T11:00:00\",\"intervalDays\":null,\
    \"intervalWeeks\":null,\"weekDays\":[],\"months\":[],\"days\":[]}}"
    
    assert get_step_is_immediate(scheduling) == expected_result

def test_get_start_time():
    
    """
    tests if  getter method works for start time
    """
    scheduling_data = {
        "stepSchedule": {
            "executeImmediately": None,
            "frequency": None,
            "startDateTime": "2024-04-24T11:00:00",
            "intervalDays": None,
            "intervalWeeks": None,
            "weekDays": [],
            "months": [],
            "days": []
        }
    }
    scheduling = json.dumps(scheduling_data)
    datetime_start_time = datetime(year=2024, month=4, day=24, hour=11, minute=0, second=0)
    assert get_step_start_time(scheduling) == datetime_start_time

# @pytest.fixture
# def mocked_functions():
#     with patch("stand.scheduler.trigger_scheduled_jobs.get_pipeline_steps") as mocked_get_pipeline_steps, \
#          patch("stand.scheduler.trigger_scheduled_jobs.get_order_of_last_completed_step") as mocked_get_order_of_last_completed_step, \
#          patch("stand.scheduler.trigger_scheduled_jobs.create_step_run") as mocked_create_step_run, \
#          patch("stand.scheduler.trigger_scheduled_jobs.update_pipeline_step_run_status") as mocked_update_pipeline_step_run_status,\
#          patch("stand.scheduler.trigger_scheduled_jobs.update_pipeline_run_status") as mocked_update_pipeline_run_status,\
#          patch("stand.scheduler.trigger_scheduled_jobs.step_has_associated_active_job") as mocked_step_has_associated_active_job:
        
#         yield (mocked_get_pipeline_steps,
#                mocked_get_order_of_last_completed_step,
#                mocked_create_step_run,
#                mocked_update_pipeline_step_run_status,
#                mocked_update_pipeline_run_status,
#                mocked_step_has_associated_active_job
#               )
        
  
# def test_time_scheduled_job_is_triggered(mocked_functions):
#     """
#     tests if  a time scheduled job is triggered correctly when its
#     order is correct and theres a time match
#     """
    
#     mocked_get_pipeline_steps,\
#     mocked_get_order_of_last_completed_step,\
#     mocked_create_step_run,\
#     mocked_update_pipeline_step_run_status,\
#     mocked_update_pipeline_run_status,\
#     mocked_step_has_associated_active_job = mocked_functions
    
    
#     steps =[PipelineStep(id="1",order=1, 
#                         trigger_type= TriggerType.TIME_SCHEDULE,
#                         scheduling = "0 17 15 * *" ## trigger 17:00, day 15 , every month 
#                         )]
    
#     pipeline_run = PipelineRun(id= "1",
#                                status= StatusExecution.WAITING)
    
#     mocked_get_pipeline_steps.return_value =steps
#     mocked_get_order_of_last_completed_step.return_value =0
    
#     time = datetime(hour=17,day=15, month =5, year=2024)
#     trigger_scheduled_pipeline_steps(pipeline_run,time)
#     #order and time matched , so step run should be created
#     mocked_create_step_run.assert_called_once()
    
    
# def test_time_scheduled_job_isnt_triggered_out_of_order(mocked_functions):
#     """
#     tests if a run has its status changed to pending
#     when a pipeline step has a time match but its out of order 
#     in the execution sequence
#     """
    
#     mocked_get_pipeline_steps,\
#     mocked_get_order_of_last_completed_step,\
#     mocked_create_step_run,\
#     mocked_update_pipeline_step_run_status ,\
#     mocked_update_pipeline_run_status,\
#       mocked_step_has_associated_active_job = mocked_functions
  
    
#     steps =[PipelineStep(id="1",order=1, 
#                         trigger_type= TriggerType.TIME_SCHEDULE,
#                         scheduling = "0 17 13 * *" ## trigger 17:00, day 13 , every month,
#             ),
#             PipelineStep(id="1",order=2, 
#                         trigger_type= TriggerType.TIME_SCHEDULE,
#                         scheduling = "0 17 15 * *" 
#             ),
#             PipelineStep(id="1",order=3, 
#                         trigger_type= TriggerType.TIME_SCHEDULE,
#                         scheduling = "0 17 17 * *" 
#                         )]
    
#     pipeline_run = PipelineRun(id= "1",
#                                status= StatusExecution.WAITING)
    
#     mocked_get_pipeline_steps.return_value =steps
#     #no step was yet completed
#     mocked_get_order_of_last_completed_step.return_value =0
#     #time to match step 2
#     time = datetime(hour=17,day=15, month =5, year=2024)
    
#     trigger_scheduled_pipeline_steps(pipeline_run,time)
#     #time matched , but order didnt, pipeline_run goes to pending
#     mocked_update_pipeline_run_status.assert_called_once_with(pipeline_run,StatusExecution.PENDING)
    
    
# def test_imediate_job_is_triggered(mocked_functions):
#     """
#     tests if a job with the "exeucte imediately after the step before"
#     option is triggered correctly
#     """
    
#     mocked_get_pipeline_steps,\
#     mocked_get_order_of_last_completed_step,\
#     mocked_create_step_run,\
#     mocked_update_pipeline_step_run_status ,\
#     mocked_update_pipeline_run_status,\
#       mocked_step_has_associated_active_job = mocked_functions
    
   
    
#     steps =[PipelineStep(id="1",order=1, 
#                         trigger_type= TriggerType.TIME_SCHEDULE,
#                         scheduling = "0 17 13 * *" ## trigger 17:00, day 13 , every month,
#             ),
#             PipelineStep(id="1",order=2, 
#                         trigger_type= TriggerType.TIME_SCHEDULE,
#                         scheduling = "0 17 15 * *" 
#             ),
#             PipelineStep(id="1",order=3, 
#                         trigger_type= "immediate", # fix , decide the TriggerType 
#                         )]
    
#     pipeline_run = PipelineRun(id= "1",
#                                status= StatusExecution.WAITING)
    
#     mocked_get_pipeline_steps.return_value =steps
#     #no associated active job with the immediate step
#     mocked_step_has_associated_active_job.return_value =False
#     #step 2 was completed
#     mocked_get_order_of_last_completed_step.return_value =2
#     time = datetime(hour=18,day=15, month =5, year=2024)
    
#     trigger_scheduled_pipeline_steps(pipeline_run,time)
    
#     #order match , immediate type job dont care abut time, so step_run should be created
#     mocked_create_step_run.assert_called_once_with(steps[2])

# def test_imediate_job_isnt_triggered_out_of_order(mocked_functions):
#     """
#     tests if a job with the "execute imediately after the step before"
#     option is not trigerred if the previous step isnt completed
#     """
    
#     mocked_get_pipeline_steps,\
#     mocked_get_order_of_last_completed_step,\
#     mocked_create_step_run,\
#     mocked_update_pipeline_step_run_status ,\
#     mocked_update_pipeline_run_status,\
#       mocked_step_has_associated_active_job = mocked_functions
    
   
    
#     steps =[PipelineStep(id="1",order=1, 
#                         trigger_type= TriggerType.TIME_SCHEDULE,
#                         scheduling = "0 17 13 * *" ## trigger 17:00, day 13 , every month,
#             ),
#             PipelineStep(id="1",order=2, 
#                         trigger_type= TriggerType.TIME_SCHEDULE,
#                         scheduling = "0 17 15 * *" 
#             ),
#             PipelineStep(id="1",order=3, 
#                         trigger_type= "immediate", # fix , decide the TriggerType 
#                         )]
    
#     pipeline_run = PipelineRun(id= "1",
#                                status= StatusExecution.WAITING)
    
#     mocked_get_pipeline_steps.return_value =steps
#     mocked_step_has_associated_active_job.return_value =False
#     #step 1 was completed
#     mocked_get_order_of_last_completed_step.return_value =1
#     time = datetime(hour=18,day=15, month =5, year=2024)
    
#     trigger_scheduled_pipeline_steps(pipeline_run,time)
#     #order wasnt match, so step should be ignored
#     mocked_create_step_run.assert_not_called()

# def test_imediate_job_isnt_triggered_twice(mocked_functions):
#     """
#     tests if a job with the "execute imediately after the step before"
#     option is not trigerred if the pipeline_step already have an active job
#     """
    
#     mocked_get_pipeline_steps,\
#     mocked_get_order_of_last_completed_step,\
#     mocked_create_step_run,\
#     mocked_update_pipeline_step_run_status ,\
#     mocked_update_pipeline_run_status,\
#       mocked_step_has_associated_active_job = mocked_functions
    
#     latest_job =Job(id=1, status = StatusExecution.RUNNING)
    
#     steps =[PipelineStep(id="1",order=1, 
#                         trigger_type= TriggerType.TIME_SCHEDULE,
#                         scheduling = "0 17 13 * *" ## trigger 17:00, day 13 , every month,
#             ),
#             PipelineStep(id="1",order=2, 
#                         trigger_type= TriggerType.TIME_SCHEDULE,
#                         scheduling = "0 17 15 * *" 
#             ),
#             PipelineStep(id="1",order=3, 
#                         trigger_type= "immediate", # fix , decide the TriggerType 
#                         )]
    
#     pipeline_run = PipelineRun(id= "1",
#                                status= StatusExecution.WAITING)
    
#     mocked_get_pipeline_steps.return_value =steps
#     #the immediate step already have an associated job
#     mocked_step_has_associated_active_job.return_value =True
#     #step 1 was completed
#     mocked_get_order_of_last_completed_step.return_value =2
#     time = datetime(hour=18,day=15, month =5, year=2024)
    
#     trigger_scheduled_pipeline_steps(pipeline_run,time)
#     # there was already an associated job with the step, so step_run shouldt be created
#     mocked_create_step_run.assert_not_called()