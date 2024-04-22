import asyncio
import os
import typing
from datetime import date, datetime, timedelta
from typing import List 
import requests
import yaml
from croniter import croniter
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import (
    AsyncEngine, AsyncSession, create_async_engine)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import and_

from stand.models import (Job, PipelineRun, PipelineStepRun, StatusExecution,Pipeline,PipelineStep,TriggerType)
from stand.scheduler.utils import *

def get_order_of_last_completed_step(steps:typing.List)-> int:
    """
    retuns the order of the last completed job associated steprun
    """
    pass
def step_has_associated_active_job():
    "name"
    pass

def trigger_scheduled_pipeline_steps(pipeline_run:PipelineRun, time:datetime):
    
    steps= get_pipeline_steps(pipeline_run)
    
    for step in steps:
        if(step.trigger_type == TriggerType.TIME_SCHEDULE):
            if(croniter.match(step.scheduling,time)): #time match
                
                next_step = get_order_of_last_completed_step(steps) +1
                if(step.order ==next_step): 
                    create_step_run(step)
                    return step
                #time match , order didnt
                else:
                    update_pipeline_run_status(pipeline_run,StatusExecution.PENDING)
                    return step
        
        if(step.trigger_type == "immediate"): # steps that occur exactly after the last one
            if(not step_has_associated_active_job()):
                next_step = get_order_of_last_completed_step(steps) +1
                if(next_step== step.order): #order match
                        create_step_run(step)
                        return step
                
            
    
    