
from datetime import date, datetime, timedelta
from typing import List 

from croniter import croniter
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import (
    AsyncEngine, AsyncSession, create_async_engine)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import and_

from stand.models import (Job, PipelineRun, StatusExecution,Pipeline,PipelineStep)

from stand.scheduler.utils import *
from stand.scheduler.status_control import *
from stand.scheduler.trigger_scheduled_jobs import *
from stand.scheduler.update_pipeline_runs import *

class Command:
    def execute(self):
        pass
    def __eq__(self, other):
        if isinstance(other, Command):
            return vars(self) == vars(other)
        return False
    
    
class CreatePipelineRun(Command):
    def __init__(self, pipeline):
        self.pipeline =pipeline

    def execute(self,session):
        print("pipeline created")
        
class CreatePipelineStepRun(Command):
    def __init__(self, pipeline_step):
        
        self.pipeline_step =pipeline_step

    def execute(self,session):
        print("pipeline step created")
    
class UpdatePipelineRunStatus(Command):
    def __init__(self, pipeline_run,status):
        
        self.pipeline_run =pipeline_run
        self.status =status

    def execute(self,session):
        print("pipeline run status updated")

class UpdatePipelineStepRunStatus(Command):
    def __init__(self, pipeline_step_run,status):
        
        self.pipeline_step_run =pipeline_step_run
        self.status =status

    def execute(self,session):
        print("pipeline step run status updated")
        
class ChangeLastCompletedStep(Command):
    def __init__(self, pipeline_run,new_last_completed_step):
        
        self.pipeline_run =pipeline_run
        self.new_last_completed_step =new_last_completed_step

    def execute(self,session):
        print("pipeline last completed step changed")
        
        
class UpdatePipelineInfo(Command):
    def __init__(self, pipeline_run,update_time):
        
        self.pipeline_run =pipeline_run
        self.update_time =update_time

    def execute(self,session):
        print("pipeline info updated")