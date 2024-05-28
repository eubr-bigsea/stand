from datetime import date, datetime, timedelta
from typing import List

from croniter import croniter
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import and_

from stand.models import Job, PipelineRun, StatusExecution, Pipeline, PipelineStep

from stand.scheduler.utils import *


class Command:
    def execute(self):
        pass

    def __eq__(self, other):
        if isinstance(other, Command):
            return vars(self) == vars(other)
        return False


class CreatePipelineRun(Command):
    def __init__(self, pipeline):
        self.pipeline = pipeline

    def get_pipeline_run_start(
        self, current_time=datetime.now(), next_window_option=False
    ) -> datetime:
        frequency = self.pipeline["execution_window"]
        if frequency == "daily":
            next_datetime = current_time 
            return next_datetime.replace(hour=0, minute=0, second=0, microsecond=0)

        elif frequency == "weekly":
            days_since_sunday = (current_time.weekday() + 1) % 7
            if days_since_sunday == 0:
                days_since_sunday = 7
            previous_sunday_datetime = current_time - timedelta(days=days_since_sunday)
            return previous_sunday_datetime.replace(hour=0, minute=0, second=0, microsecond=0)


        elif frequency == "monthly":
            year = current_time.year
            month = current_time.month
           
            next_datetime = datetime(year, month, 1)
            return next_datetime.replace(hour=0, minute=0, second=0, microsecond=0)

    def get_pipeline_run_end(
        self, current_time=datetime.now(), next_window_option=False
    ) -> datetime:
        frequency = self.pipeline["execution_window"]
        if frequency == "daily":

            next_day = current_time + timedelta(days=1)
            return next_day.replace(
                hour=0, minute=0, second=0, microsecond=0
            ) - timedelta(seconds=1)
        elif frequency == "weekly":

            days_until_sunday = 6 - current_time.weekday()
            next_sunday = current_time + timedelta(days=days_until_sunday)
            return next_sunday.replace(
                hour=0, minute=0, second=0, microsecond=0
            ) - timedelta(seconds=1)
        elif frequency == "monthly":

            if current_time.month == 12:
                next_month_start = current_time.replace(
                    year=current_time.year + 1, month=1, day=1
                )
            else:
                next_month_start = current_time.replace(
                    month=current_time.month + 1, day=1
                )
            return next_month_start.replace(
                hour=0, minute=0, second=0, microsecond=0
            ) - timedelta(seconds=1)
    
    def create_step_run_from_json_step(self,step):
        pipeline_step_run =PipelineStepRun(
        status = StatusExecution.WAITING,
        created =self.get_pipeline_run_start(),
        pipeline_run_id =self.pipeline["id"],
        workflow_id = step["workflow"]["id"]
        )
        return pipeline_step_run
    
    async def execute(
        self, session: AsyncSession, user: typing.Dict, commit: bool = False
    ) -> PipelineRun:

        steps = []
        for step in self.pipeline["steps"]:
            steps.append(self.create_step_run_from_json_step(step))
            

        pipeline_run = PipelineRun(
            pipeline_id=self.pipeline["id"],
            last_completed_step =0,
            status=StatusExecution.WAITING,
            final_status=StatusExecution.WAITING,
            start=self.get_pipeline_run_start(),
            finish=self.get_pipeline_run_end(),
            steps=steps,
        )
        run: PipelineRun = pipeline_run
      
        if commit:
            session.add(run)
            await session.commit()
        return pipeline_run


class TriggerWorkflow(Command):
    def __init__(self, pipeline_step):

        self.pipeline_step = pipeline_step

    async def execute(self, session: AsyncSession):

        print("workflow was triggered, job created")


class UpdatePipelineRunStatus(Command):
    def __init__(self, pipeline_run, status):

        self.pipeline_run = pipeline_run
        self.status = status

    def execute(self, session):
        self.pipeline_run.status = self.status
        session.commit()


class UpdatePipelineStepRunStatus(Command):
    def __init__(self, pipeline_step_run, status):

        self.pipeline_step_run = pipeline_step_run
        self.status = status

    def execute(self, session):
        self.pipeline_step_run.status = self.status
        session.commit()


class ChangeLastCompletedStep(Command):
    def __init__(self, pipeline_run, new_last_completed_step):

        self.pipeline_run = pipeline_run
        self.new_last_completed_step = new_last_completed_step

    def execute(self, session):
        self.pipeline_run.last_executed_step = self.new_last_completed_step
        session.commit()


class UpdatePipelineInfo(Command):
    def __init__(self, pipeline_run, update_time):

        self.pipeline_run = pipeline_run
        self.update_time = update_time

    def execute(self, session):
        print("pipeline info updated")
