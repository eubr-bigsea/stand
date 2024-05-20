from datetime import date, datetime, timedelta
from typing import List

from croniter import croniter
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import and_

from stand.models import Job, PipelineRun, StatusExecution, Pipeline, PipelineStep

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
        self.pipeline = pipeline

    def get_pipeline_run_start(
        self, current_datetime: datetime, next_window_option=False
    ) -> bool:
        frequency = self.pipeline["execution_window"]
        if frequency == "daily":
            next_datetime = current_datetime + timedelta(days=1)
            return next_datetime.replace(hour=0, minute=0, second=0, microsecond=0)

        elif frequency == "weekly":
            days_until_sunday = (6 - current_datetime.weekday()) % 7
            if days_until_sunday == 0:
                days_until_sunday = 7
            next_datetime = current_datetime + timedelta(days=days_until_sunday)
            return next_datetime.replace(hour=0, minute=0, second=0, microsecond=0)

        elif frequency == "monthly":
            year = current_datetime.year
            month = current_datetime.month
            if month == 12:
                next_month = 1
                year += 1
            else:
                next_month = month + 1
            next_datetime = datetime(year, next_month, 1)
            return next_datetime.replace(hour=0, minute=0, second=0, microsecond=0)

    def get_pipeline_run_end(self, current_time, next_window_option=False):
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

    async def execute(
        self, session: AsyncSession, user: typing.Dict, commit: bool = False
    ) -> PipelineRun:

        steps = []
        for step in self.pipeline["steps"]:
            steps.append(PipelineStepRun(step))

        run: PipelineRun = PipelineRun(
            pipeline_id=self.pipeline["id"],
            updated=self.pipeline["updated"],
            user=user,
            status=StatusExecution.WAITING,
            final_status=StatusExecution.WAITING,
            start=self.get_pipeline_run_start(),
            finish=self.get_pipeline_run_end(),
            steps=steps,
        )
        session.add(run)
        if commit:
            await session.commit()
        return run


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
        self.pipeline_run.last_completed_step = self.new_last_completed_step
        session.commit()


class UpdatePipelineInfo(Command):
    def __init__(self, pipeline_run, update_time):

        self.pipeline_run = pipeline_run
        self.update_time = update_time

    def execute(self, session):
        print("pipeline info updated")
