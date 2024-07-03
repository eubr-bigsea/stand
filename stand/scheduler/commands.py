from datetime import datetime, timedelta, time
from calendar import monthrange
import typing

from sqlalchemy.ext.asyncio import AsyncSession

from stand.models import PipelineRun, StatusExecution

from stand.scheduler.utils import PipelineStepRun


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

    @staticmethod
    def _get_limit_dates_daily(current_time):
        return datetime.combine(current_time, time.min), datetime.combine(
            current_time, time.max
        )

    @staticmethod
    def _get_limit_dates_weekly(current_time):
        days_since_sunday = (current_time.weekday() + 1) % 7
        last_sunday = current_time + timedelta(days=-days_since_sunday)

        days_until_saturday = (
            (5 - current_time.weekday()) if days_since_sunday != 0 else 6
        )
        next_saturday = current_time + timedelta(days=days_until_saturday)

        return datetime.combine(last_sunday, time.min), datetime.combine(
            next_saturday, time.max
        )

    @staticmethod
    def _get_limit_dates_monthly(current_time):
        first_day_month = current_time.replace(day=1)

        _, last_day_month = monthrange(current_time.year, current_time.month)
        last_day_month = current_time.replace(day=last_day_month)

        return datetime.combine(first_day_month, time.min), datetime.combine(
            last_day_month, time.max
        )

    def get_pipeline_run_start(
        self, current_time=datetime.now(), next_window_option=False
    ) -> datetime:
        frequency = self.pipeline["execution_window"]
        return getattr(self, "_get_limit_dates_" + frequency)(current_time)[0]

    def get_pipeline_run_end(
        self, current_time=datetime.now(), next_window_option=False
    ) -> datetime:
        frequency = self.pipeline["execution_window"]
        return getattr(self, "_get_limit_dates_" + frequency)(current_time)[1]

    def create_step_run_from_json_step(self, step):
        pipeline_step_run = PipelineStepRun(
            status=StatusExecution.WAITING,
            created=self.get_pipeline_run_start(),
            pipeline_run_id=self.pipeline["id"],
            workflow_id=step["workflow"]["id"],
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
            last_executed_step=0,
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
