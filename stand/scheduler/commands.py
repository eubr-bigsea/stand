import logging
import logging.config
from calendar import monthrange
from datetime import datetime, time, timedelta

from stand.models import PipelineRun
from stand.scheduler.utils import update_data

logging.config.fileConfig('logging_config.ini')
logger = logging.getLogger(__name__)


class Command:
    def execute(self, config):
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
        # hard coded for testing
        # frequency = self.pipeline["execution_window"]
        frequency = "monthly"
        return getattr(self, "_get_limit_dates_" + frequency)(current_time)[0]

    def get_pipeline_run_end(
        self, current_time=datetime.now(), next_window_option=False
    ) -> datetime:
        # hard coded for testing
        # frequency = self.pipeline["execution_window"]
        frequency = "monthly"
        return getattr(self, "_get_limit_dates_" + frequency)(current_time)[1]

    async def execute(self, config) -> PipelineRun:
        stand_config = config.get("stand").get("services").get("stand")
        headers = {"X-Auth-Token": str(stand_config["auth_token"])}
        url = f"{stand_config['url']}/pipeline-runs/create"
        payload = {
            "id": self.pipeline["id"],
            "start": str(self.get_pipeline_run_start()),
            "finish": str(self.get_pipeline_run_end()),
        }
        await update_data(
            url=url, method="POST", payload=payload, headers=headers
        )
        logger.info("Created run for pipeline %s", self.pipeline["id"])
        return self.pipeline


class TriggerWorkflow(Command):
    def __init__(self, pipeline_step):
        self.pipeline_step = pipeline_step

    async def execute(self, config):
        stand_config = config.get("stand").get("services").get("stand")
        headers = {"X-Auth-Token": str(stand_config["auth_token"])}

        url = f"{stand_config['url']}/pipeline-runs/execute"
        payload = {"id": self.pipeline_step.id}

        logger.info("PipelineStep with id %s triggered.", self.pipeline_step)
        await update_data(
            url=url, method="POST", payload=payload, headers=headers
        )

        return self.pipeline_step.id


class UpdatePipelineRunStatus(Command):
    def __init__(self, pipeline_run, status):
        self.pipeline_run = pipeline_run
        self.new_status = status

    async def execute(self, config):
        stand_config = config.get("stand").get("services").get("stand")
        headers = {"X-Auth-Token": str(stand_config["auth_token"])}

        url = f"{stand_config['url']}/pipeline-runs/{self.pipeline_run.id}"
        payload = {"status": self.new_status}

        await update_data(
            url=url, method="PATCH", payload=payload, headers=headers
        )
        logger.info(
            "Status of run %s changed to %s",
            self.pipeline_run.id,
            self.new_status,
        )
        return self.pipeline_run.id


class UpdatePipelineInfo(Command):
    def __init__(self, pipeline_run, update_time):
        self.pipeline_run = pipeline_run
        self.update_time = update_time

    # TODO
    async def execute(self, config):
        logger.info("Pipeline info updated.")
