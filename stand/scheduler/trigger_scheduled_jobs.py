import asyncio
import os
import typing
from datetime import date, datetime, timedelta
from typing import List
import requests
import yaml
from croniter import croniter
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import and_
import json
from stand.models import (
    Job,
    PipelineRun,
    PipelineStepRun,
    StatusExecution,
    Pipeline,
    PipelineStep,
    TriggerType,
)
from stand.scheduler.utils import *


def is_next_step_in_order(step: PipelineStep, pipeline_run: PipelineRun) -> bool:
    "returns if step is the next step in a pipeline run execution"
    pass


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

    if parsed_scheduling["stepSchedule"]["frequency"] == "once":

        if start_time == current_time:
            return True
        else:
            return False

    if parsed_scheduling["stepSchedule"]["frequency"] == "daily":

        delta = abs(current_time - start_time)
        difference_in_minutes = delta.total_seconds() / 60

        frequency_in_days = parsed_scheduling["stepSchedule"]["intervalDays"]
        # diference in days  is divisible by frenquency in days , so match
        return difference_in_minutes % (int(frequency_in_days) * 1440) == 0

    if parsed_scheduling["stepSchedule"]["frequency"] == "monthly":

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
    start_datetime_obj = datetime.strptime(start_datetime_str, "%Y-%m-%dT%H:%M:%S")
    return start_datetime_obj


def trigger_scheduled_pipeline_steps(pipeline_run: PipelineRun, time: datetime):

    steps = get_pipeline_steps(pipeline_run)

    for step in steps:

        # time scheduled job
        if not get_step_is_immediate(step.scheduling):
            if is_next_step_in_order(step, pipeline_run):
                if time_match(step.scheduling, time):

                    # both time and order match
                    create_step_run(step)
                    return step
                else:
                    # only time match, execution out of order
                    update_pipeline_run_status(pipeline_run, StatusExecution.PENDING)

        # steps that occur imediatelly after the last one
        if get_step_is_immediate(step.scheduling):

            if is_next_step_in_order(step, pipeline_run):

                create_step_run(step)
                return step
