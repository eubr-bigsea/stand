import json
import os
import typing
from datetime import date, timedelta
from gettext import gettext

import aiohttp
import yaml

from stand.models import (
    PipelineRun,
    PipelineStepRun,
    StatusExecution,
)
from stand.schema import PipelineRunItemResponseSchema


async def get_latest_pipeline_step_run(run: PipelineRun) -> PipelineStepRun:
    return next([step for step in run.steps if step.id == run.last_completed_step])


async def get_latest_pipeline_runs(
    stand_config: typing.Dict, pipeline_ids: typing.List[int],latest_only= True
) -> typing.List[PipelineRun]:
    if len(pipeline_ids) == 0:
        return []

    headers = {"X-Auth-Token": str(stand_config["auth_token"])}
    url = f"{stand_config['url']}/pipeline-runs"

    page = 1
    page_size = 20
    all_runs = []
    if not latest_only:
        while True:
            params = {
                "pipelines": ",".join([str(x) for x in pipeline_ids]),
                "page": page,
                "size": page_size,
            }

            data = await retrieve_data(url, params=params, headers=headers)

        
            page_data = data.get("data", [])
            if not page_data:
                break

        
            runs = PipelineRunItemResponseSchema(many=True, partial=True).load(page_data)
            all_runs.extend(runs)

            pagination = data.get("pagination", {})
            total_pages = pagination.get("pages", 1)

            if page >= total_pages:
                break
            page += 1

        return all_runs
    else:
        params = {
        "latest": "true",
        "pipelines": ",".join([str(x) for x in pipeline_ids]),
        }

        data = await retrieve_data(url, params=params, headers=headers)

        return PipelineRunItemResponseSchema(many=True, partial=True).load(data)


async def get_pipelines(
    tahiti_config: typing.Dict
) -> typing.Dict[int, typing.Dict]:
    """Read all pipelines from Tahiti API."""

    tahiti_api_url = tahiti_config["url"]

    params = {
        "fields": "id,name,enabled,steps,updated,run_creation_method",
        "page": 1,
        "size": 20,
    }
    headers = {"X-Auth-Token": str(tahiti_config["auth_token"])}
    url = f"{tahiti_api_url}/pipelines"
    all_pipelines = []
    while True:

        data = await retrieve_data(url, params, headers)
        pipelines = data.get("data", [])
        all_pipelines.extend(pipelines)

        pagination = data.get("pagination", {})
        current_page = pagination.get("page", 1)
        total_pages = pagination.get("pages", 1)

        if current_page >= total_pages:
            break

        params["page"] += 1

    return {p["id"]: p for p in all_pipelines}


async def get_pipeline_run(
    stand_config: typing.Dict, pipeline_run_id: int
) -> PipelineRun:
    """Read a single pipelines by id from API."""

    headers = {"X-Auth-Token": str(stand_config["auth_token"])}
    url = f"{stand_config['url']}/pipeline-runs/{pipeline_run_id}"
    data = await retrieve_data(url, headers=headers)
    return PipelineRunItemResponseSchema(partial=True).load(data.get("data")[0])


def load_config() -> typing.Dict[int, typing.Dict]:
    """Load Stand configuration"""
    config_file = os.environ.get("STAND_CONFIG")
    if not config_file:
        raise RuntimeError(
            gettext("Required environment variable $STAND_CONFIG not set.")
        )
    with open(config_file, "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    return config


async def retrieve_data(
    url: str, params: typing.Dict = None, headers: typing.Dict = None
):
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(url, params=params) as resp:
            if resp.status != 200:
                raise RuntimeError(
                    gettext("Error {} while getting pipeline runs").format(resp.status)
                )
            return await resp.json()


async def update_data(
    url: str,
    method: str,
    payload: typing.Dict = None,
    headers: typing.Dict = None,
):
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.request(method, url, json=payload) as resp:
            if resp.status != 200:
                raise RuntimeError(
                    gettext("Error {} while updating pipeline").format(resp)
                )
            return await resp.json()


def pipeline_steps_have_valid_schedulings(steps: typing.List):
    # no steps, no need to create a pipeline_run
    if len(steps) == 0:
        return False

    for step in steps:
        if "scheduling" in step and "workflow" in step:
            schedule = json.loads(step["scheduling"])
            schedule = schedule["stepSchedule"]
          
            if (
                schedule["executeImmediately"] == "true"
                or (schedule["startDateTime"] != "null"
               
                and schedule["months"]!=[]) 
            ):
                pass
            
            else:
                return False
        else:
            return False

    return True
