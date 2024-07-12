from gettext import gettext
from stand.models import (
    Job,
    PipelineRun,
    PipelineStepRun,
)
from stand.schema import (PipelineRunItemResponseSchema)

# Basic code to start a scheduler using aiocron package
import os
import typing
from datetime import date, timedelta
import aiohttp
import yaml
from sqlalchemy import select


# async def get_canceled_pipeline_runs(session)-> typing.List[PipelineRun]:
#     return await session.execute(
#         select(PipelineRun).filter(
#             PipelineRun.status == StatusExecution.CANCELED
#         )
#     ).fetchall()


async def get_latest_pipeline_step_run(run: PipelineRun) -> PipelineStepRun:
    return next(
        [step for step in run.steps if step.id == run.last_completed_step])


async def get_latest_pipeline_runs(stand_config: typing.Dict,
                   pipeline_ids: typing.List[int]) -> typing.List[PipelineRun]:
    """"""
    headers = {"X-Auth-Token": stand_config["auth_token"]}
    url = f"{stand_config['url']}/pipeline-runs"
    params = {
        'latest': 'true',
        'pipelines': ','.join([str(x) for x in pipeline_ids])
    }
    data = await retrieve_data(url, params=params, headers=headers)
    return PipelineRunItemResponseSchema(many=True, partial=True).load(
                data)



async def get_pipelines(
    tahiti_config: typing.Dict, days: int
) -> typing.Dict[int, typing.Dict]:
    """Read pipelines from Tahiti API.
    Don't need to read all pipelines, only those updated in the last window.
    """

    tahiti_api_url = tahiti_config["url"]
    reference = date.today() - timedelta(days=days)
    params = {
        "after": reference.isoformat(),
        "fields": 'id,name,enabled,steps,updated'
    }
    headers = {"X-Auth-Token": tahiti_config["auth_token"]}
    url = f"{tahiti_api_url}/pipelines"
    data = await retrieve_data(url, params, headers)
    return dict([[p["id"], p] for p in data["data"]])

async def get_pipeline_run(
    stand_config: typing.Dict, pipeline_run_id: int
) -> PipelineRun:
    """Read a single pipelines by id from API.
    """

    headers = {"X-Auth-Token": stand_config["auth_token"]}
    url = f"{stand_config['url']}/pipeline-runs/{pipeline_run_id}"
    data = await retrieve_data(url, headers=headers)
    return PipelineRunItemResponseSchema(partial=True).load(
                data.get('data')[0])


def load_config() -> typing.Dict[int, typing.Dict]:
    """ Load Stand configuration """
    config_file = os.environ.get("STAND_CONFIG")
    if not config_file:
        raise RuntimeError(gettext(
            "Required environment variable $STAND_CONFIG not set."))
    with open(config_file, "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    return config


# def get_latest_job(run: PipelineRun) -> Job:
#     "name"
#     pass


# def get_active_step_run(run: PipelineRun) -> Job:
#     "name"
#     pass


async def get_latest_job_from_pipeline_step_run(
    config, step_run: PipelineStepRun
) -> Job:
    # query = (
    #     select(Job, func.max(Job.finished).label("latest_job_finished_time"))
    #     .filter(Job.pipeline_step_run_id == step_run.id)
    #     .group_by(Job.pipeline_step_run_id)
    # )
    # query = (select(Job).filter(Job.pipeline_step_run_id == step_run.id)
    #          .order_by(Job.finished.desc()))

    # return await session.execute(query).one()
    return None


async def retrieve_data(url: str, params: typing.Dict = None,
                       headers: typing.Dict = None):
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(url, params=params) as resp:
            if resp.status != 200:
                raise RuntimeError(gettext(
                    "Error {} while getting pipeline runs").format(resp.status))
            return await resp.json()


async def update_data(url: str, method: str, payload: typing.Dict = None,
                       headers: typing.Dict = None):
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.request(method, url, json=payload) as resp:
            if resp.status != 200:
                raise RuntimeError(gettext(
                    "Error {} while getting pipeline runs").format(resp.status))
            return await resp.json()