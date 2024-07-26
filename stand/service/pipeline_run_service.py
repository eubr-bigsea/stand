import typing
from datetime import datetime

import pytz
import requests
from flask_babel import gettext

from stand.models import PipelineRun, PipelineStepRun, StatusExecution, db
from stand.models_extra import Period, Pipeline, PipelineStep
from stand.service import ServiceException


def get_pipeline_from_api(config: typing.Dict, pipeline_id: int) -> Pipeline:
    """Load a pipeline from Tahiti API"""
    try:
        url = f"{config.get('url').strip('/')}/pipelines/{pipeline_id}"
        headers = {"X-Auth-Token": str(config.get("auth_token"))}
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            p = resp.json().get("data")[0]
            return Pipeline(**p)
        else:
            raise ServiceException(
                gettext("Error retrieving pipeline {}".format(pipeline_id))
            )
    except Exception as ex:
        raise ex


def create_pipeline_run_from_pipeline(
    pipeline: Pipeline, period: Period
) -> None:
    """Create a pipeline run from a pipeline"""
    now = datetime.utcnow()

    def create_step(st: PipelineStep):
        if st.workflow is None:
            raise ServiceException(
                gettext(
                    "At least a pipeline step is not associated to a workflow"
                )
            )
        return PipelineStepRun(
            name=st.name,
            created=now,
            updated=now,
            workflow_id=st.workflow.id,
            retries=0,
            order=st.order,
            comment=None,
            status=StatusExecution.PENDING,
            final_status=None,
        )

    start = period.start.astimezone(pytz.UTC)
    finish = period.finish.astimezone(pytz.UTC)
    pipeline_run = PipelineRun(
        start=start,
        finish=finish,
        pipeline_id=pipeline.id,
        pipeline_name=pipeline.name,
        last_executed_step=0,
        comment=f'{gettext("Execution")} - '
        f'[{start.strftime("%d-%m-%Y")} '
        f'/ {finish.strftime("%d-%m-%Y")}]',
        updated=now,
        status=StatusExecution.PENDING,
        final_status=None,
        steps=[create_step(st) for st in pipeline.steps],
    )
    db.session.add(pipeline_run)
    db.session.commit()
    return pipeline_run
