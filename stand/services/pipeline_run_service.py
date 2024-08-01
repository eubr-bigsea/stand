import json
import typing
from datetime import datetime

import pytz
import requests
from flask_babel import gettext

from stand.app_auth import User
from stand.models import Cluster, Job, JobType, PipelineRun, PipelineStepRun, StatusExecution, db
from stand.models_extra import Period, Pipeline, PipelineStep, Workflow
from stand.services import ServiceException
from stand.services.job_services import JobService

def get_resource_from_api(config: typing.Dict, resource_type: str,
                          resource_id: int) -> object:
    """Load a resource from Tahiti API"""
    url = f"{config.get('url').strip('/')}/{resource_type}s/{resource_id}"
    headers = {"X-Auth-Token": str(config.get("auth_token"))}

    try:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()  # Raise an exception for non-200 status codes

        if resource_type == "pipeline":
            data = resp.json().get("data")[0]
            return Pipeline(**data), data
        elif resource_type == "workflow":
            return resp.json(), resp.text
        else:
            raise ValueError(f"Invalid resource type: {resource_type}")
    except requests.exceptions.RequestException as e:
        raise ServiceException(
            f"Error retrieving {resource_type} {resource_id}: {str(e)}")

def get_pipeline_from_api(config: typing.Dict, pipeline_id: int) -> \
        typing.Tuple[Pipeline, typing.Dict]:
    return get_resource_from_api(config, "pipeline", pipeline_id)

def get_workflow_from_api(config: typing.Dict, workflow_id: int) -> \
        typing.Tuple[Workflow, typing.Dict]:
    return get_resource_from_api(config, "workflow", workflow_id)


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

def execute_pipeline_step_run(config: typing.Dict,
                              pipeline_step_run_id: int,
                              user: User) -> typing.Dict:
    step_run: PipelineStepRun = PipelineStepRun.query.get(pipeline_step_run_id)
    job = None
    if step_run is not None:
        now = datetime.utcnow()
        workflow, workflow_definition = get_workflow_from_api(config,
                                                       step_run.workflow_id)
        job = Job(
                created=now,
                status=StatusExecution.WAITING,
                workflow_id=workflow.get('id'),
                workflow_name=workflow.get('name'),
                user_id=user.id,
                user_login=user.login,
                user_name=user.name,
                name=gettext('{}-{} - Step {} {}').format(
                    step_run.pipeline_run.id,
                    step_run.pipeline_run.pipeline_name,
                    step_run.name,
                    step_run.pipeline_run.comment,
                ),
                type=JobType.BATCH,
                pipeline_step_run_id=pipeline_step_run_id
            )

        # FIXME
        cluster: Cluster = Cluster.query.get(workflow.get('preferred_cluster_id', 1))
        job.cluster_id = cluster.id

        job.workflow_definition = workflow_definition
        JobService.start(job, workflow, {}, JobType.BATCH, persist=True)

        step_run.status = StatusExecution.WAITING # Test if updates by hook
        db.session.add(job)
        db.session.add(step_run)
        db.session.commit()

    return step_run, job
