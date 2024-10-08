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
import logging
from stand.models import db, Job, JobStep, JobStepLog, StatusExecution as EXEC, \
    JobResult

log = logging.getLogger(__name__)
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
        #default status should waiting
        status=StatusExecution.WAITING,
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
        pipeline_run: PipelineRun = step_run.pipeline_run
        # Job will be executed in a pipeline run context, define the
        # variable "ref" as the pipeline run start date.
        run_ref = {
            "name": "ref",
            "type": "DATE",
            "default_value": pipeline_run.start.date().isoformat(),
        }
        if not workflow.get("variables"):
            workflow["variables"] = [run_ref]
        else:
            workflow["variables"] = [
                v for v in workflow["variables"] if v.get("name") != "ref"
            ]
            workflow["variables"].append(run_ref)
        log.info(gettext('Set "ref" variable to {}').format(run_ref))

        job = Job(
                created=now,
                status=StatusExecution.WAITING,
                workflow_id=workflow.get('id'),
                workflow_name=workflow.get('name'),
                user_id=user.id,
                user_login=user.login,
                user_name=user.name,
                name=step_run.name,
                type=JobType.BATCH,
                pipeline_step_run_id=pipeline_step_run_id,
                pipeline_run_id=pipeline_run.id,
                description=gettext('[{} to {}] [{}-{}] Step: {}/{} ({})').format(
                    pipeline_run.start.strftime('%Y-%m-%d'),
                    pipeline_run.finish.strftime('%Y-%m-%d'),
                    pipeline_run.id,
                    pipeline_run.pipeline_name,
                    step_run.order,
                    len(pipeline_run.steps),
                    step_run.name
                )
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

def update_pipeline_run(job: Job) -> None:
    """ Update associated pipeline step run, if any """

    job.pipeline_step_run.status = job.status
    if job.status in (EXEC.ERROR, EXEC.CANCELED, EXEC.INTERRUPTED):
        job.pipeline_run.status = job.status
        job.pipeline_run.final_status = job.status
    elif job.status in (EXEC.COMPLETED, ):
        # Test if the step is the last one
        step_order = job.pipeline_step_run.order
        job.pipeline_run.last_executed_step = step_order
        if step_order == len(job.pipeline_run.steps):
            job.pipeline_run.status = EXEC.COMPLETED
        else:
            #job completed should make pipeline run go to waiting
            job.pipeline_run.status = EXEC.WAITING 

    elif job.status in (EXEC.PENDING, EXEC.WAITING,
                        EXEC.WAITING_INTERVENTION):
        pass # Ignore
    elif job.status in (EXEC.RUNNING, ):
        pass # FIXME

    db.session.add(job.pipeline_step_run)
    db.session.add(job.pipeline_run)

def change_pipeline_run_status(run: PipelineRun, status: StatusExecution,
                               emit: callable) -> None:
    run.status = status
    db.session.add(run)
    db.session.commit()
    if (emit):
        emit('update pipeline run',
             {'message': 'status', 'id': run.id, 'value': status},
             namespace='/stand',
             room='pipeline_runs')
