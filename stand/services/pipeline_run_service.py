import typing
import json
from datetime import datetime

import pytz
import requests
from flask_babel import gettext

from stand.app_auth import User
from stand.models import Cluster, Job, JobType, PipelineRun, PipelineRunContextData, PipelineStepRun, StatusExecution, db
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
        typing.Tuple[typing.Dict, typing.Dict]:
    return get_resource_from_api(config, "workflow", workflow_id)


def create_pipeline_run_from_pipeline(
    pipeline: Pipeline, period: Period, run_creation_method="scheduler",
    context=None,
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
        scheduling = json.loads(st.scheduling)
        return PipelineStepRun(
            name=st.name,
            created=now,
            updated=now,
            workflow_id=st.workflow.id,
            retries=0,
            order=st.order,
            trigger_mode=scheduling.get('stepSchedule', {}).get('frequency', 'manual'),
            comment=None,
            status=StatusExecution.PENDING,
            final_status=None,

        )

    start = period.start.astimezone(pytz.UTC)
    finish = period.finish.astimezone(pytz.UTC)
    if context is None or len(context) == 0:
        context_data=[]
    else:
        context_data = [
            PipelineRunContextData(name=ctx.get('name'), value=ctx.get('value'))
            for ctx in context
        ]
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
        run_creation_method = run_creation_method,
        context_data=context_data
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
            variables = [run_ref]
        else:
            variables = [
                v for v in workflow["variables"] if v.get("name") != "ref"
            ]
            variables.append(run_ref)
        log.info(gettext('Set "ref" variable to {}').format(run_ref))

        run_id = {
            "name": "pipeline_run_id",
            "type": "INT",
            "default_value": pipeline_run.id,
        }
        step_id = {
            "name": "pipeline_run_step_id",
            "type": "INT",
            "default_value": pipeline_step_run_id,
        }
        variables.append(run_id)
        variables.append(step_id)

        # Set the pipeline context as workflow variables
        for ctx in pipeline_run.context_data:
            variables.append({
                "name":ctx.name,
                "value": ctx.value,
                "type": "STRING"
            }
            )

        workflow["variables"] = variables
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

    step_order = job.pipeline_step_run.order
    if job.status == EXEC.COMPLETED and job.pipeline_run.last_executed_step < step_order:
        job.pipeline_run.last_executed_step = step_order

    last_step = job.pipeline_run.last_executed_step

 
    step_statuses = {
        step.order: (job.status if step.id == job.pipeline_step_run.id else step.status)
        for step in job.pipeline_run.steps
    }

    all_statuses = list(step_statuses.values())

    all_prior_completed = all(
        step_statuses[o] == EXEC.COMPLETED
        for o in step_statuses
        if o < last_step
    )
    all_after_pending = all(
        step_statuses[o] == EXEC.PENDING
        for o in step_statuses
        if o >= last_step
    )

    if EXEC.RUNNING in all_statuses:
        job.pipeline_run.status = EXEC.RUNNING
    elif EXEC.ERROR in all_statuses:
        job.pipeline_run.status = EXEC.ERROR
    elif all_prior_completed and all_after_pending:
        job.pipeline_run.status = EXEC.WAITING
    elif all(status == EXEC.COMPLETED for status in all_statuses):
        job.pipeline_run.status = EXEC.COMPLETED
    else:
        job.pipeline_run.status = EXEC.PENDING

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
