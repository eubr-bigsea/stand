# -*- coding: utf-8 -*-} import logging
import math
import requests
import json
import datetime
import rq
import logging

from flask import g as flask_global
from flask import request, current_app
from flask_babel import gettext
from flask_restful import Resource
from marshmallow import ValidationError
from sqlalchemy import and_
from stand.app_auth import requires_auth
from stand.schema import (Job, JobCreateRequestSchema, JobItemResponseSchema,
                          JobListResponseSchema, JobStep, ExecutionPermission,
                          PermissionType, Cluster, translate_validation,
                          JobException, db)
from stand.models import JobType, StatusExecution
from stand.services.job_services import JobService
from stand.services.redis_service import connect_redis_store
from rq.exceptions import NoSuchJobError

log = logging.getLogger(__name__)


def apply_filter(query, args, name, transform=None, transform_name=None):
    result = query
    if name in args and args[name].strip() != '':
        v = transform(args[name]) if transform else args[name]
        f = transform_name(name) if transform_name else name
        result = query.filter_by(**{f: v})

    return result


def _get_jobs(jobs, permissions):
    if flask_global.user.id != 0:  # It is not a inter service call
        any_permission = ExecutionPermission.query.filter(
            ExecutionPermission.permission.in_(permissions)).all()
        if len(any_permission) == 0:
            jobs = jobs.filter(Job.user_id == flask_global.user.id)
    return jobs


class LatestJobDetailApi(Resource):
    @staticmethod
    @requires_auth
    def get():
        workflow_id = request.args.get('workflow')

        jobs = _get_jobs(Job.query, [PermissionType.LIST, PermissionType.STOP,
                                     PermissionType.MANAGE])
        jobs = jobs.filter(Job.workflow_id == workflow_id).order_by(
            Job.created.desc()).limit(1).all()

        if len(jobs) == 1:
            return JobItemResponseSchema(exclude=('workflow',)).dump(
                jobs[0])
        else:
            return dict(status="ERROR", message=gettext("Not found")), 404


class JobListApi(Resource):
    """ REST API for listing class Job """

    @staticmethod
    @requires_auth
    def get():
        only = None if request.args.get('simple') != 'true' else ('id',)
        if request.args.get('fields'):
            only = tuple(
                [x.strip() for x in request.args.get('fields').split(',')])

        jobs = _get_jobs(Job.query, [PermissionType.LIST, PermissionType.STOP,
                                     PermissionType.MANAGE])
        for name in ['workflow_id', 'user_id']:
            jobs = apply_filter(jobs, request.args, name, int,
                                lambda field: field)
        jobs = jobs.filter(
            Job.name.like(f'%%{request.args.get("name", "") or ""}%%'))

        job_type = request.args.get('type')
        if job_type:
            jobs = jobs.filter(Job.type == job_type)

        sort = request.args.get('sort', 'name')
        if sort not in ['status', 'id', 'user_name', 'workflow_name',
                        'workflow_id', 'finished', 'started', 'created']:
            sort = 'id'
        sort_option = getattr(Job, sort)
        if request.args.get('asc', 'true') == 'false':
            sort_option = sort_option.desc()

        jobs = jobs.order_by(sort_option)
        page = request.args.get('page') or '1'

        if page is not None and page.isdigit():
            page_size = int(request.args.get('size', 20))
            page = int(page)
            pagination = jobs.paginate(page, page_size, True)
            result = {
                'data': JobListResponseSchema(many=True, only=only).dump(
                    pagination.items),
                'pagination': {
                    'page': page, 'size': page_size,
                    'total': pagination.total,
                    'pages': int(math.ceil(1.0 * pagination.total / page_size))}
            }
        else:
            result = {'data': JobListResponseSchema(many=True, only=only).dump(
                jobs)}

        return result

    @staticmethod
    @requires_auth
    def post():
        result, result_code = dict(
            status="ERROR",
            message=gettext("Missing json in the request body")), 400
        if request.json is not None:
            try:
                request_json = request.json

                # This parameter is important when executing a job in
                # data explorer.
                persist = request_json.pop('persist', True)

                request_json['user'] = {
                    'id': flask_global.user.id,
                    'login': flask_global.user.login,
                    'name': f'{flask_global.user.first_name} {flask_global.user.last_name}'
                }
                request_json['job_key'] = ''

                request_schema = JobCreateRequestSchema()
                response_schema = JobItemResponseSchema()
                new_job = request_schema.load(request_json)
                new_job.status = StatusExecution.WAITING
                if not new_job.name:
                    new_job.name = request_json.get('workflow_name') or \
                        gettext('Unnamed job')
                # Check if cluster id is valid
                cluster_id = request_json.get('cluster', {}).get('id')
                cluster = Cluster.query.get(cluster_id)
                if cluster is None:
                    raise ValidationError({'cluster': ['Invalid cluster']})

                JobService.start(new_job, request_json['workflow'],
                                 request_json.get('app_configs', {}),
                                 persist=persist,
                                 testing=current_app.testing,
                                 lang=flask_global.user.locale)
                result_code = 201
                if persist:
                    result = dict(data=response_schema.dump(new_job),
                                  message='', status='OK')
                else:
                    result = {'status': 'OK', 'data': {'id': new_job.id}}
            except ValidationError as e:
                result = {'status': 'ERROR',
                          'message': gettext("Validation error"),
                          'errors': translate_validation(e.messages)}
                result_code = 400
            except KeyError:
                result['detail'] = gettext('Missing information in JSON')
            except ValueError:
                pass  # default return value
            except JobException as je:
                log.exception(gettext('Error in POST'))
                result = dict(status="ERROR", message=str(je),
                              code=je.error_code)
                result_code = 422
            except Exception as e:
                log.exception(gettext('Error in POST'))
                result, result_code = dict(status="ERROR",
                                           message=gettext(
                                               "Internal error")), 500
                if current_app.debug or True:
                    result['debug_detail'] = str(e)
                db.session.rollback()

        return result, result_code


class JobDetailApi(Resource):
    """ REST API for a single instance of class Job """

    @staticmethod
    @requires_auth
    def get(job_id):
        jobs = _get_jobs(Job.query.filter(Job.id == job_id),
                         [PermissionType.LIST, PermissionType.STOP,
                          PermissionType.MANAGE]).all()
        if len(jobs) == 1:
            return JobItemResponseSchema().dump(jobs[0])
        else:
            return dict(status="ERROR", message=gettext("Not found")), 404

    @staticmethod
    @requires_auth
    def delete(job_id):
        result, result_code = dict(status="ERROR", message="Not found"), 404
        job = _get_jobs(Job.query.filter(Job.id == job_id),
                        [PermissionType.LIST, PermissionType.STOP,
                         PermissionType.MANAGE]).first()
        if job is not None:
            try:
                JobService.stop(job, True)
                db.session.delete(job)
                db.session.commit()
                result, result_code = dict(status="OK", message="Deleted"), 204
            except Exception as e:
                log.exception('Error in DELETE')
                result, result_code = dict(status="ERROR",
                                           message="Internal error"), 500
                if current_app.debug:
                    result['debug_detail'] = str(e)
                db.session.rollback()
        return result, result_code


class JobStopActionApi(Resource):
    """ RPC API for action that stops a Job """

    @staticmethod
    @requires_auth
    def post(job_id):
        result, result_code = dict(status="ERROR",
                                   message=gettext("Not found")), 404
        breakpoint()
        job = Job.query.get(job_id)
        if job is not None:
            response_schema = JobItemResponseSchema()
            try:
                JobService.stop(job)
                result, result_code = dict(
                    status="OK", message=gettext("Job stopped"),
                    data=response_schema.dump(job)), 200
            except JobException as je:
                log.exception(gettext('Error in POST'))
                result, result_code = dict(status="ERROR",
                                           message=str(je),
                                           code=je.error_code), 400
            except Exception as e:
                log.exception(gettext('Error in POST'))
                result, result_code = dict(status="ERROR",
                                           message=gettext(
                                               "Internal error")), 500
                if current_app.debug:
                    result['debug_detail'] = str(e)
                db.session.rollback()
        else:
            # It may be a transient job (for instance, used in Experiments)
            # In this case, just send a message to stop the minion (if it's
            # running).
            JobService.stop(None, job_id=job_id)
            result, result_code = dict(
                status="OK",
                message=gettext("An attempt of stopping the job was made")), 200
        return result, result_code


class JobLockActionApi(Resource):
    """ RPC API for action that locks a Job for edition"""

    @staticmethod
    @requires_auth
    def get(job_id):
        result, result_code = dict(status="ERROR",
                                   message=gettext('Not found')), 404
        job = Job.query.get(job_id)
        if job is not None:
            lock_status = JobService.get_lock_status(job)
            result, result_code = dict(status="OK", message="",
                                       lock=lock_status), 200

        return result, result_code

    @staticmethod
    @requires_auth
    def post(job_id):
        result, result_code = dict(status="ERROR",
                                   message=gettext('Not found')), 404

        job = Job.query.get(job_id)
        if job is not None:
            data = json.loads(request.data)
            try:
                JobService.lock(job, data['user'], data['computer'])
                result, result_code = dict(status="OK",
                                           message=gettext('Locked')), 200
            except JobException as je:
                log.exception('Error in POST')
                result, result_code = dict(
                    status="ERROR", message=str(je), code=je.error_code), 422
                if je.error_code == 'ALREADY_LOCKED':
                    result_code = 409

            except Exception as e:
                log.exception('Error in POST')
                result, result_code = dict(status="ERROR",
                                           message=gettext(
                                               'Internal error')), 500
                if current_app.debug:
                    result['debug_detail'] = str(e)
                db.session.rollback()
        return result, result_code


class JobUnlockActionApi(Resource):
    """ RPC API for action that unlocks a Job for edition"""
    pass


class JobSampleActionApi(Resource):
    """ RPC API for action that retrieves sample results from backend """

    @staticmethod
    @requires_auth
    def post(job_id, task_id):
        result, result_code = dict(status="ERROR",
                                   message=gettext('Not found')), 404

        job = Job.query.get(job_id)
        if job is not None:
            try:
                data = json.loads(request.data)
                resp = JobService.retrieve_sample(data['user'], job, task_id,
                                                  data['port'], wait=30)

                result, result_code = dict(status=resp['status'],
                                           message=resp['message'],
                                           data=data,
                                           sample=resp['sample']), 200

            except JobException as je:
                log.exception('Error in POST')
                result, result_code = dict(
                    status="ERROR", message=str(je), code=je.error_code), 422

            except Exception as e:
                log.exception('Error in POST')
                result, result_code = dict(status="ERROR",
                                           message=gettext(
                                               'Internal error')), 500
                if current_app.debug:
                    result['debug_detail'] = str(e)
                db.session.rollback()

        return result, result_code


class UpdateJobStatusActionApi(Resource):
    """ RPC API for action that updates a Job status """

    @staticmethod
    @requires_auth
    def post(job_id):
        result, result_code = dict(status="ERROR",
                                   message=gettext('Not found')), 404

        if flask_global.user.id == 0:  # Only inter service requests
            job = Job.query.get(int(job_id))
            if job is not None:
                try:
                    job.status = request.json.get('status')
                    db.session.add(job)
                    db.session.commit()
                    result, result_code = dict(status="OK", message=""), 200
                except Exception as e:
                    log.exception('Error in POST')
                    result, result_code = dict(status="ERROR",
                                               message=gettext(
                                                   'Internal error')), 500
                    if current_app.debug:
                        result['debug_detail'] = str(e)
                    db.session.rollback()
        return result, result_code


class UpdateJobStepStatusActionApi(Resource):
    """ RPC API for action that updates a job step status """

    @staticmethod
    @requires_auth
    def post(job_id, task_id):
        result, result_code = dict(status="ERROR",
                                   message=gettext('Not found')), 404

        if flask_global.user.id == 0:  # Only inter service requests
            step = JobStep.query.filter(and_(
                JobStep.job_id == int(job_id),
                JobStep.task_id == task_id)).first()
            if step is not None:
                try:
                    step.status = request.json.get('status')
                    step.message = request.json.get('message')
                    db.session.add(step)
                    db.session.commit()
                    result, result_code = dict(status="OK", message=""), 200
                except Exception as e:
                    log.exception('Error in POST')
                    result, result_code = dict(status="ERROR",
                                               message=gettext(
                                                   'Internal error')), 500
                    if current_app.debug:
                        result['debug_detail'] = str(e)
                    db.session.rollback()
        return result, result_code


class JobSourceCodeApi(Resource):
    """ RPC API for action that returns generated source code
    for a job
    """

    @staticmethod
    @requires_auth
    def get(job_id):
        job = Job.query.get_or_404(ident=job_id)

        return {
            'lang': 'python',
            'source': job.source_code}, 200

    @staticmethod
    @requires_auth
    def patch(job_id):
        """ Updates the job source code """
        job = Job.query.get_or_404(ident=job_id)
        params = request.json
        if str(params.get('secret')) == str(
                current_app.config['STAND_CONFIG']['secret']):
            job.source_code = params.get('source')
            db.session.add(job)
            db.session.commit()
            return {'status': 'OK'}
        else:
            db.session.rollback()
            return {'status': 'FORBIDDEN'}, 403


class PerformanceModelEstimationResultApi(Resource):
    """
    Triggers the execution of an execution model in the backend.
    """

    @staticmethod
    @requires_auth
    def get(key):
        return JobService.get_performance_model_result(key)


class PerformanceModelEstimationApi(Resource):
    """
    Triggers the execution of an execution model in the backend.
    """

    @staticmethod
    @requires_auth
    def post(model_id):
        # Deadline in seconds
        if request.json is None:
            return {'status': 'ERROR',
                    'message': 'You need to inform the deadline'}
        deadline = request.json.get('deadline', 3600)
        return JobService.execute_performance_model(
            int(request.json.get('cluster_id', 0)),
            model_id, deadline, request.json.get('cores', [2]),
            request.json.get('platform', 'keras'),
            int(request.json.get('data_size', 1000)),
            int(request.json.get('iterations', 1000)),
            int(request.json.get('batch_size', 1000)),
        )


class DataSourceInitializationApi(Resource):
    """
    Initializes a data source
    """
    @staticmethod
    @requires_auth
    def get():
        job_id = request.args.get('key')
        redis_store = connect_redis_store(
            None, testing=False, decode_responses=False)
        try:
            rq_job = rq.job.Job(job_id, connection=redis_store)
            if rq_job and rq_job.result:
                print('*' * 10)
                print(rq_job.result)
                print('*' * 10)
                return {'status': rq_job.result.get('status'),
                        'result': rq_job.result}
            else:
                return {'status': 'PROCESSING'}
        except NoSuchJobError:
            return {'status': 'ERROR', 'message': 'Job not found'}

    @staticmethod
    @requires_auth
    def post():
        # Deadline in seconds
        if request.json is None:
            return {'status': 'ERROR',
                    'message': gettext('You need to inform the parameters')}
        redis_store = connect_redis_store(
            None, testing=False, decode_responses=False)
        q = rq.Queue('juicer', connection=redis_store)

        payload = request.json
        log.info("Payload %s", payload)
        result = q.enqueue('juicer.jobs.cache_vallum_data',
                           payload)
        return result.id


class WorkflowStartActionApi(Resource):
    """ RPC API for action that starts a Job from an workflow id """

    @staticmethod
    @requires_auth
    def post():
        if request.json is None:
            return {'status': 'ERROR',
                    'message': gettext('You need to inform the parameters')}

        workflow_id = request.json.get('workflow_id')
        if not workflow_id:
            return {'status': 'ERROR',
                    'message': gettext('You must inform workflow_id.')}

        payload = {'data': request.json, 'workflow_id': workflow_id}
        now = datetime.datetime.now()
        name = f'Workflow {workflow_id} @ {now.isoformat()}'

        job = Job(
            created=now,
            status=StatusExecution.WAITING,
            workflow_id=workflow_id,
            workflow_name=name,
            user_id=flask_global.user.id,
            user_login=flask_global.user.login,
            user_name=flask_global.user.name,
            name=payload.get('name', name),
            type=JobType.BATCH,
        )
        # Retrieves the workflow from tahiti
        tahiti_config = current_app.config['STAND_CONFIG']['services']['tahiti']
        url = f'{tahiti_config.get("url")}/workflows/{workflow_id}'
        headers = {'X-Auth-Token': str(tahiti_config['auth_token'])}
        try:
            r = requests.get(url, headers=headers)
            if r.status_code == 200:

                workflow = r.json()
                cluster_id = request.json.get('cluster_id',
                                              workflow.get('preferred_cluster_id'))
                if not cluster_id:
                    return {'status': 'ERROR',
                            'message': gettext(
                                'You must inform cluster_id or define a '
                                'preferred one in workflow.')}

                job.cluster = Cluster.query.get(int(cluster_id))
                job.workflow_definition = r.text
                JobService.start(job, workflow, {},
                                 JobType.BATCH, persist=True)
                return {'data': {'job': {'id': job.id}}}
            else:
                logging.error(gettext('Error retrieving workflow {}: {}').format(
                    workflow_id, r.text))
                return {'status': 'ERROR',
                        'message': gettext(
                            'Workflow not found or error retrieving it.')}
        except Exception as e:
            logging.error(e)
            return {'status': 'ERROR',
                    'message': gettext(
                        'Workflow not found or error retrieving it.')}


class WorkflowSourceCodeResultApi(Resource):
    """
    """

    @staticmethod
    @requires_auth
    def get(key):
        return JobService.get_generate_code_result(key)


class WorkflowSourceCodeApi(Resource):
    """
    """

    @staticmethod
    @requires_auth
    def post():
        if request.json is None:
            return {'status': 'ERROR',
                    'message': gettext('You need to inform the parameters')}

        workflow_id = request.json.get('workflow_id')
        if not workflow_id:
            return {'status': 'ERROR',
                    'message': gettext('You must inform workflow_id.')}

        return JobService.generate_code(workflow_id,
                                        request.json.get('template', 'python'))

class TriggerJobApi(Resource):
    SUPPORTED = {
        'convert_data_source': gettext('Convert data source'),
    }
    @staticmethod
    @requires_auth
    def get(name: str):
        if request.json is None:
            return {'status': 'ERROR',
                    'message': gettext('You need to inform the parameters')}
        return JobService.get_result(request.json.get('key'), flask_global.user).result

    @staticmethod
    @requires_auth
    def post(name: str):
        if request.json is None:
            return {'status': 'ERROR',
                    'message': gettext('You need to inform the parameters')}
        if name not in TriggerJobApi.SUPPORTED:
            return {'status': 'ERROR',
                    'message': gettext('Unsupported job name: {name}').format(
                        name)
            }
        job_key = JobService.trigger_job(name, request.json, flask_global.user)
        job = Job(
            name=TriggerJobApi.SUPPORTED.get(name),
            created=datetime.datetime.utcnow(),
            started=datetime.datetime.utcnow(),
            status='WAITING',
            workflow_id=0,
            workflow_name='',
            user_id=flask_global.user.id,
            user_name=flask_global.user.name,
            user_login=flask_global.user.login,
            cluster_id=1,
            type='BATCH',
            source_code=json.dumps(request.json),
            job_key=job_key)
        db.session.add(job)
        db.session.commit()
        return {'id': job_key}
