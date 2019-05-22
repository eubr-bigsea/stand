# -*- coding: utf-8 -*-}
import logging
import math

from flask import g
from flask import request, current_app
from flask_babel import gettext
from flask_restful import Resource
from sqlalchemy import and_
from stand.app_auth import requires_auth
from stand.schema import *
from stand.services.job_services import JobService

log = logging.getLogger(__name__)


def apply_filter(query, args, name, transform=None, transform_name=None):
    result = query
    if name in args and args[name].strip() != '':
        v = transform(args[name]) if transform else args[name]
        f = transform_name(name) if transform_name else name
        result = query.filter_by(**{f: v})

    return result


def _get_jobs(jobs, permissions):
    if g.user.id != 0:  # It is not a inter service call
        any_permission = ExecutionPermission.query.filter(
            ExecutionPermission.permission.in_(permissions)).all()
        if len(any_permission) == 0:
            jobs = jobs.filter(Job.user_id == g.user.id)
    return jobs


class LatestJobDetailApi(Resource):
    @staticmethod
    @requires_auth
    def get():
        workflow_id = request.args.get('workflow_id')

        jobs = _get_jobs(Job.query, [PermissionType.LIST, PermissionType.STOP,
                                     PermissionType.MANAGE])
        jobs = jobs.filter(Job.workflow_id == workflow_id).order_by(
            Job.created.desc()).limit(1).all()

        if len(jobs) == 1:
            return JobItemResponseSchema(exclude=('workflow',)).dump(
                jobs[0]).data
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
            Job.name.like('%%{}%%'.format(request.args.get('name', '') or '')))

        sort = request.args.get('sort', 'name')
        if sort not in ['status', 'id', 'user_name', 'workflow_name',
                        'workflow_id', 'updated']:
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
                    pagination.items).data,
                'pagination': {
                    'page': page, 'size': page_size,
                    'total': pagination.total,
                    'pages': int(math.ceil(1.0 * pagination.total / page_size))}
            }
        else:
            result = {'data': JobListResponseSchema(many=True, only=only).dump(
                jobs).data}

        return result

    @staticmethod
    @requires_auth
    def post():
        result, result_code = dict(
            status="ERROR",
            message=gettext("Missing json in the request body")), 400
        if request.data is not None:
            try:
                request_json = json.loads(request.data)
                request_json['user']['id'] = g.user.id
                request_json['user']['login'] = g.user.login
                request_json['user']['name'] = ' '.join([g.user.first_name,
                                                         g.user.last_name])

                request_schema = JobCreateRequestSchema()
                response_schema = JobItemResponseSchema()

                request_json['workflow']['locale'] = request.headers.get(
                    'Locale', 'en') or 'en'
                request_json['status'] = StatusExecution.WAITING
                if not request_json.get('name'):
                    request_json['name'] = request_json.get('workflow_name')

                form = request_schema.load(request_json)

                if form.errors:
                    result, result_code = dict(
                        status="ERROR", message=gettext("Validation error"),
                        errors=form.errors), 422
                else:
                    job = form.data
                    JobService.start(job, request_json['workflow'],
                                     request_json.get('app_configs', {}))
                    result_code = 200
                    result = dict(data=response_schema.dump(job).data,
                                  message='', status='OK')
            except KeyError:
                result['detail'] = gettext('Missing information in JSON')
            except ValueError:
                pass  # default return value
            except JobException as je:
                log.exception(gettext('Error in POST'))
                result = dict(status="ERROR", message=je.message,
                              code=je.error_code)
                result_code = 422
            except Exception as e:
                log.exception(gettext('Error in POST'))
                result, result_code = dict(status="ERROR",
                                           message=gettext(
                                               "Internal error")), 500
                if current_app.debug or True:
                    result['debug_detail'] = e.message
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
            return JobItemResponseSchema().dump(jobs[0]).data
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
                db.session.delete(job)
                db.session.commit()
                result, result_code = dict(status="OK", message="Deleted"), 200
            except Exception as e:
                log.exception('Error in DELETE')
                result, result_code = dict(status="ERROR",
                                           message="Internal error"), 500
                if current_app.debug:
                    result['debug_detail'] = e.message
                db.session.rollback()
        return result, result_code

    @staticmethod
    @requires_auth
    def patch(job_id):
        result = dict(status="ERROR", message=gettext("Insufficient data"))
        result_code = 404
        # noinspection PyBroadException
        try:
            if request.data:
                request_json = json.loads(request.data)
                request_schema = partial_schema_factory(
                    JobCreateRequestSchema)
                for task in request_json.get('tasks', {}):
                    task['forms'] = {k: v for k, v in task['forms'].items()
                                     if v.get('value') is not None}
                # Ignore missing fields to allow partial updates
                params = {}
                params.update(request_json)
                if 'platform_id' in params and params['platform_id'] is None:
                    params.pop('platform_id')
                if 'user' in params:
                    user = params.pop('user')
                    params['user_id'] = user['id']
                    params['user_login'] = user['login']
                    params['user_name'] = user['name']

                form = request_schema.load(params, partial=True)
                response_schema = JobItemResponseSchema()
                if not form.errors:
                    try:
                        form.data.id = job_id
                        job = db.session.merge(form.data)
                        db.session.flush()
                        db.session.commit()

                        if job is not None:
                            result, result_code = dict(
                                status="OK", message="Updated",
                                data=response_schema.dump(job).data), 200
                        else:
                            result = dict(status="ERROR",
                                          message=gettext("Not found"))
                    except Exception as e:
                        log.exception(gettext('Error in PATCH'))
                        result, result_code = dict(
                            status="ERROR",
                            message=gettext("Internal error")), 500
                        if current_app.debug:
                            result['debug_detail'] = e.message
                        db.session.rollback()
                else:
                    result = dict(status="ERROR",
                                  message=gettext("Invalid data"),
                                  errors=form.errors)
        except Exception:
            log.exception(gettext('Error in PATCH'))
            result_code = 500
            import sys
            result = {'status': "ERROR", 'message': sys.exc_info()[1]}
        return result, result_code


class JobStopActionApi(Resource):
    """ RPC API for action that stops a Job """

    @staticmethod
    @requires_auth
    def post(job_id):
        result, result_code = dict(status="ERROR",
                                   message=gettext("Not found")), 404

        job = Job.query.get(job_id)
        if job is not None:
            response_schema = JobItemResponseSchema()
            try:
                JobService.stop(job)
                result, result_code = dict(
                    status="OK", message=gettext("Deleted"),
                    data=response_schema.dump(job).data), 200
            except JobException as je:
                log.exception(gettext('Error in POST'))
                result, result_code = dict(status="ERROR",
                                           message=je.message,
                                           code=je.error_code), 422
                # if je.error_code == JobException.ALREADY_FINISHED:
                #     result['status'] = 'OK'
                #     result['data'] = response_schema.dump(job).data
                #     result_code = 200
            except Exception as e:
                log.exception(gettext('Error in POST'))
                result, result_code = dict(status="ERROR",
                                           message=gettext(
                                               "Internal error")), 500
                if current_app.debug:
                    result['debug_detail'] = e.message
                db.session.rollback()
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
                    status="ERROR", message=je.message, code=je.error_code), 422
                if je.error_code == JobException.ALREADY_LOCKED:
                    result_code = 409

            except Exception as e:
                log.exception('Error in POST')
                result, result_code = dict(status="ERROR",
                                           message=gettext(
                                               'Internal error')), 500
                if current_app.debug:
                    result['debug_detail'] = e.message
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
                    status="ERROR", message=je.message, code=je.error_code), 422

            except Exception as e:
                log.exception('Error in POST')
                result, result_code = dict(status="ERROR",
                                           message=gettext(
                                               'Internal error')), 500
                if current_app.debug:
                    result['debug_detail'] = e.message
                db.session.rollback()

        return result, result_code


class UpdateJobStatusActionApi(Resource):
    """ RPC API for action that updates a Job status """

    @staticmethod
    @requires_auth
    def post(job_id):
        result, result_code = dict(status="ERROR",
                                   message=gettext('Not found')), 404

        if g.user.id == 0:  # Only inter service requests
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
                        result['debug_detail'] = e.message
                    db.session.rollback()
        return result, result_code


class UpdateJobStepStatusActionApi(Resource):
    """ RPC API for action that updates a job step status """

    @staticmethod
    @requires_auth
    def post(job_id, task_id):
        result, result_code = dict(status="ERROR",
                                   message=gettext('Not found')), 404

        if g.user.id == 0:  # Only inter service requests
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
                        result['debug_detail'] = e.message
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
        params = json.loads(request.data)
        if str(params.get('secret')) == str(
                current_app.config['STAND_CONFIG']['secret']):
            job.source_code = params.get('source')
            db.session.add(job)
            db.session.commit()
            return {'status': 'OK'}
        else:
            db.session.rollback()
            return {'status': 'FORBIDDEN'}, 403
