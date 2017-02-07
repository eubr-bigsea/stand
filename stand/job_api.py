# -*- coding: utf-8 -*-}
import math

from app_auth import requires_auth
from flask import request, current_app
from flask_restful import Resource
from schema import *
from stand.services.job_services import JobService


def apply_filter(query, args, name, transform=None, transform_name=None):
    result = query
    if name in args and args[name].strip() != '':
        v = transform(args[name]) if transform else args[name]
        f = transform_name(name) if transform_name else name
        result = query.filter_by(**{f: v})

    return result


class JobListApi(Resource):
    """ REST API for listing class Job """

    @staticmethod
    @requires_auth
    def get():
        only = None
        if request.args.get('fields'):
            only = tuple(
                [x.strip() for x in request.args.get('fields').split(',')])

        jobs = Job.query
        for name in ['workflow_id', 'user_id']:
            jobs = apply_filter(jobs, request.args, name, int,
                                lambda field: field)

        sort = request.args.get('sort', 'name')
        if sort not in ['status', 'id', 'user_name', 'workflow_name',
                        'workflow_id', 'updated']:
            sort = 'id'
        sort_option = getattr(Job, sort)
        if request.args.get('asc', 'true') == 'false':
            sort_option = sort_option.desc()

        jobs = jobs.order_by(sort_option)

        page = request.args.get('page')

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
            status="ERROR", message="Missing json in the request body"), 401
        if request.data is not None:
            request_json = json.loads(request.data)
            request_schema = JobCreateRequestSchema()
            response_schema = JobItemResponseSchema()

            request_json['status'] = StatusExecution.WAITING

            form = request_schema.load(request_json)

            if form.errors:
                result, result_code = dict(
                    status="ERROR", message="Validation error",
                    errors=form.errors), 401
            else:
                try:
                    job = form.data
                    JobService.start(job, request_json['workflow'])
                    result_code = 200
                    result = dict(data=response_schema.dump(job).data,
                                  message='', status='OK')
                except JobException as je:
                    result = dict(status="ERROR", message=je.message,
                                  code=je.error_code)
                    result_code = 401
                except Exception as e:
                    result, result_code = dict(status="ERROR",
                                               message="Internal error"), 500
                    if current_app.debug or True:
                        result['debug_detail'] = e.message
                    db.session.rollback()

        return result, result_code


class JobDetailApi(Resource):
    """ REST API for a single instance of class Job """

    @staticmethod
    @requires_auth
    def get(job_id):
        job = Job.query.get(job_id)
        if job is not None:
            return JobItemResponseSchema().dump(job).data
        else:
            return dict(status="ERROR", message="Not found"), 404

    @staticmethod
    @requires_auth
    def patch(job_id):
        result = dict(status="ERROR", message="Insufficient data")
        result_code = 404
        try:
            if request.data:
                request_json = json.loads(request.data)
                request_schema = partial_schema_factory(
                    JobCreateRequestSchema)
                for task in request_json.get('tasks', {}):
                    task['forms'] = {k: v for k, v in task['forms'].iteritems() \
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
                            result = dict(status="ERROR", message="Not found")
                    except Exception, e:
                        result, result_code = dict(
                            status="ERROR", message="Internal error"), 500
                        if current_app.debug:
                            result['debug_detail'] = e.message
                        db.session.rollback()
                else:
                    result = dict(status="ERROR", message="Invalid data",
                                  errors=form.errors)
        except Exception as e:
            result_code = 500
            import sys
            result = {'status': "ERROR", 'message': sys.exc_info()[1]}
        return result, result_code


class JobStopActionApi(Resource):
    """ RPC API for action that stops a Job """

    @staticmethod
    @requires_auth
    def post(job_id):
        result, result_code = dict(status="ERROR", message="Not found"), 404

        job = Job.query.get(job_id)
        if job is not None:
            response_schema = JobItemResponseSchema()
            try:
                JobService.stop(job)
                result, result_code = dict(
                    status="OK", message="Deleted",
                    data=response_schema.dump(job).data), 200
            except JobException, je:
                result, result_code = dict(status="ERROR",
                                           message=je.message,
                                           code=je.error_code), 401
                # if je.error_code == JobException.ALREADY_FINISHED:
                #     result['status'] = 'OK'
                #     result['data'] = response_schema.dump(job).data
                #     result_code = 200
            except Exception as e:
                result, result_code = dict(status="ERROR",
                                           message="Internal error"), 500
                if current_app.debug:
                    result['debug_detail'] = e.message
                db.session.rollback()
        return result, result_code


class JobLockActionApi(Resource):
    """ RPC API for action that locks a Job for edition"""

    @staticmethod
    @requires_auth
    def get(job_id):
        result, result_code = dict(status="ERROR", message="Not found"), 404
        job = Job.query.get(job_id)
        if job is not None:
            lock_status = JobService.get_lock_status(job)
            result, result_code = dict(status="OK", message="",
                                       lock=lock_status), 200

        return result, result_code

    @staticmethod
    @requires_auth
    def post(job_id):
        result, result_code = dict(status="ERROR", message="Not found"), 404

        job = Job.query.get(job_id)
        if job is not None:
            data = json.loads(request.data)
            try:
                JobService.lock(job, data['user'], data['computer'])
                result, result_code = dict(status="OK", message="Locked"), 200
            except JobException, je:
                result, result_code = dict(
                    status="ERROR", message=je.message, code=je.error_code), 401
                if je.error_code == JobException.ALREADY_LOCKED:
                    result_code = 409

            except Exception, e:
                result, result_code = dict(status="ERROR",
                                           message="Internal error"), 500
                if current_app.debug:
                    result['debug_detail'] = e.message
                db.session.rollback()
        return result, result_code


class JobUnlockActionApi(Resource):
    """ RPC API for action that unlocks a Job for edition"""
    pass
