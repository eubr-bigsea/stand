# -*- coding: utf-8 -*-}
import math

from app_auth import requires_auth
from flask import request, current_app
from flask_restful import Resource
from schema import *
from stand.services import JobService


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
        for name in ['workflow', 'user']:
            jobs = apply_filter(jobs, request.args, name, int,
                                lambda field: field + '_id')
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
        if request.json is not None:
            request_schema = JobCreateRequestSchema()
            response_schema = JobItemResponseSchema()
            form = request_schema.load(request.json)
            if form.errors:
                result, result_code = dict(
                    status="ERROR", message="Validation error",
                    errors=form.errors), 401
            else:
                try:
                    job = form.data
                    JobService.save(job)

                    result, result_code = response_schema.dump(job).data, 200
                except Exception, e:
                    result, result_code = dict(status="ERROR",
                                               message="Internal error"), 500
                    if current_app.debug:
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
