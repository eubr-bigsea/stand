# -*- coding: utf-8 -*-}
from flask import request, current_app
from flask_restful import Resource

from app_auth import requires_auth
from models import db, Job
from schema import *


class JobListApi(Resource):
    """ REST API for listing class Job """

    @staticmethod
    @requires_auth
    def get():
        only = ('id', 'name') \
            if request.args.get('simple', 'false') == 'true' else None
        jobs = Job.query.all()
        return JobListResponseSchema(many=True, only=only).dump(jobs).data

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
                    errors=form.errors,), 401
            else:
                try:
                    job = form.data
                    db.session.add(job)
                    db.session.commit()
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
