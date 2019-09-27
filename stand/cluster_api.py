# -*- coding: utf-8 -*-}
import logging

import rq
from flask import request, current_app
from flask_babel import gettext
from flask_restful import Resource
from rq.job import Job as RQJob
from stand.app_auth import requires_auth
from stand.schema import *
from stand.services.redis_service import connect_redis_store

log = logging.getLogger(__name__)


class ClusterListApi(Resource):
    """ REST API for listing class Cluster """

    @staticmethod
    @requires_auth
    def get():
        if request.args.get('fields'):
            only = [f.strip() for f in
                    request.args.get('fields').split(',')]
        elif request.args.get('simple', 'false') == 'true':
            only = ('id', 'name')
        else:
            only = ('id', 'name', 'flavors', 'enabled')
        enabled_filter = request.args.get('enabled')
        if enabled_filter:
            clusters = Cluster.query.filter(
                Cluster.enabled == (enabled_filter != 'false'))
        else:
            clusters = Cluster.query.all()

        return ClusterListResponseSchema(
            many=True, only=only).dump(clusters).data

    @staticmethod
    @requires_auth
    def post():
        result, result_code = dict(
            status="ERROR",
            message=gettext('Missing json in the request body')), 400
        if request.json is not None:
            request_schema = ClusterCreateRequestSchema()
            response_schema = ClusterItemResponseSchema()
            form = request_schema.load(request.json)
            if form.errors:
                result, result_code = dict(
                    status="ERROR", message=gettext('Validation error'),
                    errors=form.errors), 400
            else:
                try:
                    cluster = form.data
                    db.session.add(cluster)
                    db.session.commit()
                    result, result_code = response_schema.dump(
                        cluster).data, 200
                except Exception as e:
                    log.exception('Error in POST')
                    result, result_code = dict(status="ERROR",
                                               message=gettext(
                                                   'Internal error')), 500
                    if current_app.debug:
                        result['debug_detail'] = str(e)
                    db.session.rollback()

        return result, result_code


class ClusterDetailApi(Resource):
    """ REST API for a single instance of class Cluster """

    @staticmethod
    @requires_auth
    def get(cluster_id):
        cluster = Cluster.query.get(cluster_id)
        if cluster is not None:
            return ClusterItemResponseSchema().dump(cluster).data
        else:
            return dict(status="ERROR", message=gettext("Not found")), 404


class PerformanceModelEstimationApi(Resource):
    @staticmethod
    @requires_auth
    def post():
        result, result_code = dict(
            status="ERROR",
            message=gettext('Missing json in the request body')), 400

        if request.json is not None:
            request_schema = PerformanceModelEstimationRequestSchema()
            response_schema = PerformanceModelEstimationResponseSchema()
            form = request_schema.load(request.json)

            if form.errors:
                result, result_code = dict(
                    status="ERROR", message=gettext('Validation error'),
                    errors=form.errors), 400
            else:
                try:
                    cluster = Cluster.query.get(form.data['cluster_id'])
                    if cluster is not None:
                        payload = {}
                        payload.update(form.data)
                        q = rq.Queue(
                            name='juicer',
                            connection=connect_redis_store(None, testing=False))
                        schedule_id = q.enqueue(
                            'juicer.jobs.estimate_time_with_performance_model',
                            payload).id
                        result, result_code = response_schema.dump(
                            {'schedule_id': schedule_id}).data, 200

                    else:
                        result, result_code = dict(status="ERROR",
                                                   message=gettext(
                                                       "Not found")), 404

                except Exception as e:
                    log.exception('Error in POST')
                    result, result_code = dict(status="ERROR",
                                               message=gettext(
                                                   'Internal error')), 500
                    if current_app.debug:
                        result['debug_detail'] = str(e)
                    db.session.rollback()

        return result, result_code


class PerformanceModelResultApi(Resource):
    @staticmethod
    @requires_auth
    def get(schedule_id):
        conn = connect_redis_store(None, testing=False, decode_responses=False)
        rq_job = RQJob(schedule_id, conn)
        if rq_job is not None and rq_job.get_status() is not None:
            if rq_job.is_finished:
                return rq_job.result, 200
            else:
                return dict(status="PROCESSING", job_status=rq_job.get_status(),
                            message=gettext("Job is still running")), 200
        else:
            return dict(status="ERROR", message=gettext("Not found")), 404
