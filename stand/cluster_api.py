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
            only = ('id', 'name', 'flavors', 'enabled', 'ui_parameters')
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
    human_name = 'Cluster'

    @staticmethod
    @requires_auth
    def get(cluster_id):
        cluster = Cluster.query.get(cluster_id)
        if cluster is not None:
            return ClusterItemResponseSchema().dump(cluster).data
        else:
            return dict(status="ERROR", message=gettext("Not found")), 404

    @requires_auth
    def delete(self, cluster_id):
        return_code = 200
        if log.isEnabledFor(logging.DEBUG):
            log.debug(gettext('Deleting %s (id=%s)'), self.human_name,
                      cluster_id)
        cluster = Cluster.query.get(cluster_id)
        if cluster is not None:
            try:
                db.session.delete(cluster)
                db.session.commit()
                result = {
                    'status': 'OK',
                    'message': gettext('%(name)s deleted with success!',
                                       name=self.human_name)
                }
            except Exception as e:
                result = {'status': 'ERROR',
                          'message': gettext("Internal error")}
                return_code = 500
                if current_app.debug:
                    result['debug_detail'] = str(e)
                db.session.rollback()
        else:
            return_code = 404
            result = {
                'status': 'ERROR',
                'message': gettext('%(name)s not found (id=%(id)s).',
                                   name=self.human_name, id=cluster_id)
            }
        return result, return_code

    @requires_auth
    def patch(self, cluster_id):
        result = {'status': 'ERROR', 'message': gettext('Insufficient data.')}
        return_code = 400

        if log.isEnabledFor(logging.DEBUG):
            log.debug(gettext('Updating %s (id=%s)'), self.human_name,
                      cluster_id)
        if request.json:
            request_schema = partial_schema_factory(
                ClusterCreateRequestSchema)
            # Ignore missing fields to allow partial updates
            form = request_schema.load(request.json, partial=True)
            response_schema = ClusterItemResponseSchema()
            if not form.errors:
                try:
                    form.data.id = cluster_id
                    cluster = db.session.merge(form.data)
                    db.session.commit()

                    if cluster is not None:
                        return_code = 200
                        result = {
                            'status': 'OK',
                            'message': gettext(
                                '%(n)s (id=%(id)s) was updated with success!',
                                n=self.human_name,
                                id=cluster_id),
                            'data': [response_schema.dump(
                                cluster).data]
                        }
                except Exception as e:
                    result = {'status': 'ERROR',
                              'message': gettext("Internal error")}
                    return_code = 500
                    if current_app.debug:
                        result['debug_detail'] = str(e)
                    db.session.rollback()
            else:
                result = {
                    'status': 'ERROR',
                    'message': gettext('Invalid data for %(name)s (id=%(id)s)',
                                       name=self.human_name,
                                       id=cluster_id),
                    'errors': form.errors
                }
        return result, return_code


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
