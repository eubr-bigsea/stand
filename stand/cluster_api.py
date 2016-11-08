# -*- coding: utf-8 -*-}
from app_auth import requires_auth
from flask import request, current_app
from flask_restful import Resource
from schema import *


class ClusterListApi(Resource):
    """ REST API for listing class Cluster """

    @staticmethod
    @requires_auth
    def get():
        only = ('id', 'name') \
            if request.args.get('simple', 'false') == 'true' else None
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
            status="ERROR", message="Missing json in the request body"), 401
        if request.json is not None:
            request_schema = ClusterCreateRequestSchema()
            response_schema = ClusterItemResponseSchema()
            form = request_schema.load(request.json)
            if form.errors:
                result, result_code = dict(
                    status="ERROR", message="Validation error",
                    errors=form.errors), 401
            else:
                try:
                    cluster = form.data
                    db.session.add(cluster)
                    db.session.commit()
                    result, result_code = response_schema.dump(
                        cluster).data, 200
                except Exception, e:
                    result, result_code = dict(status="ERROR",
                                               message="Internal error"), 500
                    if current_app.debug:
                        result['debug_detail'] = e.message
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
            return dict(status="ERROR", message="Not found"), 404
