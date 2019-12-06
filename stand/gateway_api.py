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
import json
log = logging.getLogger(__name__)


class MetricListApi(Resource):
    """ REST API for listing class Cluster """

    @staticmethod
    @requires_auth
    def post():
        result, result_code = dict(
            status="ERROR",
            message=gettext('Missing json in the request body')), 400
        if request.json is not None:
            redis_store = connect_redis_store(
                None, testing=False, decode_responses=False)
            q = rq.Queue('seed', connection=redis_store)

            payload = request.json
            log.info("Payload %s", payload)
            with open('/tmp/payload.json', 'a') as f:
                f.write(json.dumps(payload))
                f.write('\n')
            result = q.enqueue('seed.jobs.send_to_tma', payload)
            result_code = 200
            result = {'status': 'OK', 'message': 'Data received'}
        return result, result_code


