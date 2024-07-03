import math
import logging

from stand.app_auth import requires_auth, requires_permission

from flask import request
from flask_restful import Resource
from http import HTTPStatus

from stand.schema import (PipelineRunCreateRequestSchema,
                          PipelineRunItemResponseSchema,
                          PipelineRunListResponseSchema)
from stand.models import (PipelineRun, db)
from flask_babel import gettext

log = logging.getLogger(__name__)
# region Protected\s*
# endregion\w*


class PipelineRunListApi(Resource):
    """ REST API for listing class PipelineRun """

    def __init__(self):
        self.human_name = gettext('PipelineRun')

    @requires_auth
    def get(self):
        """
        Retrieve a list of instances of class PipelineRun.

        :return: A JSON object containing the list of PipelineRun instances data.
        :rtype: dict
        """
        if request.args.get('fields'):
            only = [f.strip() for f in request.args.get('fields').split(',')]
        else:
            only = ('id', ) if request.args.get(
                'simple', 'false') == 'true' else None
        pipeline_runs = PipelineRun.query

        status_filter = request.args.get('status')
        if status_filter:
            pipeline_runs = PipelineRun.query.filter(
                PipelineRun.status == status_filter)
        pipeline_filter = request.args.get('pipeline')
        if pipeline_filter:
            pipeline_runs = PipelineRun.query.filter(
                PipelineRun.pipeline_id == pipeline_filter)

        page = request.args.get('page', default=1, type=int)
        page_size = request.args.get('size', default=20, type=int)

        pagination = pipeline_runs.paginate(page, page_size, True)
        result = {
            'data': PipelineRunListResponseSchema(
                many=True, only=only).dump(pagination.items),
            'pagination': {
                'page': page, 'size': page_size,
                'total': pagination.total,
                'pages': int(math.ceil(1.0 * pagination.total / page_size))}
        }


        if log.isEnabledFor(logging.DEBUG):
            log.debug(gettext('Listing %(name)s', name=self.human_name))
        return result

    @requires_auth
    def post(self):
        """
        Add a single instance of class PipelineRun.

        :return: A JSON object containing a success message.
        :rtype: dict
        """
        result = {'status': 'ERROR',
                  'message': gettext("Missing json in the request body")}
        return_code = HTTPStatus.BAD_REQUEST

        if request.json is not None:
            request_schema = PipelineRunCreateRequestSchema()
            response_schema = PipelineRunItemResponseSchema()
            data = request.json
            pipeline_run = request_schema.load(request.json)

            if log.isEnabledFor(logging.DEBUG):
                log.debug(gettext('Adding %s'), self.human_name)
            pipeline_run = pipeline_run
            db.session.add(pipeline_run)
            db.session.commit()
            result = response_schema.dump(pipeline_run)
            return_code = HTTPStatus.CREATED
        return result, return_code


class PipelineRunDetailApi(Resource):
    """ REST API for a single instance of class PipelineRun """

    def __init__(self):
        self.human_name = gettext('PipelineRun')

    @requires_auth
    def get(self, pipeline_run_id):
        """
        Retrieve a single instance of class PipelineRun.

        :param pipeline_run_id: The ID of the PipelineRun instance to retrieve.
        :type pipeline_run_id: int
        :return: A JSON object containing the PipelineRun instance data.
        :rtype: dict
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug(gettext('Retrieving %s (id=%s)'), self.human_name,
                      pipeline_run_id)

        pipeline_run = PipelineRun.query.get(pipeline_run_id)
        return_code = HTTPStatus.OK
        if pipeline_run is not None:
            result = {
                'status': 'OK',
                'data': [PipelineRunItemResponseSchema().dump(
                    pipeline_run)]
            }
        else:
            return_code = HTTPStatus.NOT_FOUND
            result = {
                'status': 'ERROR',
                'message': gettext(
                    '%(name)s not found (id=%(id)s)',
                    name=self.human_name, id=pipeline_run_id)
            }

        return result, return_code

