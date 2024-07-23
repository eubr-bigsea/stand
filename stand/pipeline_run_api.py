import datetime
import math
import logging

from stand.app_auth import requires_auth

from flask import request, g as flask_g
from flask_restful import Resource
from http import HTTPStatus

from stand.schema import (PipelineRunCreateRequestSchema,
                          PipelineRunItemResponseSchema,
                          PipelineRunListResponseSchema, partial_schema_factory)
from stand.models import (PipelineRun, db)
from flask_babel import gettext
from sqlalchemy import func, and_, or_
from sqlalchemy.sql import and_
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
            pipeline_runs = pipeline_runs.filter(
                PipelineRun.status == status_filter)
        name_filter = request.args.get('name')
        if name_filter:
            pipeline_runs = pipeline_runs.filter(
                PipelineRun.pipeline_name.ilike(f'%{name_filter}%'))

        start_filter = request.args.get('start')
        if start_filter:
            start_filter = datetime.datetime.strptime(start_filter, '%Y-%m-%d')

        end_filter = request.args.get('end')
        if end_filter:
            end_filter = datetime.datetime.strptime(end_filter, '%Y-%m-%d')

        if start_filter and end_filter:
            pipeline_runs = pipeline_runs.filter(
                and_(
                    PipelineRun.start <= end_filter,
                    PipelineRun.finish >= start_filter
                )
            )
        elif start_filter:
            pipeline_runs = pipeline_runs.filter(
                PipelineRun.finish >= start_filter)
        elif end_filter:
            pipeline_runs = pipeline_runs.filter(
                PipelineRun.start <= end_filter)

        pipelines_filter = request.args.get('pipelines')
        if pipelines_filter:
            pipeline_ids = [int(x) for x in pipelines_filter.split(',')]
            pipeline_runs = pipeline_runs.filter(
                PipelineRun.pipeline_id.in_(pipeline_ids))

        latest_filter = request.args.get('latest')

        if latest_filter in ('true', 1, 'True', '1'):
            subquery = (
                db.session.query(
                    PipelineRun.pipeline_id,
                    func.max(PipelineRun.start).label("max_start"),
                )
            )
            if pipelines_filter:
                subquery = subquery.filter(PipelineRun.pipeline_id.in_(
                    pipeline_ids))
            subquery = subquery.group_by(PipelineRun.pipeline_id).subquery()

            pipeline_runs = (
                pipeline_runs.join(
                    subquery,
                    and_(
                        PipelineRun.pipeline_id == subquery.c.pipeline_id,
                        PipelineRun.start == subquery.c.max_start,
                    )
                ).order_by(PipelineRun.pipeline_id)
            )
            print(str(pipeline_runs))
            result = PipelineRunListResponseSchema(
                    many=True, only=only).dump(pipeline_runs.all())
        else:
            sort = request.args.get('sort', 'id')
            if sort not in ['pipeline_id', 'id', 'updated', 'start']:
                sort = 'id'
            sort_option = getattr(PipelineRun, sort)
            if request.args.get('asc', 'true') == 'false':
                sort_option = sort_option.desc()

            pipeline_runs = pipeline_runs.order_by(sort_option)
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
    @requires_auth
    def delete(self, pipeline_run_id):
        """
        Delete a single instance of class PipelineRun.

        :param pipeline_run_id: The ID of the PipelineRun instance to delete.
        :type pipeline_run_id: int
        :return: A JSON object containing a success message.
        :rtype: dict
        """

        return_code = HTTPStatus.NO_CONTENT
        if log.isEnabledFor(logging.DEBUG):
            log.debug(gettext('Deleting %s (id=%s)'), self.human_name,
                      pipeline_run_id)
        pipeline = PipelineRun.query.get(pipeline_run_id)
        if pipeline is not None:
            db.session.delete(pipeline)
            db.session.commit()
            result = {
                'status': 'OK',
                'message': gettext('%(name)s deleted with success!',
                                   name=self.human_name)
            }
        else:
            return_code = HTTPStatus.NOT_FOUND
            result = {
                'status': 'ERROR',
                'message': gettext('%(name)s not found (id=%(id)s).',
                                   name=self.human_name, id=pipeline_run_id)
            }
        return result, return_code

    @requires_auth
    def patch(self, pipeline_run_id):
        """
        Update a single instance of class PipelineRun.

        :param pipeline_run_id: The ID of the PipelinRun instance to update.
        :type pipeline_run_id: int
        :return: A JSON object containing a success message.
        :rtype: dict
        """
        result = {'status': 'ERROR', 'message': gettext('Insufficient data.')}
        return_code = HTTPStatus.NOT_FOUND

        if log.isEnabledFor(logging.DEBUG):
            log.debug(gettext('Updating %s (id=%s)'), self.human_name,
                      pipeline_run_id)
        if request.json:
            request_schema = partial_schema_factory(
                PipelineRunCreateRequestSchema)
            response_schema = PipelineRunItemResponseSchema()
            # Ignore missing fields to allow partial updates

            data = request.json
            data['user_id'] = flask_g.user.id
            data['user_login'] = flask_g.user.login
            data['user_name'] = flask_g.user.name

            pipeline_run = request_schema.load(data, partial=True)
            pipeline_run.id = pipeline_run_id
            pipeline_run = db.session.merge(pipeline_run)

            db.session.commit()

            if pipeline_run is not None:
                return_code = HTTPStatus.OK
                result = {
                    'status': 'OK',
                    'message': gettext(
                        '%(n)s (id=%(id)s) was updated with success!',
                        n=self.human_name,
                        id=pipeline_run_id),
                    'data': [response_schema.dump(
                        pipeline_run)]
                }
        return result, return_code


class PipelineRunFromPipelineApi(Resource):
    """ REST API for listing class PipelineRun """
    def post(pipeline_id: int):
        pass

