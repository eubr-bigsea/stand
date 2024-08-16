import datetime
import logging
import math
from http import HTTPStatus

from flask import current_app, request
from flask import g as flask_g
from flask_babel import gettext
from flask_restful import Resource
from marshmallow import Schema, fields
from sqlalchemy import and_, func, extract, case, or_

from stand.app_auth import requires_auth
from stand.models import Job, PipelineRun, PipelineStepRun, db
from stand.models_extra import Period
from stand.schema import (
    PipelineRunCreateRequestSchema,
    PipelineRunItemResponseSchema,
    PipelineRunListResponseSchema,
    PipelineStepRunItemResponseSchema,
    partial_schema_factory,
)
from stand.services.pipeline_run_service import (
    create_pipeline_run_from_pipeline,
    execute_pipeline_step_run,
    get_pipeline_from_api,
)

log = logging.getLogger(__name__)
# region Protected\s*
# endregion\w*


def _get_pipeline_runs_basic_query():
    pipeline_runs = PipelineRun.query

    status_filter = request.args.get("status")
    if status_filter:
        pipeline_runs = pipeline_runs.filter(PipelineRun.status == status_filter)
    name_filter = request.args.get("name")
    if name_filter:
        if name_filter.isdigit():
            pipeline_runs = pipeline_runs.filter(
                or_(
                    PipelineRun.pipeline_name.ilike(f"%{name_filter}%"),
                    PipelineRun.pipeline_id == int(name_filter),
                )
            )
        else:
            pipeline_runs = pipeline_runs.filter(
                PipelineRun.pipeline_name.ilike(f"%{name_filter}%")
            )

    return pipeline_runs


def _get_pipeline_runs_query():
    """Used to prepare filters for querying Pipeline Run"""
    pipeline_runs = _get_pipeline_runs_basic_query()

    start_filter = request.args.get("start")
    if start_filter:
        start_filter = datetime.datetime.strptime(start_filter, "%Y-%m-%d")

    end_filter = request.args.get("end")
    if end_filter:
        end_filter = datetime.datetime.strptime(end_filter, "%Y-%m-%d")

    if start_filter and end_filter:
        pipeline_runs = pipeline_runs.filter(
            and_(
                PipelineRun.start <= end_filter,
                PipelineRun.finish >= start_filter,
            )
        )
    elif start_filter:
        pipeline_runs = pipeline_runs.filter(PipelineRun.finish >= start_filter)
    elif end_filter:
        pipeline_runs = pipeline_runs.filter(PipelineRun.start <= end_filter)
    return pipeline_runs


class PipelineRunListApi(Resource):
    """REST API for listing class PipelineRun"""

    def __init__(self):
        self.human_name = gettext("PipelineRun")

    @requires_auth
    def get(self):
        """
        Retrieve a list of instances of class PipelineRun.

        :return: A JSON object containing the list of PipelineRun instances data.
        :rtype: dict
        """
        if request.args.get("fields"):
            only = [f.strip() for f in request.args.get("fields").split(",")]
        else:
            only = (
                ("id",)
                if request.args.get("simple", "false") == "true"
                else None
            )
        pipeline_runs = _get_pipeline_runs_query()

        pipelines_filter = request.args.get("pipelines")
        if pipelines_filter:
            pipeline_ids = [int(x) for x in pipelines_filter.split(",")]
            pipeline_runs = pipeline_runs.filter(
                PipelineRun.pipeline_id.in_(pipeline_ids)
            )

        latest_filter = request.args.get("latest")

        if latest_filter in ("true", 1, "True", "1"):
            subquery = db.session.query(
                PipelineRun.pipeline_id,
                func.max(PipelineRun.start).label("max_start"),
            )
            if pipelines_filter:
                subquery = subquery.filter(
                    PipelineRun.pipeline_id.in_(pipeline_ids)
                )
            subquery = subquery.group_by(PipelineRun.pipeline_id).subquery()

            pipeline_runs = pipeline_runs.join(
                subquery,
                and_(
                    PipelineRun.pipeline_id == subquery.c.pipeline_id,
                    PipelineRun.start == subquery.c.max_start,
                ),
            ).order_by(PipelineRun.pipeline_id)
            print(str(pipeline_runs))
            result = PipelineRunListResponseSchema(many=True, only=only).dump(
                pipeline_runs.all()
            )
        else:
            sort = request.args.get("sort", "id")
            if sort not in ["pipeline_id", "id", "updated", "start"]:
                sort = "id"
            sort_option = getattr(PipelineRun, sort)
            if request.args.get("asc", "true") == "false":
                sort_option = sort_option.desc()

            pipeline_runs = pipeline_runs.order_by(sort_option)
            page = request.args.get("page", default=1, type=int)
            page_size = request.args.get("size", default=20, type=int)

            pagination = pipeline_runs.paginate(page, page_size, True)
            result = {
                "data": PipelineRunListResponseSchema(many=True, only=only).dump(
                    pagination.items
                ),
                "pagination": {
                    "page": page,
                    "size": page_size,
                    "total": pagination.total,
                    "pages": int(math.ceil(1.0 * pagination.total / page_size)),
                },
            }

        if log.isEnabledFor(logging.DEBUG):
            log.debug(gettext("Listing %(name)s", name=self.human_name))
        return result

    @requires_auth
    def post(self):
        """
        Add a single instance of class PipelineRun.

        :return: A JSON object containing a success message.
        :rtype: dict
        """
        result = {
            "status": "ERROR",
            "message": gettext("Missing json in the request body"),
        }
        return_code = HTTPStatus.BAD_REQUEST

        if request.json is not None:
            request_schema = PipelineRunCreateRequestSchema()
            response_schema = PipelineRunItemResponseSchema()
            pipeline_run = request_schema.load(request.json)

            if log.isEnabledFor(logging.DEBUG):
                log.debug(gettext("Adding %s"), self.human_name)
            pipeline_run = pipeline_run
            db.session.add(pipeline_run)
            db.session.commit()
            result = response_schema.dump(pipeline_run)
            return_code = HTTPStatus.CREATED
        return result, return_code


class PipelineRunDetailApi(Resource):
    """REST API for a single instance of class PipelineRun"""

    def __init__(self):
        self.human_name = gettext("PipelineRun")

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
            log.debug(
                gettext("Retrieving %s (id=%s)"),
                self.human_name,
                pipeline_run_id,
            )

        pipeline_run = PipelineRun.query.get(pipeline_run_id)
        return_code = HTTPStatus.OK
        if pipeline_run is not None:
            result = {
                "status": "OK",
                "data": [PipelineRunItemResponseSchema().dump(pipeline_run)],
            }
        else:
            return_code = HTTPStatus.NOT_FOUND
            result = {
                "status": "ERROR",
                "message": gettext(
                    "%(name)s not found (id=%(id)s)",
                    name=self.human_name,
                    id=pipeline_run_id,
                ),
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
            log.debug(
                gettext("Deleting %s (id=%s)"), self.human_name, pipeline_run_id
            )
        pipeline = PipelineRun.query.get(pipeline_run_id)
        if pipeline is not None:
            db.session.delete(pipeline)
            db.session.commit()
            result = {
                "status": "OK",
                "message": gettext(
                    "%(name)s deleted with success!", name=self.human_name
                ),
            }
        else:
            return_code = HTTPStatus.NOT_FOUND
            result = {
                "status": "ERROR",
                "message": gettext(
                    "%(name)s not found (id=%(id)s).",
                    name=self.human_name,
                    id=pipeline_run_id,
                ),
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
        result = {"status": "ERROR", "message": gettext("Insufficient data.")}
        return_code = HTTPStatus.NOT_FOUND

        if log.isEnabledFor(logging.DEBUG):
            log.debug(
                gettext("Updating %s (id=%s)"), self.human_name, pipeline_run_id
            )
        if request.json:
            request_schema = partial_schema_factory(
                PipelineRunCreateRequestSchema
            )
            response_schema = PipelineRunItemResponseSchema()
            # Ignore missing fields to allow partial updates

            data = request.json
            data["user_id"] = flask_g.user.id
            data["user_login"] = flask_g.user.login
            data["user_name"] = flask_g.user.name

            pipeline_run = request_schema.load(data, partial=True)
            pipeline_run.id = pipeline_run_id
            pipeline_run = db.session.merge(pipeline_run)

            db.session.commit()

            if pipeline_run is not None:
                return_code = HTTPStatus.OK
                result = {
                    "status": "OK",
                    "message": gettext(
                        "%(n)s (id=%(id)s) was updated with success!",
                        n=self.human_name,
                        id=pipeline_run_id,
                    ),
                    "data": [response_schema.dump(pipeline_run)],
                }
        return result, return_code


class CreatePipelineRunSchema(Schema):
    id = fields.Integer(required=True)
    start = fields.DateTime(required=True)
    finish = fields.DateTime(required=True)


class PipelineRunFromPipelineApi(Resource):
    """REST API for creating a pipeline run from pipeline"""

    @requires_auth
    def post(self):
        if (
            request.content_type == "application/json"
            and request.json is not None
        ):
            params = CreatePipelineRunSchema().load(request.json)
            config = current_app.config["STAND_CONFIG"]
            pipeline, _ = get_pipeline_from_api(
                config.get("services").get("tahiti"), params.get("id")
            )
            run = create_pipeline_run_from_pipeline(
                pipeline, Period(params.get("start"), params.get("finish"))
            )
            return {
                "status": "OK",
                "message": gettext(
                    "%(name)s created with success!",
                    name=gettext("Pipeline Run"),
                ),
                "id": run.id,
            }, 200
        else:
            return {
                "status": "ERROR",
                "message": "Unsupported content type",
            }, 415


class PipelineRunSummaryApi(Resource):
    """REST API to generate summary of pipeline run.
    Used for reporting"""

    @requires_auth
    def get(self):
        result = []
        pipeline_runs = _get_pipeline_runs_query()
        if request.args.get("type") == "line":
            pipeline_runs = _get_pipeline_runs_basic_query()
            start_filter = request.args.get("start")
            end_filter = request.args.get("end")

            if start_filter:
                start_date = datetime.datetime.strptime(start_filter, "%Y-%m-%d")
            else:
                start_date = datetime.datetime.now() - datetime.timedelta(
                    days=30
                )
            if end_filter:
                end_date = datetime.datetime.strptime(end_filter, "%Y-%m-%d")
            else:
                end_date = datetime.datetime.now()

            # Subquery for filtered PipelineRuns
            filtered_pipeline_runs = pipeline_runs.subquery()

            # Main query
            query = (
                Job.query
                .join(PipelineStepRun, PipelineStepRun.id == Job.pipeline_run_id)
                .join(
                    filtered_pipeline_runs,
                    filtered_pipeline_runs.c.id
                    == PipelineStepRun.pipeline_run_id,
                )
                .filter(and_(Job.started < end_date, Job.finished > start_date))
                .order_by(Job.started)
            )
            jobs = query.all()
            result = []
            if (jobs):
                min_start_time = jobs[0].started
                max_end_time = jobs[-1].started

            # Generates a list of intervals
            time_intervals = []
            current_time = min_start_time
            while current_time <= max_end_time:
                time_intervals.append(current_time)
                current_time += datetime.timedelta(minutes=60)

            time_series = {time: 0 for time in time_intervals}
            for job in jobs:
                start = job.started
                end = job.finished

                for time_point in time_intervals:
                    if start <= time_point < end:
                        time_series[time_point] += 1

            # Preparando os dados para o plotly
            x_values = list(time_series.keys())
            y_values = list(time_series.values())

            result = {'x': x_values, 'y': y_values}
        else:
            result = [
                (status, total)
                for status, total in pipeline_runs.with_entities(
                    PipelineRun.status, func.count(PipelineRun.id)
                )
                .group_by(PipelineRun.status)
                .all()
            ]

        return result


class ExecutePipelineRunStepApi(Resource):
    """REST API to execute a pipeline run step"""

    class ExecutePipelineRunSchema(Schema):
        id = fields.Integer(required=True)
        variables = fields.String(required=False)

    @requires_auth
    def post(self):
        if (
            request.content_type == "application/json"
            and request.json is not None
        ):
            params = self.ExecutePipelineRunSchema().load(request.json)
            pipeline_run, job = execute_pipeline_step_run(
                current_app.config['STAND_CONFIG'].get('services').get('tahiti'),
                params.get("id"), flask_g.user
            )
            if pipeline_run is not None:
                response_schema = PipelineStepRunItemResponseSchema()
                return {
                    "status": "OK",
                    "message": gettext(
                        "Pipeline step id={} triggered by the job {}.").format(
                        pipeline_run.id,
                        job.id,
                    ),
                    "id": response_schema.dump(pipeline_run),
                }, 200
            else:
                return {
                    "status": "ERROR",
                    "message": gettext("Not found"),
                }, 404
        else:
            return {
                "status": "ERROR",
                "message": gettext("Unsupported content type"),
            }, 415
