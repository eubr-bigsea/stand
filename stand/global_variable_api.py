# -*- coding: utf-8 -*-}
import logging

import rq
import math
from flask import request, current_app
from flask_babel import gettext
from flask_restful import Resource
from marshmallow import ValidationError
from rq.job import Job as RQJob
from stand.app_auth import requires_auth
from stand.models import db, GlobalVariable
from stand.schema import (
    GlobalVariableCreateRequestSchema,
    GlobalVariableItemResponseSchema,
    GlobalVariableListResponseSchema,
    partial_schema_factory,
    translate_validation,
)
from stand.services.redis_service import connect_redis_store

log = logging.getLogger(__name__)


class GlobalVariableListApi(Resource):
    """REST API for listing class GlobalVariable"""

    def __init__(self):
        self.human_name = gettext("GlobalVariable")

    @requires_auth
    def get(self):
        if request.args.get("fields"):
            valid = GlobalVariableListResponseSchema._declared_fields.keys()
            only = [
                f.strip()
                for f in request.args.get("fields").split(",")
                if f in valid
            ]
        elif request.args.get("simple", "false") == "true":
            only = ("id", "name")
        else:
            only = (
                "id",
                "name",
                "enabled",
                "description",
            )
        enabled_filter = request.args.get("enabled")
        if enabled_filter:
            global_vars = GlobalVariable.query.filter(
                GlobalVariable.enabled == (enabled_filter != "false")
            )
        else:
            global_vars = GlobalVariable.query

        q = request.args.get("query")
        if q:
            global_vars = global_vars.filter(GlobalVariable.name.like("%" + q + "%"))

        sort = request.args.get("sort", "name")
        if sort not in ["type", "id", "name"]:
            sort = "id"
        sort_option = getattr(GlobalVariable, sort)
        if request.args.get("asc", "true") == "false":
            sort_option = sort_option.desc()

        global_vars = global_vars.order_by(sort_option)
        page = request.args.get("page") or "1"
        if page is not None and page.isdigit():
            page_size = int(request.args.get("size", 20))
            page = int(page)
            pagination = global_vars.paginate(page, page_size, True)
            result = {
                "data": GlobalVariableListResponseSchema(many=True, only=only).dump(
                    pagination.items
                ),
                "pagination": {
                    "page": page,
                    "size": page_size,
                    "total": pagination.total,
                    "pages": int(math.ceil(1.0 * pagination.total / page_size)),
                },
            }
        else:
            result = {
                "data": GlobalVariableListResponseSchema(many=True, only=only).dump(
                    global_vars
                )
            }

        if log.isEnabledFor(logging.DEBUG):
            log.debug(gettext("Listing %(name)s", name=self.human_name))
        return result

    @staticmethod
    @requires_auth
    def post():
        result, result_code = (
            dict(
                status="ERROR",
                message=gettext("Missing json in the request body"),
            ),
            400,
        )
        if request.json is not None:
            request_schema = GlobalVariableCreateRequestSchema()
            response_schema = GlobalVariableItemResponseSchema()
            try:
                global_var = request_schema.load(request.json)
                db.session.add(global_var)
                db.session.commit()
                result, result_code = response_schema.dump(global_var), 201
            except ValidationError as e:
                result = {
                    "status": "ERROR",
                    "message": gettext("Validation error"),
                    "errors": translate_validation(e.messages),
                }
            except Exception as e:
                log.exception("Error in POST")
                result, result_code = (
                    dict(status="ERROR", message=gettext("Internal error")),
                    500,
                )
                if current_app.debug:
                    result["debug_detail"] = str(e)
                db.session.rollback()

        return result, result_code


class GlobalVariableDetailApi(Resource):
    """REST API for a single instance of class GlobalVariable"""

    human_name = "GlobalVariable"

    @staticmethod
    @requires_auth
    def get(global_var_id):
        global_var = GlobalVariable.query.get(global_var_id)
        if global_var is not None:
            return {
                "data": [GlobalVariableItemResponseSchema().dump(global_var)],
                "status": "OK",
            }
        else:
            return dict(status="ERROR", message=gettext("Not found")), 404

    @requires_auth
    def delete(self, global_var_id):
        return_code = 204
        if log.isEnabledFor(logging.DEBUG):
            log.debug(
                gettext("Deleting %s (id=%s)"), self.human_name, global_var_id
            )
        global_var = GlobalVariable.query.get(global_var_id)
        if global_var is not None:
            try:
                global_var.enabled = False
                db.session.delete(global_var)
                #db.session.merge(global_var)
                db.session.commit()
                result = {
                    "status": "OK",
                    "message": gettext(
                        "%(name)s deleted with success!", name=self.human_name
                    ),
                }
            except Exception as e:
                log.exception("Error in DELETE")
                result = {
                    "status": "ERROR",
                    "message": gettext("Internal error"),
                }
                return_code = 500
                if current_app.debug:
                    result["debug_detail"] = str(e)
                db.session.rollback()
        else:
            return_code = 404
            result = {
                "status": "ERROR",
                "message": gettext(
                    "%(name)s not found (id=%(id)s).",
                    name=self.human_name,
                    id=global_var_id,
                ),
            }
        return result, return_code

    @requires_auth
    def patch(self, global_var_id):
        result = {"status": "ERROR", "message": gettext("Insufficient data.")}
        return_code = 400
        if log.isEnabledFor(logging.DEBUG):
            log.debug(
                gettext("Updating %s (id=%s)"), self.human_name, global_var_id
            )
        if request.json:
            request_schema = partial_schema_factory(GlobalVariableCreateRequestSchema)

            # Ignore missing fields to allow partial updates
            global_var = request_schema.load(request.json, partial=True)
            response_schema = GlobalVariableItemResponseSchema()
            try:
                global_var.id = global_var_id
                global_var = db.session.merge(global_var)
                db.session.commit()
                if global_var is not None:
                    return_code = 200
                    result = {
                        "status": "OK",
                        "message": gettext(
                            "%(n)s (id=%(id)s) was updated with success!",
                            n=self.human_name,
                            id=global_var_id,
                        ),
                        "data": [response_schema.dump(global_var)],
                    }
            except ValidationError as e:
                result = {
                    "status": "ERROR",
                    "message": gettext("Validation error"),
                    "errors": translate_validation(e.messages),
                }
            except Exception as e:
                log.exception("Error in PATCH")
                result = {
                    "status": "ERROR",
                    "message": gettext("Internal error"),
                }
                return_code = 500
                if current_app.debug:
                    result["debug_detail"] = str(e)
                db.session.rollback()
        return result, return_code

