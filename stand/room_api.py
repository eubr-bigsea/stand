# -*- coding: utf-8 -*-}
import logging
import socketio

from flask import request, current_app
from flask_restful import Resource
from stand.app_auth import requires_auth, requires_permission
from flask_babel import gettext

class RoomApi(Resource):

    @staticmethod
    @requires_auth
    @requires_permission('ADMINISTRATOR')
    def post():
        result, result_code = dict(
            status="ERROR",
            message=gettext('Missing json in the request body')), 400

        if request.json is not None:
            socket_io_config = current_app.config['STAND_CONFIG']['servers']
            mgr = socketio.RedisManager(socket_io_config['redis_url'], 'job_output')
            sio = socketio.Server(engineio_options={'logger': True},
                                  client_manager=mgr,
                                  cors_allowed_origins='*',
                                  allow_upgrades=True)

            sio.emit(
                event=request.json.get("event", "?"), 
                data=request.json.get("data", {}),
                room=request.json.get("room", "general"),
                namespace=request.json.get("namespace", "/stand"))
            result_code = 200
            result = {"status": "OK"}

        return result, result_code
