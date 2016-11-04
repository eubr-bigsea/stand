#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import json
import logging
import socketio

import sqlalchemy_utils
from flask import Flask, request, render_template

from flask_admin import Admin
from flask_admin.contrib.sqla import ModelView
from flask_babel import get_locale, Babel
from flask_cors import CORS
from flask_restful import Api
from cache import cache

from job_api import JobDetailApi, JobListApi
from models import db

sqlalchemy_utils.i18n.get_locale = get_locale

app = Flask(__name__)
babel = Babel(app)

# Web socket
mgr = socketio.RedisManager('redis://localhost:6379/', 'discovery')
sio = socketio.Server(engineio_options={'logger': True},
                        client_manager=mgr,
                        allow_upgrades=True)

app.secret_key = 'l3m0n4d1'
# Flask Admin 
admin = Admin(app, name='Lemonade', template_mode='bootstrap3')

# CORS
CORS(app, resources={r"/*": {"origins": "*"}})
api = Api(app)

# Cache
app.config['CACHE_TYPE'] = 'simple'
cache.init_app(app)

logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
logging.getLogger('werkzeug').setLevel(logging.DEBUG)

mappings = {
    '/jobs': JobListApi,
    '/jobs/<int:job_id>': JobDetailApi,
}
for path, view in mappings.iteritems():
    api.add_resource(view, path)


# @app.before_request
def before():
    print request.args
    if request.args and 'lang' in request.args:
        if request.args['lang'] not in ('es', 'en'):
            return abort(404)


@babel.localeselector
def get_locale():
    return request.args.get('lang', 'en')

@app.route('/')
def index():
    """Serve the client-side application."""
    return render_template('index.html')

@sio.on('my event', namespace='/stand')
def test_message(sid, message):
    sio.emit('response', {'data': message['data']}, room=sid,
             namespace='/stand')


@sio.on('my broadcast event', namespace='/stand')
def test_broadcast_message(sid, message):
    sio.emit('response', {'data': message['data']}, namespace='/stand')


@sio.on('join', namespace='/stand')
def join(sid, message):
    sio.enter_room(sid, message['room'], namespace='/stand')
    sio.emit('response', {'data': 'Entered room: ' + str(message['room'])},
             room=sid, namespace='/stand')


@sio.on('leave', namespace='/stand')
def leave(sid, message):
    sio.leave_room(sid, message['room'], namespace='/stand')
    sio.emit('response', {'data': 'Left room: ' + str(message['room'])},
             room=sid, namespace='/stand')


@sio.on('close room', namespace='/stand')
def close(sid, message):
    sio.emit('response',
             {'data': 'Room ' + message['room'] + ' is closing.'},
             room=message['room'], namespace='/stand')
    sio.close_room(message['room'], namespace='/stand')


@sio.on('my room event', namespace='/stand')
def send_room_message(sid, message):
    sio.emit('response', {'data': message['data']}, room=message['room'],
             namespace='/stand')


@sio.on('disconnect request', namespace='/stand')
def disconnect_request(sid):
    sio.disconnect(sid, namespace='/stand')


@sio.on('connect', namespace='/stand')
def test_connect(sid, environ):
    sio.emit('response', {'data': 'Connected', 'count': 0}, room=sid,
             namespace='/stand')


@sio.on('disconnect', namespace='/stand')
def test_disconnect(sid):
    print('Client disconnected')


def main(container=False):
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Config file")

    args = parser.parse_args()
    if args.config:
        with open(args.config) as f:
            config = json.load(f)

        app.config["RESTFUL_JSON"] = {
            'cls': app.json_encoder, 
            'sort_keys': False, 
        }

        server_config = config.get('servers', {})
        app.config['SQLALCHEMY_DATABASE_URI'] = server_config.get(
            'database_url')
        app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
        app.config['SQLALCHEMY_POOL_SIZE'] = 10
        app.config['SQLALCHEMY_POOL_RECYCLE'] = 240

        db.init_app(app)
        with app.app_context():
            db.create_all()

        if server_config.get('environment', 'dev') == 'dev':
            if not container:
                app.run(debug=True, host='0.0.0.0')
            else:
                app.debug = True

        socketio_app = socketio.Middleware(sio, app)

        return True, socketio_app
    else:
        parser.print_usage()
        return False, None

#main()
