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
from flask_restful import Api, abort
from cache import cache

from job_api import JobDetailApi, JobListApi
from cluster_api import ClusterDetailApi, ClusterListApi
from models import db
from flask import Blueprint
sqlalchemy_utils.i18n.get_locale = get_locale

"""
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
"""

simple_page = Blueprint('simple_page', __name__,
                        template_folder='templates')

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



"""
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
                app.run(debug=True, host='0.0.0.0', port=3320)
            else:
                app.debug = True

        socketio_app = socketio.Middleware(sio, app)

        return True, socketio_app
    else:
        parser.print_usage()
        return False, None

# main()
"""
