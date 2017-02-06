import json
import logging

import os
import socketio
from flask import Flask
from flask_admin import Admin
from flask_babel import Babel
from flask_caching import Cache
from flask_cors import CORS
from flask_redis import FlaskRedis
from flask_restful import Api
from mockredis import MockRedis
from stand.cluster_api import ClusterDetailApi
from stand.cluster_api import ClusterListApi
from stand.configuration import stand_configuration
from stand.job_api import JobListApi, JobDetailApi, \
    JobStopActionApi, JobLockActionApi, JobUnlockActionApi
from stand.models import db
from stand.services.redis_service import connect_redis_store


class MockRedisWrapper(MockRedis):
    """
    A wrapper to add the `from_url` classmethod
    """

    @classmethod
    def from_url(cls, *args, **kwargs):
        return cls()


def create_app(settings_override=None, log_level=logging.DEBUG):
    app = Flask(__name__)

    app.config["RESTFUL_JSON"] = {
        'cls': app.json_encoder,
        'sort_keys': False,
    }
    app.secret_key = 'l3m0n4d1'
    config = stand_configuration
    app.config['STAND_CONFIG'] = config['stand']

    server_config = config['stand'].get('servers', {})
    app.config['SQLALCHEMY_DATABASE_URI'] = server_config.get('database_url')
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

    app.config['REDIS_URL'] = server_config.get('redis_url')
    app.config.update(config.get('config', {}))
    app.debug = config['stand'].get('debug', False)

    if settings_override:
        app.config.update(settings_override)

    db.init_app(app)
    with app.app_context():
        db.create_all()

    # Flask Admin
    admin = Admin(app, name='Stand', template_mode='bootstrap3')

    # Logging configuration
    logging.basicConfig()
    logging.getLogger('sqlalchemy.engine').setLevel(log_level)
    logging.getLogger('werkzeug').setLevel(log_level)

    # CORS configuration
    CORS(app, resources={r"/*": {"origins": "*"}})

    # API configuration
    api = Api(app)
    mappings = {
        '/jobs': JobListApi,
        '/jobs/<int:job_id>': JobDetailApi,
        '/jobs/<int:job_id>/stop': JobStopActionApi,
        '/jobs/<int:job_id>/lock': JobLockActionApi,
        '/jobs/<int:job_id>/unlock': JobUnlockActionApi,
        '/clusters': ClusterListApi,
        '/clusters/<int:cluster_id>': ClusterDetailApi,
    }
    for path, view in mappings.iteritems():
        api.add_resource(view, path)

    # Cache configuration for API
    app.config['CACHE_TYPE'] = 'simple'
    cache = Cache(config={'CACHE_TYPE': 'simple'})
    cache.init_app(app)

    return app


def create_socket_io_app(_app):
    """
    Creates websocket app
    :param _app: Flask app
    """
    socket_io_config = _app.config['STAND_CONFIG']['servers']
    mgr = socketio.RedisManager(socket_io_config['redis_url'], 'job_output')
    sio = socketio.Server(engineio_options={'logger': True},
                          client_manager=mgr,
                          allow_upgrades=True)
    return sio, socketio.Middleware(sio, _app)


def create_babel_i18n(_app):
    """ i18n configuration
    :param _app: Flask app
    :return: Babel configuration
    """
    return Babel(_app)


def create_redis_store(_app):
    """
    Redis store. Used to control queues and subscription services
    :param _app: Flask app
    :return: redis_store instance (wrapper around pyredis)
    """
    redis_store = connect_redis_store(_app.config['REDIS_URL'], False)
    redis_store.init_app(_app)

    return redis_store


def create_services(_app):
    pass
