import logging

import socketio
from flask import Flask
from flask_admin import Admin
from flask_babel import Babel
from flask_cors import CORS
from flask_restful import Api
from flask_cache import Cache
from stand.app import simple_page
from stand.cluster_api import ClusterDetailApi
from stand.cluster_api import ClusterListApi
from stand.job_api import JobListApi, JobDetailApi
from stand.models import db


def create_app(config, container=False):
    app = Flask(__name__)

    app.config["RESTFUL_JSON"] = {
        'cls': app.json_encoder,
        'sort_keys': False,
    }
    app.secret_key = 'l3m0n4d1'
    server_config = config.get('servers', {})
    app.config['SQLALCHEMY_DATABASE_URI'] = server_config.get('database_url')
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

    # i18n configuration
    babel = Babel(app)

    # Web socket
    socket_io_config = config['socket_id']
    mgr = socketio.RedisManager(socket_io_config['queue_url'], 'discovery')
    sio = socketio.Server(engineio_options={'logger': True},
                          client_manager=mgr,
                          allow_upgrades=True)
    socketio_app = socketio.Middleware(sio, app)

    # Flask Admin
    admin = Admin(app, name='Stand', template_mode='bootstrap3')

    # Logging configuration
    logging.basicConfig()
    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
    logging.getLogger('werkzeug').setLevel(logging.DEBUG)

    # CORS configuration
    CORS(app, resources={r"/*": {"origins": "*"}})

    # API configuration
    api = Api(app)
    mappings = {
        '/jobs': JobListApi,
        '/jobs/<int:job_id>': JobDetailApi,
        '/clusters': ClusterListApi,
        '/clusters/<int:cluster_id>': ClusterDetailApi,
    }
    for path, view in mappings.iteritems():
        api.add_resource(view, path)

    app.register_blueprint(simple_page)

    # Cache configuration for API
    app.config['CACHE_TYPE'] = 'simple'
    cache = Cache(config={'CACHE_TYPE': 'simple'})
    cache.init_app(app)

    return True, app, socketio_app
