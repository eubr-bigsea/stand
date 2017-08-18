import datetime
import logging
import logging.config
import os
import socketio
from flask import Flask
from flask_admin import Admin
from flask_babel import Babel
from flask_caching import Cache
from flask_cors import CORS
from flask_restful import Api
from mockredis import MockRedis
from sqlalchemy import and_
from stand.cluster_api import ClusterDetailApi
from stand.cluster_api import ClusterListApi
from stand.job_api import JobListApi, JobDetailApi, \
    JobStopActionApi, JobLockActionApi, JobUnlockActionApi, \
    UpdateJobStatusActionApi, UpdateJobStepStatusActionApi, \
    JobSampleActionApi
from stand.models import db, Job, JobStep, JobStepLog, StatusExecution, \
    JobResult
from stand.services.redis_service import connect_redis_store


class MockRedisWrapper(MockRedis):
    """
    A wrapper to add the `from_url` classmethod
    """

    @classmethod
    def from_url(cls, *args, **kwargs):
        return cls()

def create_app(settings_override=None, log_level=logging.DEBUG, config_file=''):
    if config_file:
        os.environ['STAND_CONFIG'] = config_file

    from stand.configuration import stand_configuration
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
    if app.testing:
        with app.app_context():
            db.create_all()

    # Flask Admin
    admin = Admin(app, name='Stand', template_mode='bootstrap3')

    os.chdir(os.environ.get('STAND_HOME', '.'))
    # Logging configuration
    logging.config.fileConfig('logging_config.ini')

    # logging.getLogger('sqlalchemy.engine').setLevel(log_level)
    # logging.getLogger('werkzeug').setLevel(log_level)

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
        '/jobs/<int:job_id>/status': UpdateJobStatusActionApi,
        '/jobs/<int:job_id>/<task_id>/status': UpdateJobStepStatusActionApi,
        '/jobs/<int:job_id>/sample/<task_id>': JobSampleActionApi,
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


def wait_client():
    print '#' * 20 
    print 'Client ACK'
    print '#' * 20 


def mocked_emit(original_emit, app_):
    """
    Updates database with new statuses
    """

    def new_emit(self, event, data, namespace, room=None, skip_sid=None,
                 callback=None):
        use_callback = callback

        try:
            print '-' * 20
            print data, room, event, namespace
            print '-' * 20

            with app_.app_context():
                if event == 'update job':
                    job_id = int(room)
                    job = Job.query.get(job_id)
                    if job is not None:
                        final_states = [StatusExecution.COMPLETED,
                                        StatusExecution.CANCELED,
                                        StatusExecution.ERROR]
                        job.status = data.get('status')
                        job.status_text = data.get('msg',
                                                   data.get('message', ''))
                        if job.status in final_states:
                            job.finished = datetime.datetime.utcnow()
                            data['finished'] = job.finished.strftime(
                                '%Y-%m-%dT%H:%m:%S')

                        if job.status == StatusExecution.ERROR:
                            for job_step in job.steps:
                                if job_step.status == StatusExecution.RUNNING:
                                    job_step.status = StatusExecution.ERROR
                                    msg = {'id': job_step.task_id,
                                           'msg': 'Error',
                                           'status': StatusExecution.ERROR},
                                    original_emit(self, 'update task', msg,
                                                  namespace, room,
                                                  skip_sid)
                                elif job_step.status not in final_states:
                                    job_step.status = StatusExecution.CANCELED
                                    msg = {'id': job_step.task_id,
                                           'msg': 'Canceled',
                                           'status': StatusExecution.CANCELED},
                                    original_emit(self, 'update task', msg,
                                                  namespace, room,
                                                  skip_sid)
                                db.session.add(job_step)

                        elif job.status == StatusExecution.CANCELED:
                            for job_step in job.steps:
                                if job_step.status not in final_states:
                                    job_step.status = StatusExecution.CANCELED
                                db.session.add(job_step)

                        db.session.add(job)
                        db.session.commit()
                        use_callback = wait_client
                elif event == 'update task':
                    job_id = int(room)
                    job_step = JobStep.query.filter(and_(
                        JobStep.job_id == job_id,
                        JobStep.task_id == data.get('id'))).first()
                    if job_step is not None:
                        job_step.status = data.get('status')
                        if job_step.status == StatusExecution.COMPLETED:
                            level = 'INFO'
                        else:
                            level = StatusExecution.WAITING
                        data['date'] = datetime.datetime.now().strftime(
                            '%Y-%m-%dT%H:%m:%S')
                        job_step.logs.append(JobStepLog(
                            level=level, date=datetime.datetime.now(),
                            type=data.get('type', 'TEXT'),
                            message=data.get('message',
                                             data.get('msg', 'no message'))))
                        db.session.add(job_step)
                        db.session.commit()
                        use_callback = wait_client
                        data['type'] = data.get('type', 'TEXT') or 'TEXT'
                elif event == 'task result':
                    job_id = int(room)
                    job = Job.query.get(job_id)
                    if job is not None:
                        task_id = data.get('id')
                        op_id = data.get('operation_id')
                        job.results.append(JobResult(
                            task_id=task_id,
                            operation_id=op_id,
                            type=data.get('type'),
                            title=data.get('title'),
                            content=data.get('content')))
                        db.session.add(job)
                        db.session.commit()
        except Exception as ex:
            logger = logging.getLogger(__name__)
            logger.exception(ex)

        return original_emit(self, event, data, namespace, room=room,
                             skip_sid=skip_sid,
                             callback=use_callback)

    return new_emit


def create_socket_io_app(_app):
    """
    Creates websocket app
    :param _app: Flask app
    """
    original_emit = socketio.base_manager.BaseManager.emit
    socketio.base_manager.BaseManager.emit = mocked_emit(original_emit, _app)

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
