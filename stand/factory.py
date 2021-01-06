# -*- coding: utf-8 -*-
import datetime
import json
import logging
import logging.config
import rq_dashboard

import os
import rq
import socketio
from flask import Flask
from flask_admin import Admin
from flask_babel import Babel, gettext
from flask_caching import Cache
from flask_cors import CORS
from flask_restful import Api
from mockredis import MockRedis
from sqlalchemy import and_
from stand.cluster_api import ClusterDetailApi, PerformanceModelEstimationApi
from stand.cluster_api import ClusterListApi
from stand.job_api import JobListApi, JobDetailApi, \
    JobStopActionApi, JobLockActionApi, JobUnlockActionApi, \
    UpdateJobStatusActionApi, UpdateJobStepStatusActionApi, \
    JobSampleActionApi, JobSourceCodeApi, LatestJobDetailApi,\
    PerformanceModelEstimationApi, PerformanceModelEstimationResultApi, \
    DataSourceInitializationApi, WorkflowStartActionApi, WorkflowSourceCodeApi,\
    WorkflowSourceCodeResultApi

from stand.gateway_api import MetricListApi
from stand.models import db, Job, JobStep, JobStepLog, StatusExecution, \
    JobResult
from stand.services.redis_service import connect_redis_store

SEED_QUEUE_NAME = 'seed'
SEED_METRIC_JOB_NAME = 'seed.jobs.metric_probe_updater'


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

    locales_path = os.path.join(os.path.dirname(__file__), 'i18n',
                                'locales')
    app.config['BABEL_TRANSLATION_DIRECTORIES'] = locales_path
    app.config['BABEL_DEFAULT_LOCALE'] = 'en'

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
        '/jobs/latest': LatestJobDetailApi,
        '/jobs/<int:job_id>': JobDetailApi,
        '/jobs/<int:job_id>/source-code': JobSourceCodeApi,
        '/jobs/<int:job_id>/stop': JobStopActionApi,
        '/jobs/<int:job_id>/lock': JobLockActionApi,
        '/jobs/<int:job_id>/unlock': JobUnlockActionApi,
        '/jobs/<int:job_id>/status': UpdateJobStatusActionApi,
        '/jobs/<int:job_id>/<task_id>/status': UpdateJobStepStatusActionApi,
        '/jobs/<int:job_id>/sample/<task_id>': JobSampleActionApi,
        '/clusters': ClusterListApi,
        '/clusters/<int:cluster_id>': ClusterDetailApi,
        '/performance/<int:model_id>': PerformanceModelEstimationApi,
        '/performance/result/<key>': PerformanceModelEstimationResultApi,
        '/datasource/init': DataSourceInitializationApi,
        '/metric': MetricListApi,
        '/workflow': WorkflowStartActionApi,
        '/workflow/source-code': WorkflowSourceCodeApi,
        '/workflow/source-code/<key>': WorkflowSourceCodeResultApi,
    }
    for path, view in mappings.items():
        api.add_resource(view, path)

    # Cache configuration for API
    app.config['CACHE_TYPE'] = 'simple'
    cache = Cache(config={'CACHE_TYPE': 'simple'})
    cache.init_app(app)

    app.config.from_object(rq_dashboard.default_settings)
    app.register_blueprint(rq_dashboard.blueprint, url_prefix="/rq")

    return app


def mocked_emit(original_emit, app_):
    """
    Updates database with new statuses
    """

    redis_store_ = create_redis_store(app_)

    def new_emit(self, event, data, namespace, room=None, skip_sid=None,
                 callback=None):
        use_callback = callback

        if room and room.isdigit():
            use_callback = handle_emit(data, event, namespace, room, self,
                                       skip_sid, use_callback, redis_store_)
            if isinstance(data.get('message', ''), bytes):
                data['message'] = str(data['message'], 'utf-8')
            redis_store_.rpush('cache_room_{}'.format(room), json.dumps(
                {'event': event, 'data': data, 'namespace': namespace,
                 'room': room}))
        return original_emit(self, event, data, namespace, room=room,
                             skip_sid=skip_sid,
                             callback=use_callback)

    def _gettext(title):
        with app_.request_context(
                {'wsgi.url_scheme': "", 'SERVER_PORT': "", 'SERVER_NAME': "",
                 'REQUEST_METHOD': ""}):
            return gettext(title)

    def handle_emit(data, event, namespace, room, self, skip_sid, use_callback,
                    redis_store):
        logger = logging.getLogger(__name__)
        print(('-' * 40))
        print((data, event, namespace, room, self, skip_sid, use_callback,
              redis_store_))
        print(('-' * 40))
        try:
            now = datetime.datetime.now().strftime(
                '%Y-%m-%dT%H:%m:%S')
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
                        job.exception_stack = data.get('exception_stack')
                        if job.status in final_states:
                            job.finished = datetime.datetime.utcnow()
                            data['finished'] = job.finished.strftime(
                                '%Y-%m-%dT%H:%m:%S')

                        if job.status == StatusExecution.ERROR:
                            for job_step in job.steps:
                                level = data.get('level', 'ERROR')

                                step_log = JobStepLog(
                                    level=level, date=datetime.datetime.now(),
                                    status=job_step.status,
                                    type=data.get('type', 'TEXT'),
                                    message=_gettext('Canceled by error'))

                                job_step.logs.append(step_log)
                                db.session.add(job_step)
                                db.session.commit()

                                msg = {
                                    'type': data.get('type', 'TEXT') or 'TEXT',
                                    'task': {'id': job_step.task_id},
                                    'id': step_log.id,
                                    'level': level,
                                    'date': now,
                                }
                                cache = False
                                if job_step.status == StatusExecution.RUNNING:
                                    job_step.status = StatusExecution.ERROR
                                    msg['message'] = _gettext(
                                        'Canceled by error')
                                    msg['status'] = StatusExecution.ERROR
                                    original_emit(self, 'update task', msg,
                                                  namespace, room, skip_sid,
                                                  use_callback)
                                    cache = True
                                elif job_step.status not in final_states:
                                    job_step.status = StatusExecution.CANCELED
                                    msg['message'] = _gettext('Skiped by error')
                                    msg['status'] = StatusExecution.CANCELED

                                    original_emit(self, 'update task', msg,
                                                  namespace, room, skip_sid,
                                                  use_callback)
                                    cache = True
                                if cache:
                                    redis_store.rpush(
                                        'cache_room_{}'.format(room),
                                        json.dumps({'event': 'update task',
                                                    'data': msg,
                                                    'namespace': namespace,
                                                    'room': room}))
                                db.session.add(job_step)

                        elif job.status == StatusExecution.CANCELED:
                            for job_step in job.steps:
                                if job_step.status not in final_states:
                                    job_step.status = StatusExecution.CANCELED
                                db.session.add(job_step)

                        db.session.add(job)
                        db.session.commit()
                elif event == 'update task':
                    job_id = int(room)
                    job_step = JobStep.query.filter(and_(
                        JobStep.job_id == job_id,
                        JobStep.task_id == data.get('id'))).first()
                    # print('=' * 20)
                    # print(data)
                    # print(job_step)
                    # print('=' * 20)
                    if job_step is not None:
                        job_step.status = data.get('status')
                        level = data.get('level')
                        if level is None:
                            if job_step.status == StatusExecution.ERROR:
                                level = 'WARN'
                            else:
                                level = 'INFO'
                        data['date'] = now
                        step_log = JobStepLog(
                            level=level, date=datetime.datetime.now(),
                            status=job_step.status,
                            type=data.get('type', 'TEXT'),
                            message=data.get('message',
                                             data.get('msg',
                                                      'no message')))
                        job_step.logs.append(step_log)
                        if data.get('type') != 'SILENT':
                            db.session.add(job_step)
                            db.session.commit()
                        data['type'] = data.get('type', 'TEXT') or 'TEXT'
                        data['id'] = step_log.id
                        data['level'] = level
                        data['task'] = {'id': job_step.task_id}
                elif event == 'task result':
                    job_id = int(room)
                    job = Job.query.get(job_id)
                    if job is not None:
                        task_id = data.get('id')
                        op_id = data.get('operation_id')
                        result = JobResult(
                            task_id=task_id,
                            operation_id=op_id,
                            type=data.get('type'),
                            title=data.get('title'),
                            content=data.get('message'))
                        job.results.append(result)
                        db.session.add(job)
                        db.session.commit()
                        if 'operation_id' in data:
                            del data['operation_id']
                        data['time'] = datetime.datetime.now().isoformat()
                        data['id'] = result.id
                        data['task'] = {'id': task_id}
                        data['message'] = json.loads(data['message'])

                        # If metric, post again to be read by metric agent
                        if data.get('type') == 'METRIC':
                            q = rq.Queue(name=SEED_QUEUE_NAME,
                                         connection=redis_store)
                            data['content'] = json.loads(data['content'])
                            rq_job = q.enqueue(SEED_METRIC_JOB_NAME, data)
                            logger.info('Scheduled job for metric %s', rq_job)
        except Exception as ex:
            logger.exception(ex)
        return use_callback

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
                          cors_allowed_origins='*',
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
