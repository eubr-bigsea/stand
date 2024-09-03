import datetime
import sys
from marshmallow import ValidationError
from werkzeug.exceptions import HTTPException
import json
import logging
import logging.config

import os
import socketio
from flask import Flask, g
from flask_babel import Babel, gettext
from flask_caching import Cache
from flask_cors import CORS
from flask_migrate import Migrate
from flask_restful import Api
from mockredis import MockRedis
from sqlalchemy import and_
from stand.cluster_api import ClusterDetailApi, PerformanceModelEstimationApi
from stand.cluster_api import ClusterListApi
from stand.pipeline_run_api import (PipelineRunDetailApi, PipelineRunListApi,
                                    PipelineRunFromPipelineApi,
                                    ExecutePipelineRunStepApi,
                                    PipelineRunSummaryApi,
                                    ChangePipelineRunStepApi)
from stand.room_api import RoomApi
from stand.job_api import (JobListApi, JobDetailApi,
    JobStopActionApi, JobLockActionApi, JobUnlockActionApi,
    UpdateJobStatusActionApi, UpdateJobStepStatusActionApi,
    JobSampleActionApi, JobSourceCodeApi, LatestJobDetailApi,
    PerformanceModelEstimationResultApi,
    DataSourceInitializationApi, WorkflowStartActionApi, WorkflowSourceCodeApi,
    WorkflowSourceCodeResultApi)

from stand.gateway_api import MetricListApi
from stand.models import db, Job, JobStep, JobStepLog, StatusExecution as EXEC, \
    JobResult
from stand.schema import translate_validation
from stand.services import ServiceException
from stand.services.redis_service import connect_redis_store

SEED_QUEUE_NAME = 'seed'
SEED_METRIC_JOB_NAME = 'seed.jobs.metric_probe_updater'

log = logging.getLogger(__name__)


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
    engine_config = config.get('config', {})
    if engine_config:
        final_config = {'pool_pre_ping': True}
        if 'mysql://' in app.config['SQLALCHEMY_DATABASE_URI']:
            if 'SQLALCHEMY_POOL_SIZE' in engine_config:
                final_config['pool_size'] = engine_config['SQLALCHEMY_POOL_SIZE']
            if 'SQLALCHEMY_POOL_RECYCLE' in engine_config:
                final_config['pool_recycle'] = engine_config['SQLALCHEMY_POOL_RECYCLE']
        app.config['SQLALCHEMY_ENGINE_OPTIONS'] = final_config
    app.debug = config['stand'].get('debug', False)


    # Error handlers
    @app.errorhandler(ValidationError)
    def register_validation_error(e):
        result = {'status': 'ERROR',
                  'message': gettext("Validation error"),
                  'errors': translate_validation(e.messages)}
        db.session.rollback()
        return result, 400

    @app.errorhandler(ServiceException)
    def register_service_exception(e):
        result = {'status': 'ERROR',
                  'message': str(e)}
        db.session.rollback()
        return result, 400

    @app.errorhandler(Exception)
    def handle_exception(e):
        # pass through HTTP errors
        if isinstance(e, HTTPException):
            return e
        result = {'status': 'ERROR',
                  'message': gettext("Internal error")}
        if app.debug:
            result['debug_detail'] = str(e)
        log.exception(e)
        print(e, file=sys.stderr)
        db.session.rollback()
        return result, 500

    if settings_override:
        app.config.update(settings_override)
    babel = create_babel_i18n(app)

    @babel.localeselector
    def get_locale():
        user = getattr(g, 'user', None)
        if user is not None and user.locale:
            return user.locale
        preferred = [x.replace('-', '_') for x in
                     list(request.accept_languages.values())]
        return negotiate_locale(preferred, ['pt_BR', 'en_US'])


    db.init_app(app)
    if app.testing:
        with app.app_context():
            db.create_all()

    os.chdir(os.environ.get('STAND_HOME', '.'))
    # Logging configuration
    logging.config.fileConfig('logging_config.ini')

    # logging.getLogger('sqlalchemy.engine').setLevel(log_level)
    # logging.getLogger('werkzeug').setLevel(log_level)

    # CORS configuration
    CORS(app, resources={r"/*": {"origins": "*"}})

    # Flask Migrate
    migrate = Migrate(app, db)

    # API configuration
    api = Api(app)
    mappings = {
        '/room': RoomApi,
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
        '/pipeline-runs': PipelineRunListApi,
        '/pipeline-runs/<int:pipeline_run_id>': PipelineRunDetailApi,
        '/pipeline-runs/create': PipelineRunFromPipelineApi,
        '/pipeline-runs/execute': ExecutePipelineRunStepApi,
        '/pipeline-runs/summary': PipelineRunSummaryApi,
        '/pipeline-runs/<int:pipeline_run_id>/status/<status>':
            ChangePipelineRunStepApi,
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

    # Global error handling
    @app.errorhandler(Exception)
    def handle_exception(e):
        # pass through HTTP errors
        if isinstance(e, HTTPException):
            return e

        logger = logging.getLogger(__name__)
        logger.exception(e)
        return {"error": "Internal error"}, 500

    # Cache configuration for API
    app.config['CACHE_TYPE'] = 'simple'
    cache = Cache(config={'CACHE_TYPE': 'simple'})
    cache.init_app(app)

    return app


def mocked_emit(original_emit, app_):
    """
    Updates database with new statuses
    """

    redis_store_ = create_redis_store(app_)

    def new_emit(self, event, data, namespace, room=None, skip_sid=None,
                 callback=None):
        use_callback = callback
        if room:
            use_callback = handle_emit(data, event, namespace, room, self,
                                       skip_sid, use_callback, redis_store_)
            if isinstance(data.get('message', ''), bytes):
                data['message'] = str(data['message'], 'utf-8')
            if data.get('type') == 'OBJECT':
                data['message'] = json.loads(data['message'])
            cached_data = data.copy()
            cached_data['fromcache'] = True
            redis_store_.rpush(f'cache_room_{room}', json.dumps(
                {'event': event, 'data': cached_data, 'namespace': namespace,
                 'room': room}, indent=0))
            redis_store_.expire(f'cache_room_{room}', 600)
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
        # print(('-' * 40))
        # print((data, event, namespace, room, self, skip_sid, use_callback,
        #       redis_store_))
        # print(('-' * 40))
        try:
            now = datetime.datetime.now().strftime(
                '%Y-%m-%dT%H:%m:%S')
            with app_.app_context():
                if event == 'update job':
                    status = data.get('status')
                    if status is None:
                        raise ValueError('Status not provided!')
                    job_id = int(room)
                    #logger.info(_gettext('Updating job id=%s'), job_id)
                    job: Job = Job.query.get(job_id)
                    if job is not None:
                        final_states = [EXEC.COMPLETED,
                                        EXEC.CANCELED,
                                        EXEC.ERROR]
                        job.status = status
                        logger.info(_gettext('Updating job id=%s to status %s'),
                            job_id, job.status)
                        job.status_text = data.get('msg',
                                                   data.get('message', ''))
                        job.exception_stack = data.get('exception_stack')
                        if job.status in final_states:
                            job.finished = datetime.datetime.utcnow()
                            data['finished'] = job.finished.strftime(
                                '%Y-%m-%dT%H:%m:%S')
                        # Update Job Step status
                        if job.status == EXEC.COMPLETED:
                            for job_step in job.steps:
                                if job_step.status == 'PENDING':
                                    job_step.status = 'COMPLETED'
                                    db.session.add(job_step)
                        elif job.status == EXEC.ERROR:
                            # Something went wrong, record the cause
                            for job_step in job.steps:
                                level = data.get('level', 'ERROR')

                                step_log = JobStepLog(
                                    level=level, date=datetime.datetime.now(),
                                    status=job_step.status,
                                    type=data.get('type', 'TEXT'),
                                    message=_gettext('Canceled by error'))

                                job_step.logs.append(step_log)
                                if job_id > 0:
                                    db.session.add(job_step)
                                    db.session.commit()

                                msg = {
                                    'type': data.get('type', 'TEXT') or 'TEXT',
                                    'task': {'id': job_step.task_id},
                                    'id': step_log.id,
                                    'level': level,
                                    'date': now,
                                    'room': job_id
                                }
                                # Indicate if the message should be stored for
                                # the client
                                cache = False
                                if job_step.status == EXEC.RUNNING:
                                    job_step.status = EXEC.ERROR
                                    msg['message'] = _gettext(
                                        'Canceled by error')
                                    msg['status'] = EXEC.ERROR
                                    #original_emit(self, 'update task', msg,
                                    #              namespace, room, skip_sid,
                                    #              use_callback)
                                    cache = True
                                elif job_step.status not in final_states:
                                    job_step.status = EXEC.CANCELED
                                    msg['message'] = _gettext('Skiped by error')
                                    msg['status'] = EXEC.CANCELED

                                    #original_emit(self, 'update task', msg,
                                    #              namespace, room, skip_sid,
                                    #              use_callback)
                                    cache = True
                                if cache:
                                    redis_store.rpush(
                                        f'cache_room_{room}',
                                        json.dumps({'event': 'update task',
                                                    'data': msg,
                                                    'namespace': namespace,
                                                    'room': room}))

                                if job_id > 0:
                                    db.session.add(job_step)

                        elif job.status == EXEC.CANCELED:
                            for job_step in job.steps:
                                if job_step.status not in final_states:
                                    job_step.status = EXEC.CANCELED
                                if job_id > 0:
                                    db.session.add(job_step)

                        logger.info('Is job associated to pipeline run? %s',
                                 f'Yes, {job.pipeline_step_run.id}' if
                                 job.pipeline_step_run is not None else 'No')
                        if (job.pipeline_step_run):
                            _update_pipeline_run(job)
                            notification_msg = {
                                'date': datetime.datetime.utcnow().isoformat(),
                                'pipeline_run': {
                                    'id': job.pipeline_run.id,
                                    'status': job.pipeline_run.status,
                                },
                                'pipeline_step_run': {
                                    'id': job.pipeline_step_run.id,
                                    'status': job.pipeline_step_run.status,
                                    'order': job.pipeline_step_run.order
                                },
                                'job': {
                                    'id': job.id,
                                    'status': job.status,
                                },
                                'message': data.get('message')
                            }
                            log.info('Notify room pipeline_runs: %s', notification_msg)
                            self.emit('update pipeline run',
                                            notification_msg,
                                            namespace, 'pipeline_runs', skip_sid,
                                            use_callback)
                        db.session.add(job)
                        db.session.commit()
                    else:
                        logger.info(gettext("Job %s is not persistent."), job_id)
                elif event == 'update task' or event == 'user message':
                    job_id = int(room)
                    job_step = JobStep.query.filter(and_(
                        JobStep.job_id == job_id,
                        JobStep.task_id == data.get('id'))).first()
                    # print('=' * 20)
                    # print(data)
                    # print(job_step)
                    # print(event)
                    # print('=' * 20)
                    status = data.get('status')
                    if job_step is not None:
                        job_step.status = status
                        level = data.get('level')
                        if level is None:
                            if job_step.status == EXEC.ERROR:
                                level = 'WARN'
                            else:
                                level = 'INFO'
                        data['date'] = now
                        step_log_msg = data.get('message', 'no message')
                        if isinstance(step_log_msg, dict):
                            step_log_msg = json.dumps(step_log_msg)

                        step_log_type = data.get('type', 'TEXT')
                        if event == 'user message':
                            step_log_type = 'USER'


                        # Messages must be serialized in the client
                        #if step_log_type == 'OBJECT':
                        #    step_log_msg = json.dumps(step_log_msg)

                        step_log = JobStepLog(
                            level=level, date=datetime.datetime.now(),
                            status=job_step.status,
                            type=step_log_type,
                            message=step_log_msg)
                        job_step.logs.append(step_log)
                        # If job_id <= 0, it is internal and it is not persisted
                        if data.get('type') != 'SILENT' and job_id > 0:
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

                        content = data.get('message')
                        if isinstance(content, dict):
                            content = json.dumps(content)

                        result = JobResult(
                            task_id=task_id,
                            operation_id=op_id,
                            type=data.get('type'),
                            title=data.get('title'),
                            content=content)
                        job.results.append(result)
                        if job_id > 0:
                            db.session.add(job)
                            db.session.commit()
                        # if 'operation_id' in data:
                        #     del data['operation_id']
                        # data['time'] = datetime.datetime.now().isoformat()
                        # data['id'] = result.id
                        # data['task'] = {'id': task_id}
                        # data['message'] = json.loads(data['message'])

                        # If metric, post again to be read by metric agent
                        # if data.get('type') == 'METRIC':
                        #     q = rq.Queue(name=SEED_QUEUE_NAME,
                        #                  connection=redis_store)
                        #     data['content'] = json.loads(data['content'])
                        #     rq_job = q.enqueue(SEED_METRIC_JOB_NAME, data)
                        #     logger.info('Scheduled job for metric %s', rq_job)
        except Exception as ex:
            logger.exception(ex)
        return use_callback


    return new_emit

def _update_pipeline_run(job: Job) -> None:
    """ Update associated pipeline step run, if any """

    job.pipeline_step_run.status = job.status
    if job.status in (EXEC.ERROR, EXEC.CANCELED, EXEC.INTERRUPTED):
        job.pipeline_run.status = job.status
        job.pipeline_run.final_status = job.status
    elif job.status in (EXEC.COMPLETED, ):
        # Test if the step is the last one
        step_order = job.pipeline_step_run.order
        if step_order == len(job.pipeline_run.steps):
            job.pipeline_run.status = EXEC.COMPLETED
        else:
            job.pipeline_run.status = EXEC.RUNNING #??

    elif job.status in (EXEC.PENDING, EXEC.WAITING,
                        EXEC.WAITING_INTERVENTION):
        pass # Ignore
    elif job.status in (EXEC.RUNNING, ):
        pass # FIXME

    db.session.add(job.pipeline_step_run)
    db.session.add(job.pipeline_run)


def create_socket_io_app(_app):
    """
    Creates websocket app
    :param _app: Flask app
    """
    original_emit = socketio.base_manager.BaseManager.emit
    if original_emit.__name__ != "new_emit":
        socketio.base_manager.BaseManager.emit = mocked_emit(original_emit, _app)

    socket_io_config = _app.config['STAND_CONFIG']['servers']
    mgr = socketio.RedisManager(socket_io_config['redis_url'], 'job_output')
    sio = socketio.Server(engineio_options={'logger': True},
                          client_manager=mgr,
                          cors_allowed_origins='*',
                          allow_upgrades=True)
    mgr.server = sio
    sio.manager_initialized = True
    import eventlet
    eventlet.spawn(mgr.initialize)
    log.info('Started socketio')

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
