# -*- coding: utf-8 -*-
import datetime
import json
import logging
from gettext import gettext

import requests
import rq
import stand.util
from rq.exceptions import NoSuchJobError
from stand.models import db, StatusExecution, JobException, Job, JobType
from stand.services.redis_service import connect_redis_store
from stand.schema import ClusterItemResponseSchema

logging.basicConfig(
    format=('[%(levelname)s] %(asctime)s,%(msecs)05.1f '
            '(%(funcName)s:%(lineno)s) %(message)s'),
    datefmt='%H:%M:%S')
log = logging.getLogger()
log.setLevel(logging.DEBUG)


class JobService:
    def __init__(self, session, config):
        self.config = config
        self.log = logging.getLogger(__name__)
        self.session = session

    def validate(self, job):
        """
        Validate the job using other services:
        Tahiti: Does the associated workflow exist?
        """
        # @FIXME: To implement!
        result = (True, '')
        tahiti_config = self.config.get('services', {}).get('tahiti', {})
        if all(['url' in tahiti_config, 'token' in tahiti_config]):
            service_url = '{}/{}'.format(tahiti_config['url'],
                                         job['workflow_id'])
            r = requests.get('url',
                             headers={'X-Auth-Token': tahiti_config['token']})
            if r.status_code == 200:
                pass  # OK
            else:
                result = (False, r.text)
                self.log.error('Error %d in tahiti: %s', r.status_code, r.text)
        else:
            msg = 'Invalid configuration (tahiti server address)'
            result = (False, msg)
            self.log.error(msg)

        return result

    @staticmethod
    def save(job):
        """Save a job and schedule its execution in Juicer """

        db.session.add(job)
        db.session.commit()

    @staticmethod
    def start(job, workflow, app_configs=None, job_type=None):
        if app_configs is None:
            app_configs = {}
        invalid_statuses = [StatusExecution.RUNNING, StatusExecution.PENDING,
                            StatusExecution.INTERRUPTED,
                            StatusExecution.WAITING]

        # @FIXME Validate workflow

        # Validate if workflow is already running
        # jobs_running = Job.query.filter(Job.status.in_(invalid_statuses)) \
        #     .filter(Job.workflow_id == job.workflow_id).first()
        # if False and jobs_running:
        #     raise JobException(
        #         'Workflow is already being run by another job ({})'.format(
        #             jobs_running.id), JobException.ALREADY_RUNNING)

        # Initial job status must be WAITING
        job.status = StatusExecution.WAITING

        if job_type == JobType.BATCH:
            job.type = JobType.BATCH
        elif  workflow.get('publishing_status') in ['EDITING', 'PUBLISHED']:
            job.type = JobType.APP

        job.started = datetime.datetime.utcnow()
        job.status_text = gettext('Job is allocating computer resources. '
                                  'Please wait')

        # Limit the name of a job
        job.name = job.name[:50]
        db.session.add(job)
        db.session.flush()  # Flush is needed to get the value of job.id

        redis_store = connect_redis_store(None, testing=False)
        # This queue is used to keep the order of execution and to know
        # what is pending.

        # @FIXME Each workflow has only one app. In future, we may support N
        if not job.cluster.enabled:
            raise JobException(
                'Cluster {} is not enabled.'.format(job.cluster.name),
                JobException.CLUSTER_DISABLED)

        cluster_properties = ['id', 'type', 'address', 'executors',
                              'executor_cores', 'executor_memory',
                              'auth_token', 'general_parameters']
        cluster_info = {}
        for p in cluster_properties:
            cluster_info[p] = getattr(job.cluster, p)

        msg = json.dumps(dict(workflow_id=job.workflow_id,
                              app_id=job.workflow_id,
                              job_id=job.id,
                              type='execute',
                              cluster=cluster_info,
                              app_configs=app_configs,
                              workflow=workflow))
        redis_store.rpush("queue_start", msg)

        # This hash controls the status of job. Used for prevent starting
        # jobs in invalid states
        record_wf_id = 'record_workflow_{}'.format(job.workflow_id)
        redis_store.hset(record_wf_id, 'status', job.status)

        # TTL=1h (sufficient time to other stages use the information)
        redis_store.expire(record_wf_id, time=3600)
        redis_store.expire('queue_app_{}'.format(job.workflow_id), time=3600)

        db.session.commit()

    @staticmethod
    def stop(job):
        valid_status_in_stop = [StatusExecution.WAITING,
                                StatusExecution.PENDING,
                                StatusExecution.RUNNING,
                                StatusExecution.INTERRUPTED]
        valid_end_status = [StatusExecution.COMPLETED, StatusExecution.CANCELED,
                            StatusExecution.ERROR]
        if job.status not in valid_status_in_stop + valid_end_status:
            raise JobException(
                'You cannot stop a job in the state \'{}\''.format(job.status),
                JobException.INVALID_STATE)
        if job.status in valid_status_in_stop:
            job.status = StatusExecution.CANCELED
            db.session.add(job)
            db.session.flush()

            redis_store = connect_redis_store(None, testing=False)

            # @FIXME Each workflow has only one app. In future, we may support N
            cluster = job.cluster
            msg = json.dumps(
                dict(workflow_id=job.workflow_id,
                     app_id=job.workflow_id,
                     job_id=job.id,
                     cluster= ClusterItemResponseSchema().dump(cluster).data,
                     type='terminate'))
            redis_store.rpush("queue_start", msg)

            # # This hash controls the status of job. Used for prevent starting
            # # a canceled job be started by Juicer (FIXME: is it used?).
            # redis_store.hset('record_workflow_{}'.format(job.workflow_id),
            #                  'status', StatusExecution.CANCELED)
            #
            # redis_store.hset('job_{}'.format(job.id),
            #                  'status', StatusExecution.CANCELED)

            db.session.commit()

        else:
            raise JobException(
                'You cannot stop a job in the state \'{}\''.format(job.status),
                JobException.ALREADY_FINISHED)

    @staticmethod
    def lock(job, user, computer, force=False):
        job_id = "job_{}".format(job.id)

        redis_store = connect_redis_store(None, testing=False)
        already_locked = redis_store.hget(job_id, 'lock')
        # unlocked
        if already_locked == '' or force:
            data = {
                'user': user, 'computer': computer,
                'date': stand.util.get_now().isoformat()
            }
            redis_store.hset(job_id, 'lock', json.dumps(data))
        else:
            already_locked = json.loads(already_locked)
            msg = ('Job {job} is locked by {user} '
                   '@ {computer} since {date}').format(
                job=job.id, user=already_locked['user']['name'],
                computer=already_locked['computer'],
                date=already_locked['date'])
            raise JobException(error_code=JobException.ALREADY_LOCKED,
                               message=msg)

    @staticmethod
    def get_lock_status(job):
        job_id = "job_{}".format(job.id)
        redis_store = connect_redis_store(None, testing=False)
        already_locked = redis_store.hget(job_id, 'lock')
        if already_locked != '':
            return json.loads(already_locked)
        else:
            return None

    @staticmethod
    def retrieve_sample(user, job, task_id, port_name, wait):
        # DELIVER messages request the delivery of a result (task_id)
        redis_store = connect_redis_store(None, testing=False)
        output = 'queue_delivery_app_{app_id}_{port_name}'.format(
            app_id=job.workflow_id, port_name=port_name)
        msg = json.dumps({
            'workflow_id': job.workflow_id,
            'app_id': job.workflow_id,
            'job_id': job.id,
            'type': 'deliver',
            'task_id': task_id,
            'output': output,
            'port': port_name
        })
        redis_store.rpush("queue_start", msg)
        return json.loads(redis_store.blpop(output)[1])

    @staticmethod
    def execute_performance_model(cluster_id, model_id, deadline, cores,
                                  platform,
                                  data_size, iterations, batch_size):
        redis_store = connect_redis_store(
            None, testing=False, decode_responses=False)
        q = rq.Queue('juicer', connection=redis_store)
        payload = {'model_id': model_id, 'deadline': deadline, 'cores': cores,
                   'platform': platform,
                   'data_size': data_size,
                   'iterations': iterations,
                   'batch_size': batch_size,
                   'cluster_id': cluster_id}
        log.info("Payload %s", payload)
        result = q.enqueue('juicer.jobs.estimate_time_with_performance_model',
                           payload)
        return result.id

    @staticmethod
    def get_performance_model_result(job_id):
        redis_store = connect_redis_store(
            None, testing=False, decode_responses=False)
        try:
            rq_job = rq.job.Job(job_id, connection=redis_store)
            return {'status': rq_job.result.get('status'),
                    'result': rq_job.result}
        except NoSuchJobError:
            return {'status': 'ERROR', 'message': 'Job not found'}

    @staticmethod
    def generate_code(workflow_id, template):
        redis_store = connect_redis_store(
            None, testing=False, decode_responses=False)
        q = rq.Queue('juicer', connection=redis_store)
        payload = {'workflow_id': workflow_id, 'template': template}

        log.info("Payload %s", payload)
        result = q.enqueue('juicer.jobs.code_gen.generate', workflow_id, template)
        return result.id

    @staticmethod
    def get_generate_code_result(job_id):
        redis_store = connect_redis_store(
            None, testing=False, decode_responses=False)
        try:
            rq_job = rq.job.Job(job_id, connection=redis_store)
            # return {'status': rq_job.result.get('status'),
            #        'code': rq_job.result.get('code')}
            return rq_job.result.get('code')
        except NoSuchJobError:
            return {'status': 'ERROR', 'message': 'Not found'}


