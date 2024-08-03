import datetime
import json
import logging
from gettext import gettext

import requests
import rq
import stand.util
from rq.exceptions import NoSuchJobError
from stand.models import (
    db, StatusExecution, JobException, JobType, Cluster)
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
            service_url = f"{tahiti_config['url']}/{job['workflow_id']}"
            r = requests.get(service_url,
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
    def start(job, workflow, app_configs=None, job_type=None, persist=True,
              testing=False, lang=None):
        if app_configs is None:
            app_configs = {}
        #invalid_statuses = [StatusExecution.RUNNING, StatusExecution.PENDING,
        #                    StatusExecution.INTERRUPTED,
        #                    StatusExecution.WAITING]

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
        elif workflow.get('publishing_status') in ['EDITING', 'PUBLISHED']:
            job.type = JobType.APP

        job.started = datetime.datetime.utcnow()
        job.status_text = gettext('Job is allocating computer resources. '
                                  'Please wait')

        # Limit the name of a job
        job.name = job.name[:50]
        log.info("Persistent job: %s", persist)
        if persist:
            db.session.add(job)
            db.session.flush()  # Flush is needed to get the value of job.id
        else:
            # Offset when job is not persistent. E.g. Data Explorer
            job.id = 800000 + job.workflow_id

        # Test if workflow has a variable indicating the Redis db to be used.
        # Useful when debugging a shared environment
        redis_db = next((v for v in workflow.get('variables', [])
                         if v.get('name') == 'redis_db'), None)
        if redis_db is not None:
            redis_store = JobService._get_redis_store(
                None, testing, db=int(redis_db.get('default_value')))
        else:
            redis_store = JobService._get_redis_store(None, testing)

        # This queue is used to keep the order of execution and to know
        # what is pending
        if job.cluster is None:
            preferred = workflow.get('preferred_cluster_id')
            if preferred:
                job.cluster = Cluster.query.get(preferred)

        # @FIXME Each workflow has only one app. In future, we may support N
        if job.cluster is None or not job.cluster.enabled:
            raise JobException(
                gettext('Cluster {} is not enabled.').format(job.cluster.name),
                JobException.CLUSTER_DISABLED)

        cluster_properties = ['id', 'type', 'address', 'executors',
                              'executor_cores', 'executor_memory',
                              'auth_token', 'general_parameters']
        cluster_info = {}
        for p in cluster_properties:
            cluster_info[p] = getattr(job.cluster, p)

        # Is job persisted in database? If so,
        # its generated source code must be updated by Juicer
        app_configs['persist'] = persist
        app_configs['locale'] = lang or 'pt'
        msg = json.dumps(dict(workflow_id=job.workflow_id,
                              app_id=job.workflow_id,
                              job_id=job.id,
                              job_type=job.type,
                              type='execute',
                              cluster=cluster_info,
                              app_configs=app_configs,
                              workflow=workflow))
        redis_store.rpush("queue_start", msg)

        # This hash controls the status of job. Used for prevent starting
        # jobs in invalid states
        record_wf_id = f'record_workflow_{job.workflow_id}'
        redis_store.hset(record_wf_id, 'status', job.status)

        # TTL=1h (sufficient time to other stages use the information)
        redis_store.expire(record_wf_id, time=3600)
        redis_store.expire(f'queue_app_{job.workflow_id}', time=3600)

        if persist:
            db.session.commit()
        else:
            db.session.rollback()

    @staticmethod
    def stop(job, ignore_if_stopped=False, job_id=None):
        redis_store = JobService._get_redis_store(None)
        if job_id:
            workflow_id = job_id - 800000
            msg = json.dumps(
                dict(workflow_id=workflow_id,
                    app_id=workflow_id,
                    job_id=job_id,
                    type='terminate'))
            redis_store.rpush("queue_start", msg)
        else:
            valid_status_in_stop = [StatusExecution.WAITING,
                                    StatusExecution.PENDING,
                                    StatusExecution.RUNNING,
                                    StatusExecution.INTERRUPTED]
            valid_end_status = [StatusExecution.COMPLETED,
                                StatusExecution.CANCELED,
                                StatusExecution.ERROR]
            if job.status not in valid_status_in_stop + valid_end_status:
                raise JobException(
                    f'You cannot stop a job in the state \'{job.status}\''
                    'INVALID_STATE')
            if job.status in valid_status_in_stop:
                job.status = StatusExecution.CANCELED
                job.finished = datetime.datetime.utcnow()
                db.session.add(job)
                db.session.flush()



                # @FIXME Each workflow has only one app. In future,
                # we may support N
                cluster = job.cluster
                msg = json.dumps(
                    dict(workflow_id=job.workflow_id,
                        app_id=job.workflow_id,
                        job_id=job.id,
                        cluster=ClusterItemResponseSchema().dump(cluster),
                        type='terminate'))
                redis_store.rpush("queue_start", msg)

                # # This hash controls the status of job. Used for prevent
                # starting a canceled job be started by Juicer (FIXME: is it
                # used?).
                # redis_store.hset('record_workflow_{}'.format(job.workflow_id),
                #                  'status', StatusExecution.CANCELED)
                #
                redis_store.hset('job_{}'.format(job.id),
                                'status', StatusExecution.CANCELED)

                db.session.commit()
            elif job.status in valid_end_status and not ignore_if_stopped:
                raise JobException(
                    'You cannot stop a job in the state \'{}\''.format(job.status),
                    'ALREADY_FINISHED')

    @staticmethod
    def lock(job, user, computer, force=False):
        job_id = "job_{}".format(job.id)

        redis_store = JobService._get_redis_store(None)
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
            raise JobException(error_code='ALREADY_LOCKED', message=msg)

    @staticmethod
    def get_lock_status(job):
        job_id = "job_{}".format(job.id)
        redis_store = JobService._get_redis_store(None)
        already_locked = redis_store.hget(job_id, 'lock')
        if already_locked != '':
            return json.loads(already_locked)
        else:
            return None

    @staticmethod
    def retrieve_sample(user, job, task_id, port_name, wait):
        # DELIVER messages request the delivery of a result (task_id)
        redis_store = JobService._get_redis_store(None)
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
    def trigger_job(name: str, payload: dict, user):
        print('--------', name, payload)
        redis_store = JobService._get_redis_store(None)
        q = rq.Queue('juicer', connection=redis_store)
        payload['user'] = dict(user._asdict())
        result = q.enqueue(f'juicer.jobs.{name}', payload)
        return result.id

    @staticmethod
    def generate_code(workflow_id, template):
        redis_store = JobService._get_redis_store(None)
        q = rq.Queue('juicer', connection=redis_store)
        payload = {'workflow_id': workflow_id, 'template': template}

        log.info("Payload %s", payload)
        result = q.enqueue('juicer.jobs.code_gen.generate',
                           workflow_id, template)
        return result.id

    @staticmethod
    def get_generate_code_result(job_id):
        return JobService.get_result(job_id, None).result.get('code')

    @staticmethod
    def get_result(key: str, user):
        redis_store = JobService._get_redis_store(None)
        try:
            rq_job = rq.job.Job(key, connection=redis_store)
            return rq_job
        except NoSuchJobError:
            return {'status': 'ERROR', 'message': 'Not found'}

    @staticmethod
    def _get_redis_store(url: str, testing: bool = False, db: int = 0) -> None:
        redis_store = connect_redis_store(url, testing=testing,
                                          decode_responses=False,
                                          db=db)
        return redis_store
