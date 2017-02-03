# -*- coding: utf-8 -*-}
import json
import logging

import requests
import stand.util
from stand.models import db, StatusExecution, JobException, Job
from stand.services.redis_service import connect_redis_store
from stand.services.tahiti_service import tahiti_service


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
    def start(job, workflow):
        invalid_statuses = [StatusExecution.RUNNING, StatusExecution.PENDING,
                            StatusExecution.INTERRUPTED,
                            StatusExecution.WAITING]

        # @FIXME Validate workflow

        # Validate if workflow is already running
        jobs_running = Job.query.filter(Job.status.in_(invalid_statuses)) \
            .filter(Job.workflow_id == job.workflow_id).first()
        if jobs_running > 0:
            raise JobException(
                'Workflow is already being run by another job ({})'.format(
                    jobs_running.id), JobException.ALREADY_RUNNING)

        # Initial job status must be WAITING
        job.status = StatusExecution.WAITING
        db.session.add(job)

        redis_store = connect_redis_store()
        # This queue is used to keep the order of execution and to know
        # what is pending.

        # @FIXME Each workflow has only one app. In future, we may support N
        msg = json.dumps(dict(app_id=job.workflow_id,
                              job_id=job.id,
                              workflow_id=job.workflow_id,
                              type='execute',
                              workflow=workflow))
        redis_store.rpush("queue_start", msg)

        db.session.flush()  # Flush is needed to get the value of job.id

        redis_store.hset('record_workflow_{}'.format(job.workflow_id),
                         'status', job.status)

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

            redis_store = connect_redis_store()

            # This queue controls what should be stopped
            redis_store.rpush("stop", dict(job_id=job.id))

            # This hash controls the status of job. Used for prevent starting
            # a canceled job be started by Juicer.
            redis_store.hset("job_{}".format(job.id), 'status',
                             StatusExecution.CANCELED)

            db.session.commit()

        else:
            raise JobException(
                'You cannot stop a job in the state \'{}\''.format(job.status),
                JobException.ALREADY_FINISHED)

    @staticmethod
    def lock(job, user, computer, force=False):
        job_id = "job_{}".format(job.id)

        redis_store = connect_redis_store()
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
        redis_store = connect_redis_store()
        already_locked = redis_store.hget(job_id, 'lock')
        if already_locked != '':
            return json.loads(already_locked)
        else:
            return None
