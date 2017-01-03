# -*- coding: utf-8 -*-}
import logging

import requests
from stand.models import db, StatusExecution, JobException, Job
from stand.services.redis_service import connect_redis_store


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
    def start(job):
        invalid_statuses = [StatusExecution.RUNNING, StatusExecution.PENDING,
                            StatusExecution.INTERRUPTED,
                            StatusExecution.WAITING]

        # Validate if workflow is already running
        jobs_running = Job.query.filter(Job.status.in_(invalid_statuses)) \
            .filter(Job.workflow_id == job.workflow_id).count()
        if jobs_running > 0:
            raise JobException('Workflow is already being run by another job',
                               JobException.ALREADY_RUNNING)

        db.session.add(job)

        redis_store = connect_redis_store()
        redis_store.rpush("start", dict(job_id=job.id,
                                        workflow=dict(id=job.workflow_id)))

        db.session.flush()
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
            redis_store.rpush("stop", dict(job_id=job.id))
            db.session.commit()
        else:
            raise JobException(
                'You cannot stop a job in the state \'{}\''.format(job.status),
                JobException.ALREADY_FINISHED)
