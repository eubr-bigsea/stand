# -*- coding: utf-8 -*-}
"""
A dummy executor that process Lemonade jobs and only returns fake statuses and
data. Used to test Stand clients in an integration test.
"""
import json
import logging
import logging.config
import random
import time

import datetime

import eventlet
import socketio
from flask_script import Manager

# Logging configuration
from sqlalchemy import and_
from stand.factory import create_app, create_redis_store
from stand.models import Job, StatusExecution, db, JobStep, JobStepLog

app = create_app(log_level=logging.WARNING)
redis_store = create_redis_store(app)
manager = Manager(app)

MESSAGES = [
    "The greatest discovery of my generation is that a human being can alter "
    "his life by altering his attitudes of mind.",
    "Human beings, by changing the inner attitudes of their minds, can change "
    "the outer aspects of their lives.",
    "Complaining is good for you as long as you're not complaining to the "
    "person you're complaining about.",
    "Education is what survives when what has been learned has been forgotten.",
    "A man's character is his fate.",
    "The farther behind I leave the past, the closer I am to forging my own "
    "character.",
    "All our dreams can come true, if we have the courage to pursue them.",
    "Always remember that you are absolutely unique. Just like everyone else. ",
    "A woman's mind is cleaner than a man's: She changes it more often. ",
    "I can resist everything except temptation. "
]


@manager.command
def simulate():
    logging.config.fileConfig('logging_config.ini')
    logger = logging.getLogger(__name__)
    # ap = argparse.ArgumentParser()
    # ap.add_argument('-c', '')
    mgr = socketio.RedisManager(app.config.get('REDIS_URL'), 'job_output')

    statuses = [StatusExecution.RUNNING,
                # StatusExecution.CANCELED, StatusExecution.ERROR,
                # StatusExecution.PENDING, StatusExecution.INTERRUPTED,
                StatusExecution.WAITING, StatusExecution.COMPLETED]
    while True:
        try:
            _, job_json = redis_store.blpop('queue_start')
            job = json.loads(job_json)
            logger.debug('Simulating workflow %s with job %s',
                         job.get('workflow_id'), job.get('job_id'))

            for k in ['job_id', 'workflow_id', 'user_id', 'app_id']:
                if k in job:
                    logger.info('Room for %s', k)
                    room = str(job[k])
                    mgr.emit('update job',
                             data={'message': random.choice(MESSAGES),
                                   'status': StatusExecution.RUNNING,
                                   'id': job['workflow_id']},
                             room=room, namespace="/stand")

            for task in job.get('workflow', {}).get('tasks', []):
                if task['operation']['id'] == 25: # comment
                    continue
                for k in ['job_id', 'workflow_id', 'user_id', 'app_id']:
                    if k in job:
                        logger.info('Room for %s and task %s', k,
                                    task.get('id'))
                        room = str(job[k])
                        mgr.emit('update task',
                                 data={'message': random.choice(MESSAGES),
                                       'status': random.choice(statuses[:-2]),
                                       'id': task.get('id')}, room=room,
                                 namespace="/stand")
                eventlet.sleep(random.randint(2, 8))
                for k in ['job_id', 'workflow_id', 'user_id', 'app_id']:
                    if k in job:
                        room = str(job[k])
                        mgr.emit('update task',
                                 data={'message': random.choice(MESSAGES),
                                       'status': StatusExecution.COMPLETED,
                                       'id': task.get('id')}, room=room,
                                 namespace="/stand")

                # Updates task in database
                job_step_entity = JobStep.query.filter(and_(
                    JobStep.job_id == job.get('job_id'),
                    JobStep.task_id == task['id'])).first()

                job_step_entity.status = StatusExecution.COMPLETED
                job_step_entity.logs.append(JobStepLog(
                    level='WARNING', date=datetime.datetime.now(),
                    message=random.choice(MESSAGES)))

                db.session.add(job_step_entity)

            # eventlet.sleep(5)
            for k in ['job_id', 'workflow_id', 'user_id', 'app_id']:
                if k in job:
                    logger.info('Room for %s', k)
                    room = str(job[k])
                    mgr.emit('update job',
                             data={'message': random.choice(MESSAGES),
                                   'status': StatusExecution.COMPLETED,
                                   'id': job['workflow_id']},
                             room=room, namespace="/stand")

            job_entity = Job.query.get(job.get('job_id'))
            job_entity.status = StatusExecution.COMPLETED
            db.session.add(job_entity)
            db.session.commit()

        except KeyError as ke:
            logger.error('Invalid json? KeyError: %s', ke)
            raise
        except Exception as ex:
            logger.error(ex.message)
            raise
        logger.info('Simulation finished')


if __name__ == "__main__":
    manager.run()
