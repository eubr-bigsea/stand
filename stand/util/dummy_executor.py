# -*- coding: utf-8 -*-}
"""
A dummy executor that process Lemonade jobs and only returns fake statuses and
data. Used to test Stand clients in an integration test.
"""
import json
import logging
import random
import time
import urlparse

import os
import socketio
from flask_redis import FlaskRedis
from flask_script import Manager
from stand.configuration import load as load_configuration

# Logging configuration
from stand.factory import create_app, create_redis_store

app = create_app(log_level=logging.WARNING)
redis_store = create_redis_store(app)
manager = Manager(app)

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

STATUSES = ['COMPLETED', 'RUNNING', 'INTERRUPTED', 'CANCELED', 'WAITING',
            'ERROR']
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
    # ap = argparse.ArgumentParser()
    # ap.add_argument('-c', '')
    import pdb
    pdb.set_trace()
    mgr = socketio.RedisManager(app.config.get('REDIS_URL'), 'job_output')

    while True:
        try:
            _, job_json = redis_store.blpop('queue_start')
            job = json.loads(job_json)
            logger.debug('Simulating workflow %s with job %s', job.get('id'),
                         job.get('name'))

            room = job['workflow_id']
            mgr.emit('update workflow',
                     data={'msg': random.choice(MESSAGES),
                           'status': 'RUNNING', 'id': job['workflow_id']},
                     room=room, namespace="/stand")

            for task in job.get('tasks', []):
                mgr.emit('update task',
                         data={'msg': random.choice(MESSAGES),
                               'status': random.choice(STATUSES),
                               'id': task.get('id')}, room=room,
                         namespace="/stand")
                time.sleep(1)

            time.sleep(1)
            mgr.emit('update workflow',
                     data={'msg': random.choice(MESSAGES),
                           'status': 'FINISHED', 'id': job['workflow_id']},
                     room=job['workflow_id'], namespace="/stand")
        except KeyError as ke:
            logger.error('Invalid json? KeyError: %s', ke)
        except Exception as ex:
            logger.error(ex.message)
        logger.info('Simulation finished')


if __name__ == "__main__":
    manager.run()
