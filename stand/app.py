#!/usr/bin/env python
# -*- coding: utf-8 -*-
import eventlet
eventlet.monkey_patch(all=True)
import json
import logging

import urllib.parse

from babel import negotiate_locale
from flask import request, g
from flask_babel import gettext, Babel
from pymysql import OperationalError
from redis import StrictRedis
from sqlalchemy import event
from sqlalchemy.exc import DisconnectionError
from sqlalchemy.pool import Pool
from stand.factory import create_app, create_redis_store
from stand.models import Job
from stand.socketio_events import StandSocketIO


app = create_app()
babel = Babel()
# babel = create_babel_i18n(app)
babel.init_app(app)
stand_socket_io = StandSocketIO(app)
redis_store = create_redis_store(app)
logger = logging.getLogger(__name__)

@babel.localeselector
def get_locale():
    user = getattr(g, 'user', None)
    if user is not None:
        return user.locale or 'en'
    preferred = [x.replace('-', '_') for x in
                 list(request.accept_languages.values())]
    return negotiate_locale(preferred, ['pt_BR', 'en_US'])


@event.listens_for(Pool, "checkout")
def check_connection(dbapi_con, con_record, con_proxy):
    cursor = dbapi_con.cursor()
    try:
        cursor.execute("SELECT 1")
    except OperationalError as ex:
        if ex.args[0] in (
                2006,  # MySQL server has gone away
                2013,  # Lost connection to MySQL server during query
                2055):  # Lost connection to MySQL server
            # caught by pool, which will retry with a new connection
            raise DisconnectionError()
        else:
            raise


def handle_updates(app_, redis_url):
    parsed = urllib.parse.urlparse(redis_url)
    redis_conn = StrictRedis(
            decode_responses=True,host=parsed.hostname, port=parsed.port)
    with app_.app_context():
        while True:
            _, msg = redis_conn.blpop('stand_updates')
            msg_json = json.loads(msg)
            job = Job.query.get(msg_json.get('id'))
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(msg)


def main(is_main_module):
    config = app.config['STAND_CONFIG']
    port = int(config.get('port', 5000))
    logger.debug(gettext('Running in %s mode'), config.get('environment'))

    if is_main_module:
        if config.get('environment', 'dev') == 'dev':
            # admin.add_view(DataSourceModelView(DataSource, db.session))
            # admin.add_view(StorageModelView(Storage, db.session))
            app.run(debug=True, port=port)
        else:
            # eventlet.spawn(handle_updates, app,
            #                config.get('servers').get('redis_url'))
            # noinspection PyUnresolvedReferences
            eventlet.wsgi.server(eventlet.listen(('', port)),
                                 stand_socket_io.socket_app)


main(__name__ == '__main__')
