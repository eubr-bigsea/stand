#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

import eventlet
from factory import create_app, create_babel_i18n, create_redis_store
from pymysql import OperationalError
from sqlalchemy import event
from sqlalchemy.exc import DisconnectionError
from sqlalchemy.pool import Pool
from stand.socketio_events import StandSocketIO

eventlet.monkey_patch(all=True)

app = create_app()
babel = create_babel_i18n(app)
stand_socket_io = StandSocketIO(app)
redis_store = create_redis_store(app)


@event.listens_for(Pool, "checkout")
def check_connection(dbapi_con, con_record, con_proxy):
    cursor = dbapi_con.cursor()
    try:
        cursor.execute("SELECT 1")
    except OperationalError, ex:
        if ex.args[0] in (
                2006,  # MySQL server has gone away
                2013,  # Lost connection to MySQL server during query
                2055):  # Lost connection to MySQL server at '%s', system error: %d
            # caught by pool, which will retry with a new connection
            raise DisconnectionError()
        else:
            raise


def main(is_main_module):
    logger = logging.getLogger(__name__)
    config = app.config['STAND_CONFIG']
    port = int(config.get('port', 5000))
    logger.debug('Running in %s mode', config.get('environment'))

    if is_main_module:
        if config.get('environment', 'dev') == 'dev':
            # admin.add_view(DataSourceModelView(DataSource, db.session))
            # admin.add_view(StorageModelView(Storage, db.session))
            app.run(debug=True, port=port)
        else:
            eventlet.wsgi.server(eventlet.listen(('', port)),
                                 stand_socket_io.socket_app)


main(__name__ == '__main__')
