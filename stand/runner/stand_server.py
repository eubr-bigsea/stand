#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import eventlet
from stand.socketio_events import StandSocketIO
import os

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", type=str,
            help="Config file", required=True)
    args = parser.parse_args()

    eventlet.monkey_patch(all=True)

    from stand.factory import create_app, create_babel_i18n, \
        create_socket_io_app, create_redis_store

    app = create_app(config_file=args.config)
    babel = create_babel_i18n(app)
    # socketio, socketio_app = create_socket_io_app(app)
    stand_socket_io = StandSocketIO(app)
    redis_store = create_redis_store(app)

    if app.debug:
        app.run(debug=True)
    else:        
        port = int(app.config['STAND_CONFIG'].get('port', 5000))

        # noinspection PyUnresolvedReferences
        eventlet.wsgi.server(eventlet.listen(('', port)),
                         stand_socket_io.socket_app)
