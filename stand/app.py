#!/usr/bin/env python
# -*- coding: utf-8 -*-
import eventlet
from factory import create_app, create_babel_i18n, create_redis_store
from stand.socketio_events import StandSocketIO

eventlet.monkey_patch(all=True)

# print "#" * 20
# print 'Starting Lemonade Stand'
# print "#" * 20

app = create_app()
babel = create_babel_i18n(app)
stand_socket_io = StandSocketIO(app)
redis_store = create_redis_store(app)

# wrap Flask application with engineio's middleware
if app.debug:
    # app.run(debug=True, port=int(app.config['STAND_CONFIG']
    # .get('port', 5000)))
    port = int(app.config['STAND_CONFIG'].get('port', 5000))

    # noinspection PyUnresolvedReferences
    eventlet.wsgi.server(eventlet.listen(('', port)),
                         stand_socket_io.socket_app)
