#!/usr/bin/env python
# -*- coding: utf-8 -*-
from factory import create_app, create_babel_i18n, create_socket_io_app, \
    create_redis_store

# print "#" * 20
# print 'Starting Lemonade Stand'
# print "#" * 20
app = create_app()
babel = create_babel_i18n(app)
socketio, socketio_app = create_socket_io_app(app)
redis_store = create_redis_store(app)
