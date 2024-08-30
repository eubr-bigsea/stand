#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import gettext

import eventlet
import os
from stand.socketio_events import StandSocketIO
from flask import g, request
from babel import negotiate_locale

locales_path = os.path.join(os.path.dirname(__file__), '..', 'i18n', 'locales')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", type=str,
                        help="Config file", required=True)
    parser.add_argument("--lang", help="Minion messages language (i18n)",
                        required=False, default="en_US")
    args = parser.parse_args()

    eventlet.monkey_patch(all=True)

    from stand.factory import create_app, create_babel_i18n, \
        create_redis_store

    t = gettext.translation('messages', locales_path, [args.lang],
                            fallback=True)
    t.install()

    app = create_app(config_file=args.config)
    babel = create_babel_i18n(app)

    @babel.localeselector
    def get_locale():
        user = getattr(g, 'user', None)
        if user is not None and user.locale:
            return user.locale
        preferred = [x.replace('-', '_') for x in
                     list(request.accept_languages.values())]
        return negotiate_locale(preferred, ['pt_BR', 'en_US'])


    # socketio, socketio_app = create_socket_io_app(app)
    stand_socket_io = StandSocketIO(app)
    redis_store = create_redis_store(app)

    port = int(app.config['STAND_CONFIG'].get('port', 5000))
    if app.debug:
        app.run(debug=True, port=port)
    else:
        # noinspection PyUnresolvedReferences
        eventlet.wsgi.server(eventlet.listen(('', port)),
                             stand_socket_io.socket_app)
