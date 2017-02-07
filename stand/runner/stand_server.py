#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import os

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", type=str,
            help="Config file", required=True)
    args = parser.parse_args()

    # set environ variables
    # TODO: evaluate if we should change the way those configs are passed along
    os.environ["STAND_CONFIG_FILE"] = args.config

    from stand.factory import create_app, create_babel_i18n, \
        create_socket_io_app, create_redis_store

    app = create_app()
    babel = create_babel_i18n(app)
    socketio, socketio_app = create_socket_io_app(app)
    redis_store = create_redis_store(app)

    if app:
        app.run(debug=True)
