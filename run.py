#!/usr/bin/env python
# -*- coding: utf-8 -*-
import eventlet

import werkzeug.serving
from stand.app_api import main

eventlet.monkey_patch()


def run(new_sio_app):
    @werkzeug.serving.run_with_reloader
    def do_run():
        # http_server = WSGIServer(('', 5000), new_app)
        # http_server.serve_forever()
        eventlet.wsgi.server(eventlet.listen(('0.0.0.0', 3320)), new_sio_app)
        # (do_run)


if __name__ == '__main__':
    result, new_app = main(True)
    if result:
        run(new_app)
