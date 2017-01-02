#!/usr/bin/env python
# -*- coding: utf-8 -*-
import eventlet
from stand.app import app, socketio_app, socketio
import werkzeug.serving
from stand.views import index
from stand.events import *

eventlet.monkey_patch()


def start_socketio():
    print "Starting socketio"
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', 3320)), socketio_app)


def start_api():
    print "Starting api"
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', 3321)), app)

def run():
    @werkzeug.serving.run_with_reloader
    def do_run():
        '''

        if server_config.get('environment', 'dev') == 'dev':
        app.run(debug=True, host='0.0.0.0', port=3320)
        app.debug = True
        '''
        # http_server = WSGIServer(('', 5000), new_app)
        # http_server.serve_forever()
        eventlet.spawn(start_socketio)
        #eventlet.spawn(start_api)
        # (do_run)


if __name__ == '__main__':
    run()
