#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sqlalchemy_utils
from flask import request, render_template
from flask_babel import get_locale
from stand.app import babel, app

sqlalchemy_utils.i18n.get_locale = get_locale

'''
# @app.before_request
def before():
    print request.args
    if request.args and 'lang' in request.args:
        if request.args['lang'] not in ('es', 'en'):
            return abort(404)
'''


@babel.localeselector
def get_locale():
    return request.args.get('lang', 'en')


@app.route('/')
def index():
    """Serve the client-side application."""
    return render_template('index.html')


"""
def main(container=False):
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Config file")

    args = parser.parse_args()
    if args.config:
        with open(args.config) as f:
            config = json.load(f)

        app.config["RESTFUL_JSON"] = {
            'cls': app.json_encoder,
            'sort_keys': False,
        }

        server_config = config.get('servers', {})
        app.config['SQLALCHEMY_DATABASE_URI'] = server_config.get(
            'database_url')
        app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
        app.config['SQLALCHEMY_POOL_SIZE'] = 10
        app.config['SQLALCHEMY_POOL_RECYCLE'] = 240

        db.init_app(app)
        with app.app_context():
            db.create_all()

        if server_config.get('environment', 'dev') == 'dev':
            if not container:
                app.run(debug=True, host='0.0.0.0', port=3320)
            else:
                app.debug = True

        socketio_app = socketio.Middleware(sio, app)

        return True, socketio_app
    else:
        parser.print_usage()
        return False, None

# main()
"""
