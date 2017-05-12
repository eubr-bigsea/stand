# -*- coding: utf-8 -*-}
import json
import logging 
from collections import namedtuple
from functools import wraps

import requests
from flask import request, Response, g, current_app
logger = logging.getLogger(__name__)
User = namedtuple("User", "id, login, email, first_name, last_name, locale")

MSG1 = 'Could not verify your access level for that URL. ' \
       'You have to login with proper credentials provided by Lemonade Thorn'

MSG2 = 'Could not verify your access level for that URL. ' \
       'Invalid authentication token'

CONFIG_KEY = 'STAND_CONFIG'


def authenticate(msg, extra_info):
    """Sends a 401 response that enables basic auth"""
    logging.info('User parameters: %s', json.dumps(extra_info))
    return Response(json.dumps({'status': 'ERROR', 'message': msg}), 401,
                    mimetype="application/json")


def requires_auth(f):
    @wraps(f)
    def decorated(*_args, **kwargs):
        access_token = request.headers.get('access-token')
        user_id = request.args.get('user_id') or \
            request.headers.get('x-user-id') or (
            request.json and (request.json.get('user_id') or 
                              request.json.get('user', {}).get('id')))
        client = request.headers.get('client')

        config = current_app.config[CONFIG_KEY]
        internal_token = request.args.get('token', request.headers.get('x-auth-token'))
        if access_token and user_id and client:
            # It is using Thorn
            url = '{}/users/valid_token'.format(
                config['services']['thorn']['url'])
            result = requests.post(url, data={'access-token': access_token,
                                              'user_id': user_id,
                                              'client': client})
            if result.status_code != 200:
                return authenticate(MSG2)
            else:
                user_data = json.loads(result.text)
                setattr(g, 'user', User(id=user_data['id'],
                                        login=user_data['uid'],
                                        email=user_data['email'],
                                        first_name=user_data['firstname'],
                                        last_name=user_data['lastname'],
                                        locale=user_data['locale']))
                return f(*_args, **kwargs)
        elif request.args.get('token'):
            if request.args.get('token') == str(config['secret']):
                setattr(g, 'user',
                        User(0, '', '', '', '', ''))  # System user
                return f(*_args, **kwargs)
            else:
                return authenticate(MSG2, {'client': client, 
                                       'access_token': access_token,
                                       'user_id': user_id })
        else:
            return authenticate(MSG1, {'client': client, 
                                       'access_token': access_token,
                                       'user_id': user_id })

    return decorated
