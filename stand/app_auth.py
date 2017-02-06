# -*- coding: utf-8 -*-}
import json
from collections import namedtuple
from functools import wraps

from flask import request, Response, g

User = namedtuple("User", "id, login, name")


def check_auth(token):
    return token == '123456'


def authenticate():
    """Sends a 401 response that enables basic auth"""
    headers = {'WWW-Authenticate': 'Basic realm="Login Required"',
               'Content-Type': 'application/json'}
    content = json.dumps(
        dict(msg=('Could not verify your access level for that URL.'
                  'You have to login with proper credentials'), status="ERROR"))
    return Response(content, 401, headers)


def requires_auth(f):
    @wraps(f)
    def decorated(*_args, **kwargs):
        # auth = request.authorization
        token = (request.json and request.json.get('token')) or \
                request.args.get('token') or \
                request.headers.get('X-Auth-Token')
        if not (token and check_auth(token)):
            return authenticate()
        setattr(g, 'user', User(id=1, login="walter", name="Walter SF"))
        return f(*_args, **kwargs)

    return decorated
