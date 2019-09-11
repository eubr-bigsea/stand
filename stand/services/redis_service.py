# -*- coding: utf-8 -*-}
import urllib.parse

from flask import current_app
from flask_redis import FlaskRedis
from mockredis import MockRedis


class MockRedisWrapper(MockRedis):
    """
    A wrapper to add the `from_url` classmethod
    """

    @classmethod
    def from_url(cls, *args, **kwargs):
        return cls()


def connect_redis_store(url, testing=False, decode_responses=True):
    """ Connect to redis or FakeRedis if testing app """

    def _redis_from_url(redis_url):
        parsed = urllib.parse.urlparse(redis_url)
        parsed_qs = urllib.parse.parse_qs(parsed.query)
        return FlaskRedis(host=parsed.hostname, port=parsed.port,
                          db=int(parsed_qs.get('db', 0)),
                          decode_responses=decode_responses)

    if testing or (current_app and current_app.testing):
        redis_store = FlaskRedis.from_custom_provider(MockRedisWrapper)
        redis_store.init_app(current_app)
        setattr(redis_store, 'expire', lambda x, time: 1)
    elif current_app:
        redis_store = _redis_from_url(current_app.config['REDIS_URL'])
        redis_store.init_app(current_app)
    elif url is None:
        redis_store = FlaskRedis()
        redis_store.init_app(current_app)
    else:
        redis_store = _redis_from_url(url)
    return redis_store
