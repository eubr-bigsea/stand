# -*- coding: utf-8 -*-}
import urlparse

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


def connect_redis_store(url, testing=False):
    """ Connect to redis or FakeRedis if testing app """
    if testing:
        redis_store = FlaskRedis.from_custom_provider(MockRedisWrapper)
    elif url is None:
        redis_store = FlaskRedis()
        redis_store.init_app(current_app)
    else:
        parsed = urlparse.urlparse(url)
        parsed_qs = urlparse.parse_qs(parsed.query)
        redis_store = FlaskRedis(host=parsed.hostname, port=parsed.port,
                                 db=int(parsed_qs.get('db', 0)))
    return redis_store
