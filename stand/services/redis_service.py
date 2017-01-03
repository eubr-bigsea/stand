# -*- coding: utf-8 -*-}
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


def connect_redis_store():
    """ Connect to redis or FakeRedis if testing app """
    if current_app.debug:
        redis_store = FlaskRedis.from_custom_provider(MockRedisWrapper)
    else:
        redis_store = FlaskRedis()
    redis_store.init_app(current_app)
    return redis_store
