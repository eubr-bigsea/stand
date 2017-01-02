# from project.database import db as _db
import logging
import sys

import os
from collections import namedtuple

import pytest
from factories import JobFactory, ClusterFactory
from pytest_factoryboy import register
from stand.factory import create_app, create_redis_store
from stand.models import db as _db

sys.path.append(os.path.dirname(os.path.curdir))

TESTDB = 'test_project.db'
TESTDB_PATH = "{}/{}".format(os.path.dirname(__file__), TESTDB)
TEST_DATABASE_URI = 'sqlite:///' + TESTDB_PATH


@pytest.fixture(scope='function')
def app(request):
    """Session-wide test `Flask` application."""
    settings_override = {
        'TESTING': True,
        'SQLALCHEMY_DATABASE_URI': TEST_DATABASE_URI
    }
    result = create_app(settings_override, log_level=logging.WARN)
    result.debug = True
    # Establish an application context before running the tests.
    ctx = result.app_context()
    ctx.push()

    # Redis
    redis_store = create_redis_store(result)

    yield result

    ctx.pop()


# noinspection PyShadowingNames
@pytest.fixture(scope='function')
def db(app, request):
    """Session-wide test database."""
    if os.path.exists(TESTDB_PATH):
        os.unlink(TESTDB_PATH)

    # See http://stackoverflow.com/a/28527080/1646932 about setup and teardown
    # the database
    _db.app = app
    _db.create_all()
    yield _db
    _db.drop_all()


# noinspection PyShadowingNames
@pytest.fixture(scope='function')
def session(db, request):
    """Creates a new database session for a test."""
    connection = db.engine.connect()
    transaction = connection.begin()

    result_session = db.create_scoped_session(
        options=dict(bind=connection, binds={}))

    db.session = result_session
    yield result_session

    transaction.rollback()
    connection.close()
    result_session.remove()


# noinspection PyShadowingNames
@pytest.fixture(scope='function')
def job_factory(session):
    JobFactory._meta.sqlalchemy_session = session
    return JobFactory


# noinspection PyShadowingNames
@pytest.fixture(scope='function')
def cluster_factory(session):
    ClusterFactory._meta.sqlalchemy_session = session
    return ClusterFactory


# noinspection PyShadowingNames
@pytest.fixture(scope='function')
def model_factories(job_factory, cluster_factory):
    factories = namedtuple('ModelFactories', 'job_factory, cluster_factory')
    return factories(job_factory=job_factory, cluster_factory=cluster_factory)
