import random

import factory
from factory.alchemy import SQLAlchemyModelFactory
from faker import Factory as FakerFactory
from stand.models import StatusExecution, Job, Cluster, ClusterType

faker = FakerFactory.create()


class ClusterFactory(SQLAlchemyModelFactory):
    id = factory.Sequence(lambda x: x)
    name = factory.LazyAttribute(lambda x: faker.text(40))
    description = factory.LazyAttribute(lambda x: faker.text(40))
    enabled = factory.LazyAttribute(lambda x: random.random() >= .5)
    type = factory.LazyAttribute(lambda x: random.sample(
        [x for x in dir(ClusterType) if x[:2] != '__'], 1)[0])
    address = factory.LazyAttribute(lambda x: faker.text(40))

    class Meta:
        model = Cluster
        sqlalchemy_session_persistence = 'commit'


class JobFactory(SQLAlchemyModelFactory):
    """Job testing factory."""

    id = factory.Sequence(lambda x: x)
    created = factory.LazyAttribute(lambda x: faker.date_time())
    started = factory.LazyAttribute(lambda x: faker.date_time())
    finished = factory.LazyAttribute(lambda x: faker.date_time())

    status = factory.LazyAttribute(lambda x: random.sample(
        [x for x in dir(StatusExecution) if x[:2] != '__'], 1)[0])

    workflow_id = factory.LazyAttribute(lambda x: random.randint(1, 100))

    cluster = factory.SubFactory(ClusterFactory)

    workflow_name = factory.LazyAttribute(lambda x: faker.text(40))
    workflow_definition = factory.LazyAttribute(lambda x: "{}")
    user_id = factory.LazyAttribute(lambda x: random.randint(1, 10))
    user_login = factory.LazyAttribute(lambda x: faker.free_email())
    user_name = factory.LazyAttribute(lambda x: faker.name())

    class Meta:
        model = Job
        sqlalchemy_session_persistence = 'commit'
