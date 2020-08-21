# -*- coding: utf-8 -*-
import datetime
import json
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, Float, \
    Enum, DateTime, Numeric, Text, Unicode, UnicodeText
from sqlalchemy import event
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship, backref
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy_i18n import make_translatable, translation_base, Translatable

make_translatable(options={'locales': ['pt', 'en'],
                           'auto_create_locales': True,
                           'fallback_locale': 'en'})

db = SQLAlchemy()


# noinspection PyClassHasNoInit
class StatusExecution:
    COMPLETED = 'COMPLETED'
    ERROR = 'ERROR'
    INTERRUPTED = 'INTERRUPTED'
    PENDING = 'PENDING'
    RUNNING = 'RUNNING'
    WAITING = 'WAITING'
    CANCELED = 'CANCELED'

    @staticmethod
    def values():
        return [n for n in list(StatusExecution.__dict__.keys())
                if n[0] != '_' and n != 'values']


# noinspection PyClassHasNoInit
class ResultType:
    VISUALIZATION = 'VISUALIZATION'
    MODEL = 'MODEL'
    HTML = 'HTML'
    TEXT = 'TEXT'
    METRIC = 'METRIC'
    OTHER = 'OTHER'

    @staticmethod
    def values():
        return [n for n in list(ResultType.__dict__.keys())
                if n[0] != '_' and n != 'values']


# noinspection PyClassHasNoInit
class ClusterType:
    SPARK_LOCAL = 'SPARK_LOCAL'
    YARN = 'YARN'
    MESOS = 'MESOS'
    KUBERNETES = 'KUBERNETES'

    @staticmethod
    def values():
        return [n for n in list(ClusterType.__dict__.keys())
                if n[0] != '_' and n != 'values']


# noinspection PyClassHasNoInit
class JobType:
    NORMAL = 'NORMAL'
    APP = 'APP'
    BATCH = 'BATCH'

    @staticmethod
    def values():
        return [n for n in list(JobType.__dict__.keys())
                if n[0] != '_' and n != 'values']


# noinspection PyClassHasNoInit
class ClusterPermission:
    EXECUTE = 'EXECUTE'

    @staticmethod
    def values():
        return [n for n in list(ClusterPermission.__dict__.keys())
                if n[0] != '_' and n != 'values']


# noinspection PyClassHasNoInit
class PermissionType:
    EXECUTE = 'EXECUTE'
    LIST = 'LIST'
    STOP = 'STOP'
    MANAGE = 'MANAGE'

    @staticmethod
    def values():
        return [n for n in list(PermissionType.__dict__.keys())
                if n[0] != '_' and n != 'values']

# noinspection PyClassHasNoInit


class JobException(BaseException):
    def __init__(self, message, error_code):
        self.message = message
        self.error_code = error_code

# Association tables definition


class Cluster(db.Model):
    """ Processing cluster """
    __tablename__ = 'cluster'

    # Fields
    id = Column(Integer, primary_key=True)
    name = Column(String(200), nullable=False)
    description = Column(String(200), nullable=False)
    enabled = Column(Boolean, nullable=False)
    type = Column(Enum(*list(ClusterType.values()),
                       name='ClusterTypeEnumType'),
                  default=ClusterType.SPARK_LOCAL, nullable=False)
    address = Column(String(200), nullable=False)
    executors = Column(Integer,
                       default=1, nullable=False)
    executor_cores = Column(Integer,
                            default=1, nullable=False)
    executor_memory = Column(String(15),
                             default='1M', nullable=False)
    auth_token = Column(String(1000))
    ui_parameters = Column(String(1000))
    general_parameters = Column(String(3000))

    # Associations
    flavors = relationship("ClusterFlavor", back_populates="cluster")

    def __str__(self):
        return self.name

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)


class ClusterAccess(db.Model):
    """ Permissions for cluster utilization """
    __tablename__ = 'cluster_access'

    # Fields
    id = Column(Integer, primary_key=True)
    permission = Column(Enum(*list(ClusterPermission.values()),
                             name='ClusterPermissionEnumType'),
                        default=ClusterPermission.EXECUTE, nullable=False)
    user_id = Column(Integer, nullable=False)
    user_login = Column(String(50), nullable=False)
    user_name = Column(String(200), nullable=False)

    # Associations
    cluster_id = Column(Integer,
                        ForeignKey("cluster.id",
                                   name="fk_cluster_id"), nullable=False)
    cluster = relationship(
        "Cluster",
        foreign_keys=[cluster_id])

    def __str__(self):
        return self.permission

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)


class ClusterConfiguration(db.Model):
    """ Permissions for cluster utilization """
    __tablename__ = 'cluster_configuration'

    # Fields
    id = Column(Integer, primary_key=True)
    name = Column(String(200), nullable=False)
    value = Column(String(500), nullable=False)

    # Associations
    cluster_id = Column(Integer,
                        ForeignKey("cluster.id",
                                   name="fk_cluster_id"), nullable=False)
    cluster = relationship(
        "Cluster",
        foreign_keys=[cluster_id])

    def __str__(self):
        return self.name

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)


class ClusterFlavor(db.Model):
    """ Cluster flavor """
    __tablename__ = 'cluster_flavor'

    # Fields
    id = Column(Integer, primary_key=True)
    name = Column(String(200), nullable=False)
    enabled = Column(String(200), nullable=False)
    parameters = Column(LONGTEXT, nullable=False)

    # Associations
    cluster_id = Column(Integer,
                        ForeignKey("cluster.id",
                                   name="fk_cluster_id"), nullable=False)
    cluster = relationship(
        "Cluster",
        foreign_keys=[cluster_id])

    def __str__(self):
        return self.name

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)


class ClusterPlatform(db.Model):
    """ Cluster platform """
    __tablename__ = 'cluster_platform'

    # Fields
    id = Column(Integer, primary_key=True)
    platform_id = Column(Integer,
                         default=1, nullable=False)

    # Associations
    cluster_id = Column(Integer,
                        ForeignKey("cluster.id",
                                   name="fk_cluster_id"), nullable=False)
    cluster = relationship(
        "Cluster",
        foreign_keys=[cluster_id])

    def __str__(self):
        return self.platform_id

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)


class ExecutionPermission(db.Model):
    """ Associates permissions to a user """
    __tablename__ = 'execution_permission'

    # Fields
    id = Column(Integer, primary_key=True)
    permission = Column(Enum(*list(PermissionType.values()),
                             name='PermissionTypeEnumType'), nullable=False)
    user_id = Column(Integer, nullable=False)

    def __str__(self):
        return self.permission

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)


class Job(db.Model):
    """ A workflow execution """
    __tablename__ = 'job'

    # Fields
    id = Column(Integer, primary_key=True)
    created = Column(DateTime,
                     default=func.now(), nullable=False)
    name = Column(String(50))
    type = Column(Enum(*list(JobType.values()),
                       name='JobTypeEnumType'),
                  default=JobType.NORMAL, nullable=False)
    started = Column(DateTime)
    finished = Column(DateTime)
    status = Column(Enum(*list(StatusExecution.values()),
                         name='StatusExecutionEnumType'),
                    default=StatusExecution.WAITING, nullable=False)
    status_text = Column(LONGTEXT)
    exception_stack = Column(LONGTEXT)
    workflow_id = Column(Integer, nullable=False)
    workflow_name = Column(String(200), nullable=False)
    workflow_definition = Column(LONGTEXT)
    user_id = Column(Integer, nullable=False)
    user_login = Column(String(50), nullable=False)
    user_name = Column(String(200), nullable=False)
    source_code = Column(LONGTEXT)
    job_key = Column(String(200), nullable=False)

    # Associations
    cluster_id = Column(Integer,
                        ForeignKey("cluster.id",
                                   name="fk_cluster_id"), nullable=False)
    cluster = relationship(
        "Cluster",
        foreign_keys=[cluster_id])
    steps = relationship("JobStep", back_populates="job",
                         cascade="all, delete-orphan")
    results = relationship("JobResult", back_populates="job",
                           cascade="all, delete-orphan")

    def __str__(self):
        return self.created

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)


class JobResult(db.Model):
    """ Result of a job """
    __tablename__ = 'job_result'

    # Fields
    id = Column(Integer, primary_key=True)
    task_id = Column(String(200), nullable=False)
    operation_id = Column(Integer, nullable=False)
    title = Column(String(200))
    type = Column(Enum(*list(ResultType.values()),
                       name='ResultTypeEnumType'), nullable=False)
    content = Column(LONGTEXT)

    # Associations
    job_id = Column(Integer,
                    ForeignKey("job.id",
                               name="fk_job_id"), nullable=False)
    job = relationship(
        "Job",
        foreign_keys=[job_id])

    def __str__(self):
        return self.task_id

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)


class JobStep(db.Model):
    """ Records a task execution """
    __tablename__ = 'job_step'

    # Fields
    id = Column(Integer, primary_key=True)
    date = Column(DateTime, nullable=False)
    status = Column(Enum(*list(StatusExecution.values()),
                         name='StatusExecutionEnumType'), nullable=False)
    task_id = Column(String(200), nullable=False)
    operation_id = Column(Integer, nullable=False)
    operation_name = Column(String(200), nullable=False)
    task_name = Column(String(200))

    # Associations
    job_id = Column(Integer,
                    ForeignKey("job.id",
                               name="fk_job_id"), nullable=False)
    job = relationship(
        "Job",
        foreign_keys=[job_id])
    logs = relationship("JobStepLog",
                        cascade="all, delete-orphan")

    def __str__(self):
        return self.date

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)


class JobStepLog(db.Model):
    """ Log of task execution as a step in the job """
    __tablename__ = 'job_step_log'

    # Fields
    id = Column(Integer, primary_key=True)
    level = Column(String(200), nullable=False)
    status = Column(Enum(*list(StatusExecution.values()),
                         name='StatusExecutionEnumType'), nullable=False)
    date = Column(DateTime, nullable=False)
    message = Column(LONGTEXT, nullable=False)
    type = Column(String(50),
                  default='TEXT', nullable=False)

    # Associations
    step_id = Column(Integer,
                     ForeignKey("job_step.id",
                                name="fk_job_step_id"), nullable=False)
    step = relationship(
        "JobStep",
        foreign_keys=[step_id])

    def __str__(self):
        return self.level

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)


class Room(db.Model):
    """ Communication room available in Stand """
    __tablename__ = 'room'

    # Fields
    id = Column(Integer, primary_key=True)
    name = Column(String(200), nullable=False)
    created = Column(DateTime,
                     default=datetime.datetime.utcnow, nullable=False)
    consumers = Column(Integer,
                       default=0, nullable=False)

    def __str__(self):
        return self.name

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)


class RoomParticipant(db.Model):
    """ Participant of a room """
    __tablename__ = 'room_participant'

    # Fields
    id = Column(Integer, primary_key=True)
    sid = Column(String(200), nullable=False)
    join_date = Column(DateTime,
                       default=datetime.datetime.utcnow, nullable=False)
    leave_date = Column(DateTime)

    # Associations
    room_id = Column(Integer,
                     ForeignKey("room.id",
                                name="fk_room_id"), nullable=False)
    room = relationship(
        "Room",
        foreign_keys=[room_id],
        backref=backref("participants",
                        cascade="all, delete-orphan"))

    def __str__(self):
        return self.sid

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)

