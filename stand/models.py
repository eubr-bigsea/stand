# -*- coding: utf-8 -*-
import datetime
import json
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, Float, \
    Enum, DateTime, Numeric, Text, Unicode, UnicodeText
from sqlalchemy import event
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship, backref
from sqlalchemy_i18n import make_translatable, translation_base, Translatable

make_translatable(options={'locales': ['pt', 'en', 'es'],
                           'auto_create_locales': True,
                           'fallback_locale': 'en'})

db = SQLAlchemy()


# noinspection PyClassHasNoInit
class StatusExecution:
    COMPLETED = 'COMPLETED'
    WAITING = 'WAITING'
    INTERRUPTED = 'INTERRUPTED'
    CANCELED = 'CANCELED'
    RUNNING = 'RUNNING'
    ERROR = 'ERROR'
    PENDING = 'PENDING'

    @staticmethod
    def values():
        return [n for n in StatusExecution.__dict__.keys()
                if n[0] != '_' and n != 'values']


# noinspection PyClassHasNoInit
class ResultType:
    VISUALIZATION = 'VISUALIZATION'
    MODEL = 'MODEL'
    OTHER = 'OTHER'

    @staticmethod
    def values():
        return [n for n in ResultType.__dict__.keys()
                if n[0] != '_' and n != 'values']


# noinspection PyClassHasNoInit
class ClusterType:
    SPARK_LOCAL = 'SPARK_LOCAL'
    MESOS = 'MESOS'
    YARN = 'YARN'

    @staticmethod
    def values():
        return [n for n in ClusterType.__dict__.keys()
                if n[0] != '_' and n != 'values']


# noinspection PyClassHasNoInit
class ClusterPermission:
    EXECUTE = 'EXECUTE'

    @staticmethod
    def values():
        return [n for n in ClusterPermission.__dict__.keys()
                if n[0] != '_' and n != 'values']


# noinspection PyClassHasNoInit
class JobException(BaseException):
    ALREADY_FINISHED = 'ALREADY_FINISHED'
    ALREADY_LOCKED = 'ALREADY_LOCKED'
    ALREADY_RUNNING = 'ALREADY_RUNNING'
    INVALID_STATE = 'INVALID_STATE'

    def __init__(self, message, error_code):
        self.message = message
        self.error_code = error_code


class Cluster(db.Model):
    """ Processing cluster """
    __tablename__ = 'cluster'

    # Fields
    id = Column(Integer, primary_key=True)
    name = Column(String(200), nullable=False)
    description = Column(String(200), nullable=False)
    enabled = Column(String(200), nullable=False)
    type = Column(Enum(*ClusterType.values(),
                       name='ClusterTypeEnumType'),
                  default=ClusterType.SPARK_LOCAL, nullable=False)
    address = Column(String(200), nullable=False)

    def __unicode__(self):
        return self.name

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)


class ClusterAccess(db.Model):
    """ Permissions for cluster utilization """
    __tablename__ = 'cluster_access'

    # Fields
    id = Column(Integer, primary_key=True)
    permission = Column(Enum(*ClusterPermission.values(),
                             name='ClusterPermissionEnumType'),
                        default=ClusterPermission.EXECUTE, nullable=False)
    user_id = Column(Integer, nullable=False)
    user_login = Column(String(50), nullable=False)
    user_name = Column(String(200), nullable=False)

    # Associations
    cluster_id = Column(Integer,
                        ForeignKey("cluster.id"), nullable=False)
    cluster = relationship("Cluster", foreign_keys=[cluster_id])

    def __unicode__(self):
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
    started = Column(DateTime)
    finished = Column(DateTime)
    status = Column(Enum(*StatusExecution.values(),
                         name='StatusExecutionEnumType'),
                    default=StatusExecution.WAITING, nullable=False)
    status_text = Column(Text)
    workflow_id = Column(Integer, nullable=False)
    workflow_name = Column(String(200), nullable=False)
    workflow_definition = Column(Text)
    user_id = Column(Integer, nullable=False)
    user_login = Column(String(50), nullable=False)
    user_name = Column(String(200), nullable=False)

    # Associations
    cluster_id = Column(Integer,
                        ForeignKey("cluster.id"), nullable=False)
    cluster = relationship("Cluster", foreign_keys=[cluster_id])
    steps = relationship("JobStep", back_populates="job")
    results = relationship("JobResult", back_populates="job")

    def __unicode__(self):
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
    type = Column(Enum(*ResultType.values(),
                       name='ResultTypeEnumType'), nullable=False)

    # Associations
    job_id = Column(Integer,
                    ForeignKey("job.id"), nullable=False)
    job = relationship("Job", foreign_keys=[job_id])

    def __unicode__(self):
        return self.task_id

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)


class JobStep(db.Model):
    """ Records a task execution """
    __tablename__ = 'job_step'

    # Fields
    id = Column(Integer, primary_key=True)
    date = Column(DateTime, nullable=False)
    status = Column(Enum(*StatusExecution.values(),
                         name='StatusExecutionEnumType'), nullable=False)
    task_id = Column(String(200), nullable=False)
    operation_id = Column(Integer, nullable=False)
    operation_name = Column(String(200), nullable=False)

    # Associations
    job_id = Column(Integer,
                    ForeignKey("job.id"), nullable=False)
    job = relationship("Job", foreign_keys=[job_id])
    logs = relationship("JobStepLog")

    def __unicode__(self):
        return self.date

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)


class JobStepLog(db.Model):
    """ Log of task execution as a step in the job """
    __tablename__ = 'job_step_log'

    # Fields
    id = Column(Integer, primary_key=True)
    level = Column(String(200), nullable=False)
    date = Column(DateTime, nullable=False)
    message = Column(Text, nullable=False)

    # Associations
    step_id = Column(Integer,
                     ForeignKey("job_step.id"), nullable=False)
    step = relationship("JobStep", foreign_keys=[step_id])

    def __unicode__(self):
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

    def __unicode__(self):
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
                     ForeignKey("room.id"), nullable=False)
    room = relationship("Room", foreign_keys=[room_id],
                        backref=backref(
                            "participants",
                            cascade="all, delete-orphan"))

    def __unicode__(self):
        return self.sid

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)

