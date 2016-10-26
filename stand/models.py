# -*- coding: utf-8 -*-
import json
import datetime

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, Float, \
    Enum, DateTime, Numeric, Text, Unicode, UnicodeText
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


class Job(db.Model):
    """ A workflow execution """
    __tablename__ = 'job'

    # Fields
    id = Column(Integer, primary_key=True)
    created = Column(DateTime, nullable=False, default=func.now())
    started = Column(DateTime)
    finished = Column(DateTime)
    status = Column(Enum(*StatusExecution.__dict__.keys(), 
                         name='StatusExecutionEnumType'), nullable=False, default=StatusExecution.WAITING)
    workflow_id = Column(Integer, nullable=False)
    workflow_name = Column(String(200), nullable=False)
    workflow_definition = Column(Text)
    user_id = Column(Integer, nullable=False)
    user_login = Column(String(50), nullable=False)
    user_name = Column(String(200), nullable=False)

    # Associations
    steps = relationship("JobStep", back_populates="job")

    def __unicode__(self):
        return self.created

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)


class JobStep(db.Model):
    """ Records a task execution """
    __tablename__ = 'job_step'

    # Fields
    id = Column(Integer, primary_key=True)
    date = Column(DateTime, nullable=False)
    status = Column(Enum(*StatusExecution.__dict__.keys(), 
                         name='StatusExecutionEnumType'), nullable=False)
    task_id = Column(Integer, nullable=False)
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
