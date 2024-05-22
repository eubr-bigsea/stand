import datetime
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, Enum, \
    DateTime, Text
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship, backref

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
    APP = 'APP'
    BATCH = 'BATCH'
    MODEL_BUILDER = 'MODEL_BUILDER'
    NORMAL = 'NORMAL'

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
class TriggerType:
    MANUAL = 'MANUAL'
    TIME_SCHEDULE = 'TIME_SCHEDULE'
    MESSAGE = 'MESSAGE'
    API = 'API'
    WATCH = 'WATCH'
    OTHER = 'OTHER'

    @staticmethod
    def values():
        return [n for n in list(TriggerType.__dict__.keys())
                if n[0] != '_' and n != 'values']

# noinspection PyClassHasNoInit


class JobException(BaseException):
    def __init__(self, message, error_code):
        self.message = message
        self.error_code = error_code

    def __str__(self):
        return self.message


# Association tables definition
    # noinspection PyUnresolvedReferences
job_pipeline_step_run = db.Table(
    'job_pipeline_step_run',
    Column('pipeline_step_run_id', Integer,
           ForeignKey('pipeline_step_run.id'),
           nullable=False, index=True),
    Column('job_id', Integer,
           ForeignKey('job.id'),
           nullable=False, index=True))


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
    flavors = relationship("ClusterFlavor")
    platforms = relationship("ClusterPlatform",
                             cascade="all, delete-orphan")

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
    cluster_id = Column(
        Integer,
        ForeignKey("cluster.id",
                   name="fk_cluster_access_cluster_id"),
        nullable=False,
        index=True)
    cluster = relationship(
        "Cluster",
        overlaps='cluster',
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
    cluster_id = Column(
        Integer,
        ForeignKey("cluster.id",
                   name="fk_cluster_configuration_cluster_id"),
        nullable=False,
        index=True)
    cluster = relationship(
        "Cluster",
        overlaps='cluster',
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
    parameters = Column(Text(4294000000), nullable=False)

    # Associations
    cluster_id = Column(
        Integer,
        ForeignKey("cluster.id",
                   name="fk_cluster_flavor_cluster_id"),
        nullable=False,
        index=True)
    cluster = relationship(
        "Cluster",
        overlaps='flavors',
        foreign_keys=[cluster_id],
        back_populates="flavors"
    )

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
    cluster_id = Column(
        Integer,
        ForeignKey("cluster.id",
                   name="fk_cluster_platform_cluster_id"),
        nullable=False,
        index=True)
    cluster = relationship(
        "Cluster",
        overlaps='platforms',
        foreign_keys=[cluster_id],
        back_populates="platforms"
    )

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
    name = Column(String(50))
    created = Column(DateTime,
                     default=func.now(), nullable=False)
    type = Column(Enum(*list(JobType.values()),
                       name='JobTypeEnumType'),
                  default=JobType.NORMAL, nullable=False)
    started = Column(DateTime)
    finished = Column(DateTime)
    status = Column(Enum(*list(StatusExecution.values()),
                         name='StatusExecutionEnumType'),
                    default=StatusExecution.WAITING, nullable=False)
    status_text = Column(Text(4294000000))
    exception_stack = Column(Text(4294000000))
    pipeline_step_run_id = Column(Integer, nullable=False)
    workflow_id = Column(Integer, nullable=False)
    workflow_name = Column(String(200), nullable=False)
    workflow_definition = Column(Text(4294000000))
    user_id = Column(Integer, nullable=False)
    user_login = Column(String(50), nullable=False)
    user_name = Column(String(200), nullable=False)
    source_code = Column(Text(4294000000))
    job_key = Column(String(200), nullable=False)
    trigger_type = Column(Enum(*list(TriggerType.values()),
                               name='TriggerTypeEnumType'),
                          default=TriggerType.MANUAL, nullable=False)

    # Associations
    cluster_id = Column(
        Integer,
        ForeignKey("cluster.id",
                   name="fk_job_cluster_id"),
        nullable=False,
        index=True)
    cluster = relationship(
        "Cluster",
        overlaps='cluster',
        foreign_keys=[cluster_id])
    steps = relationship("JobStep",
                         cascade="all, delete-orphan")
    results = relationship("JobResult",
                           cascade="all, delete-orphan")
    pipeline_step_run = relationship("PipelineStepRun",back_populates="jobs")

    def __str__(self):
        return self.name

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
    content = Column(Text(4294000000))

    # Associations
    job_id = Column(
        Integer,
        ForeignKey("job.id",
                   name="fk_job_result_job_id"),
        nullable=False,
        index=True)
    job = relationship(
        "Job",
        overlaps='results',
        foreign_keys=[job_id],
        back_populates="results"
    )

    def __str__(self):
        return self.task_id

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)


class JobStep(db.Model):
    """ Records a task execution """
    __tablename__ = 'job_step'

    # Fields
    id = Column(Integer, primary_key=True)
    task_name = Column(String(200))
    date = Column(DateTime, nullable=False)
    status = Column(Enum(*list(StatusExecution.values()),
                         name='StatusExecutionEnumType'), nullable=False)
    task_id = Column(String(200), nullable=False)
    operation_id = Column(Integer, nullable=False)
    operation_name = Column(String(200), nullable=False)

    # Associations
    job_id = Column(
        Integer,
        ForeignKey("job.id",
                   name="fk_job_step_job_id"),
        nullable=False,
        index=True)
    job = relationship(
        "Job",
        overlaps='steps',
        foreign_keys=[job_id],
        back_populates="steps"
    )
    logs = relationship("JobStepLog",
                        cascade="all, delete-orphan")

    def __str__(self):
        return self.task_name

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
    message = Column(Text(4294000000), nullable=False)
    type = Column(String(50),
                  default='TEXT', nullable=False)

    # Associations
    step_id = Column(
        Integer,
        ForeignKey("job_step.id",
                   name="fk_job_step_log_step_id"),
        nullable=False,
        index=True)
    step = relationship(
        "JobStep",
        overlaps='logs',
        foreign_keys=[step_id],
        back_populates="logs"
    )

    def __str__(self):
        return self.level

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)


class PipelineRun(db.Model):
    """ Pipeline run """
    __tablename__ = 'pipeline_run'

    # Fields
    id = Column(Integer, primary_key=True)
    start = Column(DateTime, nullable=False, index=True)
    finish = Column(DateTime, nullable=False, index=True)
    pipeline_id = Column(Integer, nullable=False, index=True)
    comment = Column(String(200))
    status = Column(Enum(*list(StatusExecution.values()),
                         name='StatusExecutionEnumType'), nullable=False)
    final_status = Column(Enum(*list(StatusExecution.values()),
                               name='StatusExecutionEnumType'))

    #new field sugestion
    last_completed_step = Column(Integer, nullable=False, index=True)
    #
    # Associations
    steps = relationship("PipelineStepRun", back_populates="pipeline_run")

    def __str__(self):
        return self.start

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)


class PipelineStepRun(db.Model):
    """ Pipeline step run """
    __tablename__ = 'pipeline_step_run'

    # Fields
    id = Column(Integer, primary_key=True)
    created = Column(DateTime,
                     default=datetime.datetime.utcnow, nullable=False)
    updated = Column(DateTime, nullable=False, index=True)
    workflow_id = Column(Integer, nullable=False, index=True)
    retries = Column(Integer,
                     default=0, nullable=False)
    comment = Column(String(200))
    status = Column(Enum(*list(StatusExecution.values()),
                         name='StatusExecutionEnumType'), nullable=False)
    final_status = Column(Enum(*list(StatusExecution.values()),
                               name='StatusExecutionEnumType'))

    # Associations
    jobs = relationship(
        "Job",
        overlaps="pipeline_step_runs",
        secondary=job_pipeline_step_run,
        back_populates="pipeline_step_run")
    pipeline_run_id = Column(
        Integer,
        ForeignKey("pipeline_run.id",
                   name="fk_pipeline_step_run_pipeline_run_id"),
        nullable=False,
        index=True)
    pipeline_run = relationship(
        "PipelineRun",
        overlaps='steps',
        foreign_keys=[pipeline_run_id],
        back_populates="steps"
    )
    logs = relationship("PipelineStepRunLog")

    def __str__(self):
        return self.created

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)


class PipelineStepRunLog(db.Model):
    """ Pipeline step run log """
    __tablename__ = 'pipeline_step_run_log'

    # Fields
    id = Column(Integer, primary_key=True)
    created = Column(DateTime,
                     default=datetime.datetime.utcnow, nullable=False)
    action = Column(String(200), nullable=False)
    user_id = Column(Integer, nullable=False)
    user_login = Column(String(200), nullable=False)
    user_name = Column(String(200), nullable=False)
    comment = Column(String(200))

    # Associations
    pipeline_step_run_id = Column(
        Integer,
        ForeignKey("pipeline_step_run.id",
                   name="fk_pipeline_step_run_log_pipeline_step_run_id"),
        nullable=False,
        index=True)
    pipeline_step_run = relationship(
        "PipelineStepRun",
        overlaps='logs',
        foreign_keys=[pipeline_step_run_id],
        back_populates="logs"
    )

    def __str__(self):
        return self.created

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
    room_id = Column(
        Integer,
        ForeignKey("room.id",
                   name="fk_room_participant_room_id"),
        nullable=False,
        index=True)
    room = relationship(
        "Room",
        overlaps='participants',
        foreign_keys=[room_id],
        backref=backref("participants",
                        cascade="all, delete-orphan"))

    def __str__(self):
        return self.sid

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)


## duplicated model that also exists in tahiti
## this class should be used only for convenience
class Pipeline():
    """ Pipeline """
    # Fields
    id = None
    name = None 
    description = None 
    user_id = None 
    user_login = None 
    user_name = None
    steps = None
    created = None 
    updated = None 
    version = None 
 
    execution_window = None
    variables = None
    preferred_cluster_id = None
    
    def __str__(self):
        return self.name

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)
    
## duplicated model that also exists in tahiti
## this class should be used only for convenience
class PipelineStep():
    """ Pipeline step """
    
    def __init__(self, id=None, name=None, order=None, 
                 scheduling=None, trigger_type=None,description=None, enabled=None,
                 workflow_type=None):
        self.id = id
        self.name = name
        self.order = order
        self.scheduling = scheduling
        self.trigger_type =trigger_type
        self.description = description
        self.enabled = enabled
        self.workflow_type = workflow_type

  
    def __str__(self):
        return self.name

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)