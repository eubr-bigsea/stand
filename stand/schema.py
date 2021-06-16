# -*- coding: utf-8 -*-
import datetime
import json
from copy import deepcopy
from marshmallow import Schema, fields, post_load, post_dump
from marshmallow.validate import OneOf
from stand.models import *


def partial_schema_factory(schema_cls):
    schema = schema_cls(partial=True)
    for field_name, field in list(schema.fields.items()):
        if isinstance(field, fields.Nested):
            new_field = deepcopy(field)
            new_field.schema.partial = True
            schema.fields[field_name] = new_field
    return schema


def load_json(str_value):
    try:
        return json.loads(str_value)
    except BaseException:
        return None


# region Protected
class EntityIdCreateRequestSchema(Schema):
    id = fields.Integer(required=True)


class ClusterIdCreateRequestSchema(EntityIdCreateRequestSchema):
    pass


class PlatformIdCreateRequestSchema(EntityIdCreateRequestSchema):
    pass


class OperationIdCreateRequestSchema(EntityIdCreateRequestSchema):
    pass


class WorkflowDefinitionCreateRequestSchema(Schema):
    """
    Workflow definition. Must be in same format as in Tahiti.
    """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    description = fields.String(required=False, allow_none=True)
    enabled = fields.Boolean(required=True, default=True)
    image = fields.String(required=False, allow_none=True)
    tasks = fields.Nested('stand.schema.TaskDefinitionCreateRequestSchema',
                          required=True,
                          many=True)
    flows = fields.Nested('stand.schema.FlowDefinitionCreateRequestSchema',
                          required=False,
                          many=True)
    platform = fields.Nested('stand.schema.PlatformIdCreateRequestSchema',
                             required=True)

    user = fields.Nested('stand.schema.UserCreateRequestSchema',
                         required=False)


class TaskDefinitionCreateRequestSchema(Schema):
    id = fields.String(required=True)
    left = fields.Integer(required=False)
    enabled = fields.Boolean(required=False)
    top = fields.Integer(required=False)
    z_index = fields.Integer(required=False)
    name = fields.String(required=False, allow_none=True)
    environment = fields.String(required=False, allow_none=True)
    forms = fields.Dict(required=True)
    operation = fields.Nested(OperationIdCreateRequestSchema, required=True)


class FlowDefinitionCreateRequestSchema(Schema):
    """ JSON schema for new instance """
    source_port = fields.Integer(required=True)
    target_port = fields.Integer(required=True)
    source_id = fields.String(required=True)
    target_id = fields.String(required=True)
    environment = fields.String(required=False, allow_none=True)


class UserCreateRequestSchema(Schema):
    id = fields.Integer(required=True)
    login = fields.String(required=True)
    name = fields.String(required=True)


class PerformanceModelEstimationRequestSchema(Schema):
    model_id = fields.Integer(required=True)
    deadline = fields.Integer(required=True)
    data_size = fields.Integer(required=True)
    batch_size = fields.Integer(required=False, allow_none=True)
    cores = fields.Integer(required=False, allow_none=True)
    iterations = fields.Integer(required=False, allow_none=True)
    platform = fields.String(required=True)
    cluster_id = fields.Integer(required=True)

    data_type = fields.String()


class PerformanceModelEstimationResponseSchema(Schema):
    deadline = fields.String()
    schedule_id = fields.String()


# endregion

class BaseSchema(Schema):
    @post_dump
    def remove_skip_values(self, data, **kwargs):
        return {
            key: value for key, value in data.items()
            if value is not None and value != []
        }


class ClusterSimpleListResponseSchema(BaseSchema):
    """ JSON simple """
    id = fields.Integer(required=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of Cluster"""
        return Cluster(**data)

    class Meta:
        ordered = True


class ClusterListResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    description = fields.String(required=True)
    executors = fields.Integer(
        required=False,
        allow_none=True,
        missing=1,
        default=1)
    executor_cores = fields.Integer(
        required=False,
        allow_none=True,
        missing=1,
        default=1)
    executor_memory = fields.String(
        required=False,
        allow_none=True,
        missing='1M',
        default='1M')
    auth_token = fields.String(required=False, allow_none=True)
    ui_parameters = fields.String(required=False, allow_none=True)
    general_parameters = fields.String(required=False, allow_none=True)
    flavors = fields.Nested(
        'stand.schema.ClusterFlavorListResponseSchema',
        allow_none=True,
        many=True)
    platforms = fields.Nested(
        'stand.schema.ClusterPlatformListResponseSchema',
        allow_none=True,
        many=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of Cluster"""
        return Cluster(**data)

    class Meta:
        ordered = True


class ClusterItemResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    description = fields.String(required=True)
    enabled = fields.Boolean(required=True)
    type = fields.String(required=False, allow_none=True, missing=ClusterType.SPARK_LOCAL, default=ClusterType.SPARK_LOCAL,
                         validate=[OneOf(list(ClusterType.__dict__.keys()))])
    address = fields.String(required=True)
    executors = fields.Integer(
        required=False,
        allow_none=True,
        missing=1,
        default=1)
    executor_cores = fields.Integer(
        required=False,
        allow_none=True,
        missing=1,
        default=1)
    executor_memory = fields.String(
        required=False,
        allow_none=True,
        missing='1M',
        default='1M')
    auth_token = fields.String(required=False, allow_none=True)
    ui_parameters = fields.String(required=False, allow_none=True)
    general_parameters = fields.String(required=False, allow_none=True)
    flavors = fields.Nested(
        'stand.schema.ClusterFlavorItemResponseSchema',
        allow_none=True,
        many=True)
    platforms = fields.Nested(
        'stand.schema.ClusterPlatformItemResponseSchema',
        allow_none=True,
        many=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of Cluster"""
        return Cluster(**data)

    class Meta:
        ordered = True


class ClusterCreateRequestSchema(BaseSchema):
    """ JSON serialization schema """
    name = fields.String(required=True)
    description = fields.String(required=True)
    enabled = fields.Boolean(required=True)
    type = fields.String(required=False, allow_none=True, missing=ClusterType.SPARK_LOCAL, default=ClusterType.SPARK_LOCAL,
                         validate=[OneOf(list(ClusterType.__dict__.keys()))])
    address = fields.String(required=True)
    executors = fields.Integer(
        required=False,
        allow_none=True,
        missing=1,
        default=1)
    executor_cores = fields.Integer(
        required=False,
        allow_none=True,
        missing=1,
        default=1)
    executor_memory = fields.String(
        required=False,
        allow_none=True,
        missing='1M',
        default='1M')
    auth_token = fields.String(required=False, allow_none=True)
    ui_parameters = fields.String(required=False, allow_none=True)
    general_parameters = fields.String(required=False, allow_none=True)
    flavors = fields.Nested(
        'stand.schema.ClusterFlavorCreateRequestSchema',
        allow_none=True,
        many=True)
    platforms = fields.Nested(
        'stand.schema.ClusterPlatformCreateRequestSchema',
        allow_none=True,
        many=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of Cluster"""
        return Cluster(**data)

    class Meta:
        ordered = True


class ClusterFlavorSimpleListResponseSchema(BaseSchema):
    """ JSON simple """
    id = fields.Integer(required=True)
    name = fields.String(required=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of ClusterFlavor"""
        return ClusterFlavor(**data)

    class Meta:
        ordered = True


class ClusterFlavorListResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of ClusterFlavor"""
        return ClusterFlavor(**data)

    class Meta:
        ordered = True


class ClusterFlavorItemResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of ClusterFlavor"""
        return ClusterFlavor(**data)

    class Meta:
        ordered = True


class ClusterFlavorCreateRequestSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    enabled = fields.String(required=True)
    parameters = fields.String(required=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of ClusterFlavor"""
        return ClusterFlavor(**data)

    class Meta:
        ordered = True


class ClusterPlatformSimpleListResponseSchema(BaseSchema):
    """ JSON simple """
    id = fields.Function(lambda x: x.platform_id)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of ClusterPlatform"""
        return ClusterPlatform(**data)

    class Meta:
        ordered = True


class ClusterPlatformListResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Function(lambda x: x.platform_id)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of ClusterPlatform"""
        return ClusterPlatform(**data)

    class Meta:
        ordered = True


class ClusterPlatformItemResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Function(lambda x: x.platform_id)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of ClusterPlatform"""
        return ClusterPlatform(**data)

    class Meta:
        ordered = True


class ClusterPlatformCreateRequestSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Function(lambda x: x.platform_id)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of ClusterPlatform"""
        return ClusterPlatform(**data)

    class Meta:
        ordered = True


class JobItemResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    created = fields.DateTime(required=False, allow_none=True)
    name = fields.String(required=False, allow_none=True)
    type = fields.String(required=False, allow_none=True, missing=JobType.NORMAL, default=JobType.NORMAL,
                         validate=[OneOf(list(JobType.__dict__.keys()))])
    started = fields.DateTime(required=False, allow_none=True)
    finished = fields.DateTime(required=False, allow_none=True)
    status = fields.String(required=False, allow_none=True, missing=StatusExecution.WAITING, default=StatusExecution.WAITING,
                           validate=[OneOf(list(StatusExecution.__dict__.keys()))])
    status_text = fields.String(required=False, allow_none=True)
    exception_stack = fields.String(required=False, allow_none=True)
    job_key = fields.String(required=True)
    cluster = fields.Nested(
        'stand.schema.ClusterItemResponseSchema',
        required=True)
    steps = fields.Nested(
        'stand.schema.JobStepItemResponseSchema',
        required=True,
        many=True)
    results = fields.Nested(
        'stand.schema.JobResultItemResponseSchema',
        required=True,
        many=True)
    user = fields.Function(
        lambda x: {
            "id": x.user_id,
            "name": x.user_name,
            "login": x.user_login})
    workflow = fields.Function(lambda x: json.loads(x.workflow_definition))

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of Job"""
        return Job(**data)

    class Meta:
        ordered = True


class JobListResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    created = fields.DateTime(required=False, allow_none=True)
    name = fields.String(required=False, allow_none=True)
    type = fields.String(required=False, allow_none=True, missing=JobType.NORMAL, default=JobType.NORMAL,
                         validate=[OneOf(list(JobType.__dict__.keys()))])
    started = fields.DateTime(required=False, allow_none=True)
    finished = fields.DateTime(required=False, allow_none=True)
    status = fields.String(required=False, allow_none=True, missing=StatusExecution.WAITING, default=StatusExecution.WAITING,
                           validate=[OneOf(list(StatusExecution.__dict__.keys()))])
    status_text = fields.String(required=False, allow_none=True)
    exception_stack = fields.String(required=False, allow_none=True)
    job_key = fields.String(required=True)
    cluster = fields.Nested(
        'stand.schema.ClusterListResponseSchema',
        required=True)
    results = fields.Nested(
        'stand.schema.JobResultListResponseSchema',
        required=True,
        many=True)
    user = fields.Function(
        lambda x: {
            "id": x.user_id,
            "name": x.user_name,
            "login": x.user_login})
    workflow = fields.Function(lambda x: json.loads(x.workflow_definition))

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of Job"""
        return Job(**data)

    class Meta:
        ordered = True


class JobCreateRequestSchema(BaseSchema):
    """ JSON serialization schema """
    name = fields.String(required=False, allow_none=True)
    type = fields.String(required=False, allow_none=True, missing=JobType.NORMAL, default=JobType.NORMAL,
                         validate=[OneOf(list(JobType.__dict__.keys()))])
    exception_stack = fields.String(required=False, allow_none=True)
    job_key = fields.String(required=True)
    workflow = fields.Nested(
        'stand.schema.WorkflowDefinitionCreateRequestSchema',
        required=True)
    cluster = fields.Nested(
        'stand.schema.ClusterIdCreateRequestSchema',
        required=True)
    user = fields.Nested(
        'stand.schema.UserCreateRequestSchema',
        required=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of Job"""

        data['cluster_id'] = data['cluster']['id']
        data['user_id'] = data['user']['id']
        data['user_name'] = data['user']['name']
        data['user_login'] = data['user']['login']
        data['workflow_id'] = data['workflow']['id']
        data['workflow_name'] = data['workflow']['name']
        data['workflow_definition'] = json.dumps(data['workflow'])

        now = datetime.datetime.now()
        data['steps'] = [JobStep(date=now, status=StatusExecution.PENDING,
                                 task_id=t['id'],
                                 task_name=t.get('name'),
                                 operation_id=t['operation']['id'],
                                 operation_name="EMPTY")
                         for t in data['workflow'].get('tasks', [])]

        data.pop('cluster')
        data.pop('workflow')
        data.pop('user')

        return Job(**data)

    class Meta:
        ordered = True


class JobExecuteResponseSchema(BaseSchema):
    """ JSON schema for response """
    id = fields.Integer(required=True)
    created = fields.DateTime(required=False, allow_none=True)
    name = fields.String(required=False, allow_none=True)
    type = fields.String(required=False, allow_none=True, missing=JobType.NORMAL, default=JobType.NORMAL,
                         validate=[OneOf(list(JobType.__dict__.keys()))])
    started = fields.DateTime(required=False, allow_none=True)
    status = fields.String(required=False, allow_none=True, missing=StatusExecution.WAITING, default=StatusExecution.WAITING,
                           validate=[OneOf(list(StatusExecution.__dict__.keys()))])
    status_text = fields.String(required=False, allow_none=True)
    exception_stack = fields.String(required=False, allow_none=True)
    workflow_id = fields.Integer(required=True)
    job_key = fields.String(required=True)
    message = fields.String(allow_none=True)
    status_url = fields.Url(required=True)
    cluster = fields.Nested(
        'stand.schema.ClusterExecuteResponseSchema',
        required=True)
    steps = fields.Nested(
        'stand.schema.JobStepExecuteResponseSchema',
        required=True,
        many=True)
    results = fields.Nested(
        'stand.schema.JobResultExecuteResponseSchema',
        required=True,
        many=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of Job"""
        return Job(**data)

    class Meta:
        ordered = True


class JobStatusRequestSchema(BaseSchema):
    """ JSON schema for executing tasks """
    token = fields.String(allow_none=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of Job"""
        return Job(**data)

    class Meta:
        ordered = True


class JobResultItemResponseSchema(BaseSchema):
    """ JSON serialization schema """
    title = fields.String(required=False, allow_none=True)
    type = fields.String(required=True,
                         validate=[OneOf(list(ResultType.__dict__.keys()))])
    content = fields.String(required=False, allow_none=True)
    task = fields.Function(lambda x: {"id": x.task_id})
    operation = fields.Function(lambda x: {"id": x.operation_id})

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of JobResult"""
        return JobResult(**data)

    class Meta:
        ordered = True


class JobResultListResponseSchema(BaseSchema):
    """ JSON serialization schema """
    task_id = fields.String(required=True)
    operation_id = fields.Integer(required=True)
    title = fields.String(required=False, allow_none=True)
    type = fields.String(required=True,
                         validate=[OneOf(list(ResultType.__dict__.keys()))])
    content = fields.String(required=False, allow_none=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of JobResult"""
        return JobResult(**data)

    class Meta:
        ordered = True


class JobStepItemResponseSchema(BaseSchema):
    """ JSON serialization schema """
    date = fields.DateTime(required=True)
    status = fields.String(required=True,
                           validate=[OneOf(list(StatusExecution.__dict__.keys()))])
    message = fields.String(allow_none=True)
    std_out = fields.String(allow_none=True)
    std_err = fields.String(allow_none=True)
    exit_code = fields.Integer(allow_none=True)
    logs = fields.Nested(
        'stand.schema.JobStepLogItemResponseSchema',
        required=True,
        many=True)
    operation = fields.Function(
        lambda x: {
            "id": x.operation_id,
            "name": x.operation_name})
    task = fields.Function(lambda x: {"id": x.task_id})

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of JobStep"""
        return JobStep(**data)

    class Meta:
        ordered = True


class JobStepListResponseSchema(BaseSchema):
    """ JSON serialization schema """
    task_name = fields.String(required=False, allow_none=True)
    date = fields.DateTime(required=True)
    status = fields.String(required=True,
                           validate=[OneOf(list(StatusExecution.__dict__.keys()))])
    task_id = fields.String(required=True)
    operation_id = fields.Integer(required=True)
    operation_name = fields.String(required=True)
    logs = fields.Nested(
        'stand.schema.JobStepLogListResponseSchema',
        required=True,
        many=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of JobStep"""
        return JobStep(**data)

    class Meta:
        ordered = True


class JobStepCreateRequestSchema(BaseSchema):
    """ JSON serialization schema """
    task_name = fields.String(required=False, allow_none=True)
    date = fields.DateTime(required=True)
    status = fields.String(required=True,
                           validate=[OneOf(list(StatusExecution.__dict__.keys()))])
    task_id = fields.String(required=True)
    operation_id = fields.Integer(required=True)
    operation_name = fields.String(required=True)
    logs = fields.Nested(
        'stand.schema.JobStepLogCreateRequestSchema',
        required=True,
        many=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of JobStep"""
        return JobStep(**data)

    class Meta:
        ordered = True


class JobStepLogListResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    level = fields.String(required=True)
    status = fields.String(required=True,
                           validate=[OneOf(list(StatusExecution.__dict__.keys()))])
    date = fields.DateTime(required=True)
    message = fields.String(required=True)
    type = fields.String(
        required=False,
        allow_none=True,
        missing='TEXT',
        default='TEXT')

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of JobStepLog"""
        return JobStepLog(**data)

    class Meta:
        ordered = True


class JobStepLogItemResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    level = fields.String(required=True)
    status = fields.String(required=True,
                           validate=[OneOf(list(StatusExecution.__dict__.keys()))])
    date = fields.DateTime(required=True)
    message = fields.String(required=True)
    type = fields.String(
        required=False,
        allow_none=True,
        missing='TEXT',
        default='TEXT')

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of JobStepLog"""
        return JobStepLog(**data)

    class Meta:
        ordered = True


class JobStepLogCreateRequestSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(allow_none=True)
    level = fields.String(required=True)
    status = fields.String(required=True,
                           validate=[OneOf(list(StatusExecution.__dict__.keys()))])
    date = fields.DateTime(required=True)
    message = fields.String(required=True)
    type = fields.String(
        required=False,
        allow_none=True,
        missing='TEXT',
        default='TEXT')

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of JobStepLog"""
        return JobStepLog(**data)

    class Meta:
        ordered = True

