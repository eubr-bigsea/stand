import datetime
import json
import re
from copy import deepcopy
from marshmallow import Schema, fields, post_load, post_dump, EXCLUDE, INCLUDE
from marshmallow.validate import OneOf
from flask_babel import gettext
from .models import *


def partial_schema_factory(schema_cls):
    schema = schema_cls(partial=True)
    for field_name, field in list(schema.fields.items()):
        if isinstance(field, fields.Nested):
            new_field = deepcopy(field)
            new_field.schema.partial = True
            schema.fields[field_name] = new_field
    return schema


enum_re = re.compile(r'(Must be one of:) (.+)')

enum_re = re.compile(r'(Must be one of:) (.+)')


def translate_validation(validation_errors):
    for field, errors in list(validation_errors.items()):
        if isinstance(errors, dict):
            validation_errors[field] = translate_validation(errors)
        else:
            final_errors = []
            for error in errors:
                found = enum_re.findall(error)
                if found:
                    final_errors.append(
                        f'{gettext(found[0][0])} {found[0][1]}')
                else:
                    final_errors.append(gettext(error))
            validation_errors[field] = final_errors
        return validation_errors


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
    class Meta:
        unknown = INCLUDE


class OperationIdCreateRequestSchema(EntityIdCreateRequestSchema):
    class Meta:
        unknown = EXCLUDE


class WorkflowDefinitionCreateRequestSchema(Schema):
    """
    Workflow definition. Must be in same format as in Tahiti.
    """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    locale = fields.String(required=False, default="pt")
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

    class Meta:
        unknown = INCLUDE  # must be INCLUDE to add cluster info and other dynamic props


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

    class Meta:
        unknown = EXCLUDE


class FlowDefinitionCreateRequestSchema(Schema):
    """ JSON schema for new instance """
    source_port = fields.Integer(required=True)
    target_port = fields.Integer(required=True)
    source_id = fields.String(required=True)
    target_id = fields.String(required=True)
    environment = fields.String(required=False, allow_none=True)

    class Meta:
        unknown = INCLUDE


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
            if value is not None  # Empty lists must be kept!
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
        unknown = EXCLUDE


class ClusterListResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    description = fields.String(required=True)
    enabled = fields.Boolean(required=True)
    type = fields.String(required=False, allow_none=True, load_default=ClusterType.SPARK_LOCAL, dump_default=ClusterType.SPARK_LOCAL,
                         validate=[OneOf(ClusterType.values())])
    executors = fields.Integer(
        required=False,
        allow_none=True,
        load_default=1,
        dump_default=1)
    executor_cores = fields.Integer(
        required=False,
        allow_none=True,
        load_default=1,
        dump_default=1)
    executor_memory = fields.String(
        required=False,
        allow_none=True,
        load_default='1M',
        dump_default='1M')
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
        unknown = EXCLUDE


class ClusterItemResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    description = fields.String(required=True)
    enabled = fields.Boolean(required=True)
    type = fields.String(required=False, allow_none=True, load_default=ClusterType.SPARK_LOCAL, dump_default=ClusterType.SPARK_LOCAL,
                         validate=[OneOf(ClusterType.values())])
    address = fields.String(required=True)
    executors = fields.Integer(
        required=False,
        allow_none=True,
        load_default=1,
        dump_default=1)
    executor_cores = fields.Integer(
        required=False,
        allow_none=True,
        load_default=1,
        dump_default=1)
    executor_memory = fields.String(
        required=False,
        allow_none=True,
        load_default='1M',
        dump_default='1M')
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
        unknown = EXCLUDE


class ClusterCreateRequestSchema(BaseSchema):
    """ JSON serialization schema """
    name = fields.String(required=True)
    description = fields.String(required=True)
    enabled = fields.Boolean(required=True)
    type = fields.String(required=False, allow_none=True, load_default=ClusterType.SPARK_LOCAL, dump_default=ClusterType.SPARK_LOCAL,
                         validate=[OneOf(ClusterType.values())])
    address = fields.String(required=True)
    executors = fields.Integer(
        required=False,
        allow_none=True,
        load_default=1,
        dump_default=1)
    executor_cores = fields.Integer(
        required=False,
        allow_none=True,
        load_default=1,
        dump_default=1)
    executor_memory = fields.String(
        required=False,
        allow_none=True,
        load_default='1M',
        dump_default='1M')
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
        unknown = EXCLUDE


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
        unknown = EXCLUDE


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
        unknown = EXCLUDE


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
        unknown = EXCLUDE


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
        unknown = EXCLUDE


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
        unknown = EXCLUDE


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
        unknown = EXCLUDE


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
        unknown = EXCLUDE


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
        unknown = EXCLUDE


class JobItemResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=False, allow_none=True)
    description = fields.String(required=False, allow_none=True)
    created = fields.DateTime(required=False, allow_none=True)
    type = fields.String(required=False, allow_none=True, load_default=JobType.NORMAL, dump_default=JobType.NORMAL,
                         validate=[OneOf(JobType.values())])
    started = fields.DateTime(required=False, allow_none=True)
    finished = fields.DateTime(required=False, allow_none=True)
    status = fields.String(required=False, allow_none=True, load_default=StatusExecution.WAITING, dump_default=StatusExecution.WAITING,
                           validate=[OneOf(StatusExecution.values())])
    status_text = fields.String(required=False, allow_none=True)
    exception_stack = fields.String(required=False, allow_none=True)
    job_key = fields.String(required=False, allow_none=True)
    trigger_type = fields.String(required=False, allow_none=True, load_default=TriggerType.MANUAL, dump_default=TriggerType.MANUAL,
                                 validate=[OneOf(TriggerType.values())])
    cluster = fields.Nested(
        'stand.schema.ClusterItemResponseSchema',
        required=True)
    pipeline_run = fields.Nested(
        'stand.schema.PipelineRunItemResponseSchema',
        allow_none=True)
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
        unknown = EXCLUDE


class JobListResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=False, allow_none=True)
    description = fields.String(required=False, allow_none=True)
    created = fields.DateTime(required=False, allow_none=True)
    type = fields.String(required=False, allow_none=True, load_default=JobType.NORMAL, dump_default=JobType.NORMAL,
                         validate=[OneOf(JobType.values())])
    started = fields.DateTime(required=False, allow_none=True)
    finished = fields.DateTime(required=False, allow_none=True)
    status = fields.String(required=False, allow_none=True, load_default=StatusExecution.WAITING, dump_default=StatusExecution.WAITING,
                           validate=[OneOf(StatusExecution.values())])
    status_text = fields.String(required=False, allow_none=True)
    exception_stack = fields.String(required=False, allow_none=True)
    job_key = fields.String(required=False, allow_none=True)
    trigger_type = fields.String(required=False, allow_none=True, load_default=TriggerType.MANUAL, dump_default=TriggerType.MANUAL,
                                 validate=[OneOf(TriggerType.values())])
    cluster = fields.Nested(
        'stand.schema.ClusterListResponseSchema',
        required=True)
    pipeline_run = fields.Nested(
        'stand.schema.PipelineRunListResponseSchema',
        allow_none=True)
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
        unknown = EXCLUDE


class JobCreateRequestSchema(BaseSchema):
    """ JSON serialization schema """
    name = fields.String(required=False, allow_none=True)
    description = fields.String(required=False, allow_none=True)
    type = fields.String(required=False, allow_none=True, load_default=JobType.NORMAL, dump_default=JobType.NORMAL,
                         validate=[OneOf(JobType.values())])
    exception_stack = fields.String(required=False, allow_none=True)
    job_key = fields.String(required=False, allow_none=True)
    trigger_type = fields.String(required=False, allow_none=True, load_default=TriggerType.MANUAL, dump_default=TriggerType.MANUAL,
                                 validate=[OneOf(TriggerType.values())])
    pipeline_run_id = fields.Integer(required=False, allow_none=True)
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
        unknown = EXCLUDE


class JobExecuteResponseSchema(BaseSchema):
    """ JSON schema for response """
    id = fields.Integer(required=True)
    name = fields.String(required=False, allow_none=True)
    description = fields.String(required=False, allow_none=True)
    created = fields.DateTime(required=False, allow_none=True)
    type = fields.String(required=False, allow_none=True, load_default=JobType.NORMAL, dump_default=JobType.NORMAL,
                         validate=[OneOf(JobType.values())])
    started = fields.DateTime(required=False, allow_none=True)
    status = fields.String(required=False, allow_none=True, load_default=StatusExecution.WAITING, dump_default=StatusExecution.WAITING,
                           validate=[OneOf(StatusExecution.values())])
    status_text = fields.String(required=False, allow_none=True)
    exception_stack = fields.String(required=False, allow_none=True)
    workflow_id = fields.Integer(required=True)
    job_key = fields.String(required=False, allow_none=True)
    trigger_type = fields.String(required=False, allow_none=True, load_default=TriggerType.MANUAL, dump_default=TriggerType.MANUAL,
                                 validate=[OneOf(TriggerType.values())])
    message = fields.String(allow_none=True)
    status_url = fields.Url(required=True)
    cluster = fields.Nested(
        'stand.schema.ClusterExecuteResponseSchema',
        required=True)
    pipeline_run = fields.Nested(
        'stand.schema.PipelineRunExecuteResponseSchema',
        allow_none=True)
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
        unknown = EXCLUDE


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
        unknown = EXCLUDE


class JobResultItemResponseSchema(BaseSchema):
    """ JSON serialization schema """
    title = fields.String(required=False, allow_none=True)
    type = fields.String(required=True,
                         validate=[OneOf(ResultType.values())])
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
        unknown = EXCLUDE


class JobResultListResponseSchema(BaseSchema):
    """ JSON serialization schema """
    task_id = fields.String(required=True)
    operation_id = fields.Integer(required=True)
    title = fields.String(required=False, allow_none=True)
    type = fields.String(required=True,
                         validate=[OneOf(ResultType.values())])
    content = fields.String(required=False, allow_none=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of JobResult"""
        return JobResult(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class JobStepItemResponseSchema(BaseSchema):
    """ JSON serialization schema """
    date = fields.DateTime(required=True)
    status = fields.String(required=True,
                           validate=[OneOf(StatusExecution.values())])
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
        unknown = EXCLUDE


class JobStepListResponseSchema(BaseSchema):
    """ JSON serialization schema """
    task_name = fields.String(required=False, allow_none=True)
    date = fields.DateTime(required=True)
    status = fields.String(required=True,
                           validate=[OneOf(StatusExecution.values())])
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
        unknown = EXCLUDE


class JobStepCreateRequestSchema(BaseSchema):
    """ JSON serialization schema """
    task_name = fields.String(required=False, allow_none=True)
    date = fields.DateTime(required=True)
    status = fields.String(required=True,
                           validate=[OneOf(StatusExecution.values())])
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
        unknown = EXCLUDE


class JobStepLogListResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    level = fields.String(required=True)
    status = fields.String(required=True,
                           validate=[OneOf(StatusExecution.values())])
    date = fields.DateTime(required=True)
    message = fields.String(required=True)
    type = fields.String(
        required=False,
        allow_none=True,
        load_default='TEXT',
        dump_default='TEXT')

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of JobStepLog"""
        return JobStepLog(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class JobStepLogItemResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    level = fields.String(required=True)
    status = fields.String(required=True,
                           validate=[OneOf(StatusExecution.values())])
    date = fields.DateTime(required=True)
    message = fields.String(required=True)
    type = fields.String(
        required=False,
        allow_none=True,
        load_default='TEXT',
        dump_default='TEXT')

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of JobStepLog"""
        return JobStepLog(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class JobStepLogCreateRequestSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(allow_none=True)
    level = fields.String(required=True)
    status = fields.String(required=True,
                           validate=[OneOf(StatusExecution.values())])
    date = fields.DateTime(required=True)
    message = fields.String(required=True)
    type = fields.String(
        required=False,
        allow_none=True,
        load_default='TEXT',
        dump_default='TEXT')

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of JobStepLog"""
        return JobStepLog(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class PipelineRunCreateRequestSchema(BaseSchema):
    """ JSON serialization schema """
    start = fields.DateTime(required=True)
    finish = fields.DateTime(required=True)
    pipeline_id = fields.Integer(required=True)
    pipeline_name = fields.String(required=True)
    last_executed_step = fields.Integer(required=True)
    comment = fields.String(required=False, allow_none=True)
    updated = fields.DateTime(
        required=False,
        allow_none=True,
        load_default=datetime.datetime.utcnow,
        dump_default=datetime.datetime.utcnow)
    status = fields.String(required=True,
                           validate=[OneOf(StatusExecution.values())])
    final_status = fields.String(required=False, allow_none=True,
                                 validate=[OneOf(StatusExecution.values())])
    steps = fields.Nested(
        'stand.schema.PipelineStepRunCreateRequestSchema',
        required=True,
        many=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of PipelineRun"""
        return PipelineRun(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class PipelineRunListResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    start = fields.DateTime(required=True)
    finish = fields.DateTime(required=True)
    pipeline_id = fields.Integer(required=True)
    pipeline_name = fields.String(required=True)
    last_executed_step = fields.Integer(required=True)
    comment = fields.String(required=False, allow_none=True)
    updated = fields.DateTime(
        required=False,
        allow_none=True,
        load_default=datetime.datetime.utcnow,
        dump_default=datetime.datetime.utcnow)
    status = fields.String(required=True,
                           validate=[OneOf(StatusExecution.values())])
    final_status = fields.String(required=False, allow_none=True,
                                 validate=[OneOf(StatusExecution.values())])
    steps = fields.Nested(
        'stand.schema.PipelineStepRunListResponseSchema',
        required=True,
        many=True,
        only=['id', 'name', 'created', 'updated', 'workflow_id', 'retries', 'order', 'status', 'final_status'])

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of PipelineRun"""
        return PipelineRun(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class PipelineRunItemResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    start = fields.DateTime(required=True)
    finish = fields.DateTime(required=True)
    pipeline_id = fields.Integer(required=True)
    pipeline_name = fields.String(required=True)
    last_executed_step = fields.Integer(required=True)
    comment = fields.String(required=False, allow_none=True)
    updated = fields.DateTime(
        required=False,
        allow_none=True,
        load_default=datetime.datetime.utcnow,
        dump_default=datetime.datetime.utcnow)
    status = fields.String(required=True,
                           validate=[OneOf(StatusExecution.values())])
    final_status = fields.String(required=False, allow_none=True,
                                 validate=[OneOf(StatusExecution.values())])
    steps = fields.Nested(
        'stand.schema.PipelineStepRunItemResponseSchema',
        required=True,
        many=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of PipelineRun"""
        return PipelineRun(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class PipelineRunCreateRequestSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(allow_none=True)
    start = fields.DateTime(required=True)
    finish = fields.DateTime(required=True)
    pipeline_id = fields.Integer(required=True)
    pipeline_name = fields.String(required=True)
    last_executed_step = fields.Integer(required=True)
    comment = fields.String(required=False, allow_none=True)
    updated = fields.DateTime(
        required=False,
        allow_none=True,
        load_default=datetime.datetime.utcnow,
        dump_default=datetime.datetime.utcnow)
    status = fields.String(required=True,
                           validate=[OneOf(StatusExecution.values())])
    final_status = fields.String(required=False, allow_none=True,
                                 validate=[OneOf(StatusExecution.values())])
    steps = fields.Nested(
        'stand.schema.PipelineStepRunCreateRequestSchema',
        required=True,
        many=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of PipelineRun"""
        return PipelineRun(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class PipelineStepRunCreateRequestSchema(BaseSchema):
    """ JSON serialization schema """
    name = fields.String(required=True)
    created = fields.DateTime(
        required=False,
        allow_none=True,
        load_default=datetime.datetime.utcnow,
        dump_default=datetime.datetime.utcnow)
    updated = fields.DateTime(required=True)
    workflow_id = fields.Integer(required=True)
    retries = fields.Integer(
        required=False,
        allow_none=True,
        load_default=0,
        dump_default=0)
    order = fields.Integer(
        required=False,
        allow_none=True,
        load_default=0,
        dump_default=0)
    comment = fields.String(required=False, allow_none=True)
    status = fields.String(required=True,
                           validate=[OneOf(StatusExecution.values())])
    final_status = fields.String(required=False, allow_none=True,
                                 validate=[OneOf(StatusExecution.values())])
    jobs = fields.Nested(
        'stand.schema.JobCreateRequestSchema',
        allow_none=True,
        many=True,
        only=['id', 'finished', 'created', 'results', 'steps', 'started', 'status', 'user', 'exception_stack'])
    pipeline_run = fields.Nested(
        'stand.schema.PipelineRunCreateRequestSchema',
        required=True)
    logs = fields.Nested(
        'stand.schema.PipelineStepRunLogCreateRequestSchema',
        required=True,
        many=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of PipelineStepRun"""
        return PipelineStepRun(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class PipelineStepRunListResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    created = fields.DateTime(
        required=False,
        allow_none=True,
        load_default=datetime.datetime.utcnow,
        dump_default=datetime.datetime.utcnow)
    updated = fields.DateTime(required=True)
    workflow_id = fields.Integer(required=True)
    retries = fields.Integer(
        required=False,
        allow_none=True,
        load_default=0,
        dump_default=0)
    order = fields.Integer(
        required=False,
        allow_none=True,
        load_default=0,
        dump_default=0)
    comment = fields.String(required=False, allow_none=True)
    status = fields.String(required=True,
                           validate=[OneOf(StatusExecution.values())])
    final_status = fields.String(required=False, allow_none=True,
                                 validate=[OneOf(StatusExecution.values())])
    jobs = fields.Nested(
        'stand.schema.JobListResponseSchema',
        allow_none=True,
        many=True,
        only=['id', 'finished', 'created', 'results', 'steps', 'started', 'status', 'user', 'exception_stack'])
    logs = fields.Nested(
        'stand.schema.PipelineStepRunLogListResponseSchema',
        required=True,
        many=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of PipelineStepRun"""
        return PipelineStepRun(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class PipelineStepRunItemResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    created = fields.DateTime(
        required=False,
        allow_none=True,
        load_default=datetime.datetime.utcnow,
        dump_default=datetime.datetime.utcnow)
    updated = fields.DateTime(required=True)
    workflow_id = fields.Integer(required=True)
    retries = fields.Integer(
        required=False,
        allow_none=True,
        load_default=0,
        dump_default=0)
    order = fields.Integer(
        required=False,
        allow_none=True,
        load_default=0,
        dump_default=0)
    comment = fields.String(required=False, allow_none=True)
    status = fields.String(required=True,
                           validate=[OneOf(StatusExecution.values())])
    final_status = fields.String(required=False, allow_none=True,
                                 validate=[OneOf(StatusExecution.values())])
    jobs = fields.Nested(
        'stand.schema.JobItemResponseSchema',
        allow_none=True,
        many=True,
        only=['id', 'finished', 'created', 'results', 'steps', 'started', 'status', 'user', 'exception_stack'])
    logs = fields.Nested(
        'stand.schema.PipelineStepRunLogItemResponseSchema',
        required=True,
        many=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of PipelineStepRun"""
        return PipelineStepRun(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class PipelineStepRunCreateRequestSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(allow_none=True)
    name = fields.String(required=True)
    created = fields.DateTime(
        required=False,
        allow_none=True,
        load_default=datetime.datetime.utcnow,
        dump_default=datetime.datetime.utcnow)
    updated = fields.DateTime(required=True)
    workflow_id = fields.Integer(required=True)
    retries = fields.Integer(
        required=False,
        allow_none=True,
        load_default=0,
        dump_default=0)
    order = fields.Integer(
        required=False,
        allow_none=True,
        load_default=0,
        dump_default=0)
    comment = fields.String(required=False, allow_none=True)
    status = fields.String(required=True,
                           validate=[OneOf(StatusExecution.values())])
    final_status = fields.String(required=False, allow_none=True,
                                 validate=[OneOf(StatusExecution.values())])
    jobs = fields.Nested(
        'stand.schema.JobCreateRequestSchema',
        allow_none=True,
        many=True,
        only=['id', 'finished', 'created', 'results', 'steps', 'started', 'status', 'user', 'exception_stack'])
    logs = fields.Nested(
        'stand.schema.PipelineStepRunLogCreateRequestSchema',
        required=True,
        many=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of PipelineStepRun"""
        return PipelineStepRun(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class PipelineStepRunLogListResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    created = fields.DateTime(
        required=False,
        allow_none=True,
        load_default=datetime.datetime.utcnow,
        dump_default=datetime.datetime.utcnow)
    action = fields.String(required=True)
    user_id = fields.Integer(required=True)
    user_login = fields.String(required=True)
    user_name = fields.String(required=True)
    comment = fields.String(required=False, allow_none=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of PipelineStepRunLog"""
        return PipelineStepRunLog(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class PipelineStepRunLogItemResponseSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    created = fields.DateTime(
        required=False,
        allow_none=True,
        load_default=datetime.datetime.utcnow,
        dump_default=datetime.datetime.utcnow)
    action = fields.String(required=True)
    user_id = fields.Integer(required=True)
    user_login = fields.String(required=True)
    user_name = fields.String(required=True)
    comment = fields.String(required=False, allow_none=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of PipelineStepRunLog"""
        return PipelineStepRunLog(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class PipelineStepRunLogCreateRequestSchema(BaseSchema):
    """ JSON serialization schema """
    id = fields.Integer(allow_none=True)
    created = fields.DateTime(
        required=False,
        allow_none=True,
        load_default=datetime.datetime.utcnow,
        dump_default=datetime.datetime.utcnow)
    action = fields.String(required=True)
    user_id = fields.Integer(required=True)
    user_login = fields.String(required=True)
    user_name = fields.String(required=True)
    comment = fields.String(required=False, allow_none=True)

    # noinspection PyUnresolvedReferences
    @post_load
    def make_object(self, data, **kwargs):
        """ Deserialize data into an instance of PipelineStepRunLog"""
        return PipelineStepRunLog(**data)

    class Meta:
        ordered = True
        unknown = EXCLUDE

