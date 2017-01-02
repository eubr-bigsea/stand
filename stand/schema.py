# -*- coding: utf-8 -*-
import datetime
import json
from copy import deepcopy
from marshmallow import Schema, fields, post_load
from marshmallow.validate import OneOf
from models import *


def partial_schema_factory(schema_cls):
    schema = schema_cls(partial=True)
    for field_name, field in schema.fields.items():
        if isinstance(field, fields.Nested):
            new_field = deepcopy(field)
            new_field.schema.partial = True
            schema.fields[field_name] = new_field
    return schema


def load_json(str_value):
    try:
        return json.loads(str_value)
    except:
        return "Error loading JSON"

# region Protected\s*
# endregion


class ClusterSimpleListResponseSchema(Schema):
    """ JSON simple """
    id = fields.Integer(required=True)

    @post_load
    def make_object(self, data):
        """ Deserializes data into an instance of Cluster"""
        return Cluster(**data)

    class Meta:
        ordered = True


class ClusterListResponseSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    type = fields.String(required=True, missing=ClusterType.SPARK_LOCAL,
                         default=ClusterType.SPARK_LOCAL,
                         validate=[OneOf(ClusterType.__dict__.keys())])
    address = fields.String(required=True)

    @post_load
    def make_object(self, data):
        """ Deserializes data into an instance of Cluster"""
        return Cluster(**data)

    class Meta:
        ordered = True


class ClusterItemResponseSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    type = fields.String(required=True, missing=ClusterType.SPARK_LOCAL,
                         default=ClusterType.SPARK_LOCAL,
                         validate=[OneOf(ClusterType.__dict__.keys())])
    address = fields.String(required=True)

    @post_load
    def make_object(self, data):
        """ Deserializes data into an instance of Cluster"""
        return Cluster(**data)

    class Meta:
        ordered = True


class ClusterCreateRequestSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    description = fields.String(required=True)
    enabled = fields.String(required=True)
    type = fields.String(required=True, missing=ClusterType.SPARK_LOCAL,
                         default=ClusterType.SPARK_LOCAL,
                         validate=[OneOf(ClusterType.__dict__.keys())])
    address = fields.String(required=True)

    @post_load
    def make_object(self, data):
        """ Deserializes data into an instance of Cluster"""
        return Cluster(**data)

    class Meta:
        ordered = True


class JobItemResponseSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    created = fields.DateTime(required=True, missing=func.now(),
                            default=func.now())
    started = fields.DateTime(required=False, allow_none=True)
    finished = fields.DateTime(required=False, allow_none=True)
    status = fields.String(required=True, missing=StatusExecution.WAITING,
                           default=StatusExecution.WAITING,
                           validate=[OneOf(StatusExecution.__dict__.keys())])
    cluster = fields.Nested('stand.schema.ClusterItemResponseSchema',
                            required=True)
    steps = fields.Nested('stand.schema.JobStepItemResponseSchema',
                          required=True,
                          many=True)
    user = fields.Function(lambda x: {"id": x.user_id, "name": x.user_name, "login": x.user_login})
    workflow = fields.Function(lambda x: {"id": x.workflow_id, "name": x.workflow_name})

    @post_load
    def make_object(self, data):
        """ Deserializes data into an instance of Job"""
        return Job(**data)

    class Meta:
        ordered = True


class JobListResponseSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    created = fields.DateTime(required=True, missing=func.now(),
                            default=func.now())
    started = fields.DateTime(required=False, allow_none=True)
    finished = fields.DateTime(required=False, allow_none=True)
    status = fields.String(required=True, missing=StatusExecution.WAITING,
                           default=StatusExecution.WAITING,
                           validate=[OneOf(StatusExecution.__dict__.keys())])
    cluster = fields.Nested('stand.schema.ClusterListResponseSchema',
                            required=True)
    user = fields.Function(lambda x: {"id": x.user_id, "name": x.user_name, "login": x.user_login})
    workflow = fields.Function(lambda x: {"id": x.workflow_id, "name": x.workflow_name})

    @post_load
    def make_object(self, data):
        """ Deserializes data into an instance of Job"""
        return Job(**data)

    class Meta:
        ordered = True


class JobCreateRequestSchema(Schema):
    """ JSON serialization schema """
    started = fields.DateTime(required=False, allow_none=True)
    finished = fields.DateTime(required=False, allow_none=True)
    workflow_id = fields.Integer(required=True)
    workflow_name = fields.String(required=True)
    workflow_definition = fields.String(required=False, allow_none=True)
    user_id = fields.Integer(required=True)
    user_login = fields.String(required=True)
    user_name = fields.String(required=True)
    cluster_id = fields.Integer(required=True)
    steps = fields.Nested('stand.schema.JobStepCreateRequestSchema',
                          required=True,
                          many=True)

    @post_load
    def make_object(self, data):
        """ Deserializes data into an instance of Job"""
        return Job(**data)

    class Meta:
        ordered = True


class JobExecuteResponseSchema(Schema):
    """ JSON schema for response """
    id = fields.Integer(required=True)
    created = fields.DateTime(required=True, missing=func.now(),
                            default=func.now())
    started = fields.DateTime(required=False, allow_none=True)
    status = fields.String(required=True, missing=StatusExecution.WAITING,
                           default=StatusExecution.WAITING,
                           validate=[OneOf(StatusExecution.__dict__.keys())])
    workflow_id = fields.Integer(required=True)
    message = fields.String(allow_none=True)
    status_url = fields.Url(required=True)
    cluster = fields.Nested('stand.schema.ClusterExecuteResponseSchema',
                            required=True)
    steps = fields.Nested('stand.schema.JobStepExecuteResponseSchema',
                          required=True,
                          many=True)

    @post_load
    def make_object(self, data):
        """ Deserializes data into an instance of Job"""
        return Job(**data)

    class Meta:
        ordered = True


class JobStatusRequestSchema(Schema):
    """ JSON schema for executing tasks """
    token = fields.String(allow_none=True)

    @post_load
    def make_object(self, data):
        """ Deserializes data into an instance of Job"""
        return Job(**data)

    class Meta:
        ordered = True


class JobStepItemResponseSchema(Schema):
    """ JSON serialization schema """
    date = fields.DateTime(required=True)
    status = fields.String(required=True,
                           validate=[OneOf(StatusExecution.__dict__.keys())])
    message = fields.String(allow_none=True)
    std_out = fields.String(allow_none=True)
    std_err = fields.String(allow_none=True)
    exit_code = fields.Integer(allow_none=True)
    logs = fields.Nested('stand.schema.JobStepLogItemResponseSchema',
                         required=True,
                         many=True)
    operation = fields.Function(lambda x: {"id": x.operation_id, "name": x.operation_name})
    task = fields.Function(lambda x: {"id": x.task_id})

    @post_load
    def make_object(self, data):
        """ Deserializes data into an instance of JobStep"""
        return JobStep(**data)

    class Meta:
        ordered = True


class JobStepListResponseSchema(Schema):
    """ JSON serialization schema """
    date = fields.DateTime(required=True)
    status = fields.String(required=True,
                           validate=[OneOf(StatusExecution.__dict__.keys())])
    task_id = fields.Integer(required=True)
    operation_id = fields.Integer(required=True)
    operation_name = fields.String(required=True)
    logs = fields.Nested('stand.schema.JobStepLogListResponseSchema',
                         required=True,
                         many=True)

    @post_load
    def make_object(self, data):
        """ Deserializes data into an instance of JobStep"""
        return JobStep(**data)

    class Meta:
        ordered = True


class JobStepCreateRequestSchema(Schema):
    """ JSON serialization schema """
    date = fields.DateTime(required=True)
    status = fields.String(required=True,
                           validate=[OneOf(StatusExecution.__dict__.keys())])
    task_id = fields.Integer(required=True)
    operation_id = fields.Integer(required=True)
    operation_name = fields.String(required=True)
    logs = fields.Nested('stand.schema.JobStepLogCreateRequestSchema',
                         required=True,
                         many=True)

    @post_load
    def make_object(self, data):
        """ Deserializes data into an instance of JobStep"""
        return JobStep(**data)

    class Meta:
        ordered = True


class JobStepLogListResponseSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    level = fields.String(required=True)
    date = fields.DateTime(required=True)
    message = fields.String(required=True)

    @post_load
    def make_object(self, data):
        """ Deserializes data into an instance of JobStepLog"""
        return JobStepLog(**data)

    class Meta:
        ordered = True


class JobStepLogItemResponseSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    level = fields.String(required=True)
    date = fields.DateTime(required=True)
    message = fields.String(required=True)

    @post_load
    def make_object(self, data):
        """ Deserializes data into an instance of JobStepLog"""
        return JobStepLog(**data)

    class Meta:
        ordered = True


class JobStepLogCreateRequestSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(allow_none=True)
    level = fields.String(required=True)
    date = fields.DateTime(required=True)
    message = fields.String(required=True)

    @post_load
    def make_object(self, data):
        """ Deserializes data into an instance of JobStepLog"""
        return JobStepLog(**data)

    class Meta:
        ordered = True

