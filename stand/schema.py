# -*- coding: utf-8 -*-

import json
from copy import deepcopy
from marshmallow import Schema, fields, post_load
from marshmallow.validate import OneOf
from models import *


def PartialSchemaFactory(schema_cls):
    schema = schema_cls(partial=True)
    for field_name, field in schema.fields.items():
        if isinstance(field, fields.Nested):
            new_field = deepcopy(field)
            new_field.schema.partial = True
            schema.fields[field_name] = new_field
    return schema

def load_json(str):
    print ">>>>>>>>>>", str
    try:
        return json.loads(str)
    except:
        return "Error loading JSON"

# region Protected\s*
# endregion


class JobItemResponseSchema(Schema):
    """ JSON serialization schema """
    id = fields.Integer(required=True)
    created = fields.DateTime(required=True, missing=func.now(),
                             default=func.now())
    started = fields.DateTime(required=False)
    finished = fields.DateTime(required=False)
    status = fields.String(required=True, missing=StatusExecution.WAITING,
                            default=StatusExecution.WAITING,
                           validate=[OneOf(StatusExecution.__dict__.keys())])
    steps = fields.Nested('schema.JobStepItemResponseSchema',
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
    started = fields.DateTime(required=False)
    finished = fields.DateTime(required=False)
    status = fields.String(required=True, missing=StatusExecution.WAITING,
                            default=StatusExecution.WAITING,
                           validate=[OneOf(StatusExecution.__dict__.keys())])
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
    started = fields.DateTime(required=False)
    finished = fields.DateTime(required=False)
    workflow_id = fields.Integer(required=True)
    workflow_name = fields.String(required=True)
    workflow_definition = fields.String(required=False)
    user_id = fields.Integer(required=True)
    user_login = fields.String(required=True)
    user_name = fields.String(required=True)
    steps = fields.Nested('schema.JobStepCreateRequestSchema',
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
    started = fields.DateTime(required=False)
    status = fields.String(required=True, missing=StatusExecution.WAITING,
                            default=StatusExecution.WAITING,
                           validate=[OneOf(StatusExecution.__dict__.keys())])
    workflow_id = fields.Integer(required=True)
    message = fields.String()
    status_url = fields.Url(required=True)
    steps = fields.Nested('schema.JobStepExecuteResponseSchema',
                          many=True)

    @post_load
    def make_object(self, data):
        """ Deserializes data into an instance of Job"""
        return Job(**data)

    class Meta:
        ordered = True


class JobStatusRequestSchema(Schema):
    """ JSON schema for executing tasks """
    token = fields.String()

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
    message = fields.String()
    std_out = fields.String()
    std_err = fields.String()
    exit_code = fields.Integer()
    logs = fields.Nested('schema.JobStepLogItemResponseSchema',
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
    logs = fields.Nested('schema.JobStepLogListResponseSchema',
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
    logs = fields.Nested('schema.JobStepLogCreateRequestSchema',
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
    id = fields.Integer()
    level = fields.String(required=True)
    date = fields.DateTime(required=True)
    message = fields.String(required=True)

    @post_load
    def make_object(self, data):
        """ Deserializes data into an instance of JobStepLog"""
        return JobStepLog(**data)

    class Meta:
        ordered = True

