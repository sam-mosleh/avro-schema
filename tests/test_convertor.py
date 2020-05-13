from __future__ import annotations

from enum import Enum
from typing import List, Optional, Union

import fastavro
from pydantic import BaseModel

from avro_schema import __version__
from avro_schema.convertor import json_to_avro


def test_version():
    assert __version__ == '0.1.0'


def test_string_model():
    class Model(BaseModel):
        string_field: str

    model_avro = {
        'name': 'Model',
        'type': 'record',
        'fields': [{
            'name': 'string_field',
            'type': 'string'
        }]
    }
    fastavro.parse_schema(model_avro)
    assert json_to_avro(Model.schema()) == model_avro


def test_namespace():
    class Model(BaseModel):
        string_field: str

    namespace = 'streamer'
    model_avro = {
        'name': 'Model',
        'type': 'record',
        'namespace': namespace,
        'fields': [{
            'name': 'string_field',
            'type': 'string'
        }]
    }
    fastavro.parse_schema(model_avro)
    assert json_to_avro(Model.schema(), namespace) == model_avro


def test_int_and_string_model():
    class Model(BaseModel):
        int_field: int
        string_field: str

    model_avro = {
        'name':
        'Model',
        'type':
        'record',
        'fields': [{
            'name': 'int_field',
            'type': 'long'
        }, {
            'name': 'string_field',
            'type': 'string'
        }]
    }
    fastavro.parse_schema(model_avro)
    assert json_to_avro(Model.schema()) == model_avro


def test_optional_and_default_model():
    class Model(BaseModel):
        optional_int: Optional[int]
        default_int: int = 1000

    model_avro = {
        'name':
        'Model',
        'type':
        'record',
        'fields': [{
            'name': 'optional_int',
            'type': ['null', 'long']
        }, {
            'name': 'default_int',
            'type': 'long',
            'default': 1000
        }]
    }
    fastavro.parse_schema(model_avro)
    assert json_to_avro(Model.schema()) == model_avro


def test_enum_model():
    class StrEnum(str, Enum):
        first = 'F'
        second = 'S'

    class Model(BaseModel):
        string_enum: StrEnum

    model_avro = {
        'name':
        'Model',
        'type':
        'record',
        'fields': [{
            'name': 'string_enum',
            'type': {
                'name': 'string_enum',
                'type': 'enum',
                'symbols': ['F', 'S']
            }
        }]
    }
    print(Model.schema())
    print(json_to_avro(Model.schema()))
    fastavro.parse_schema(model_avro)
    assert json_to_avro(Model.schema()) == model_avro


def test_list_model():
    class Model(BaseModel):
        list_of_int: List[int]

    model_avro = {
        'name':
        'Model',
        'type':
        'record',
        'fields': [{
            'name': 'list_of_int',
            'type': {
                'type': 'array',
                'items': {
                    'type': 'long'
                }
            }
        }]
    }
    fastavro.parse_schema(model_avro)
    assert json_to_avro(Model.schema()) == model_avro


def test_union_model():
    class Model(BaseModel):
        union_of_int_and_string: Union[int, str]

    model_avro = {
        'name':
        'Model',
        'type':
        'record',
        'fields': [{
            'name': 'union_of_int_and_string',
            'type': [{
                'type': 'long'
            }, {
                'type': 'string'
            }]
        }]
    }
    fastavro.parse_schema(model_avro)
    assert json_to_avro(Model.schema()) == model_avro


class StrEnum(str, Enum):
    first = 'F'
    second = 'S'


class Model(BaseModel):
    string_enum: StrEnum


print(Model.schema())
