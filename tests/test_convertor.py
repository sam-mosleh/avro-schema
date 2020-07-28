from __future__ import annotations

import datetime
import uuid
from enum import Enum, IntEnum
from typing import Dict, List, Optional, Union

import fastavro
import pytest
from pydantic import BaseModel

from avro_schema import __version__
from avro_schema.convertor import JsonSchema


def test_version():
    assert __version__ == "0.3.1"


def test_invalid_schema():
    with pytest.raises(TypeError):
        JsonSchema({}).to_avro()


def test_empty_model():
    class Model(BaseModel):
        pass

    model_avro = {"namespace": "base", "name": "Model", "type": "record", "fields": []}
    fastavro.parse_schema(model_avro)
    assert JsonSchema(Model.schema()).to_avro() == model_avro


def test_string_model():
    class Model(BaseModel):
        string_field: str

    model_avro = {
        "namespace": "base",
        "name": "Model",
        "type": "record",
        "fields": [{"name": "string_field", "type": "string"}],
    }
    fastavro.parse_schema(model_avro)
    assert JsonSchema(Model.schema()).to_avro() == model_avro


def test_namespace():
    class Model(BaseModel):
        string_field: str

    namespace = "streamer"
    model_avro = {
        "name": "Model",
        "type": "record",
        "namespace": namespace,
        "fields": [{"name": "string_field", "type": "string"}],
    }
    fastavro.parse_schema(model_avro)
    assert JsonSchema(Model.schema(), namespace).to_avro() == model_avro


def test_int_and_string_model():
    class Model(BaseModel):
        int_field: int
        string_field: str

    model_avro = {
        "namespace": "base",
        "name": "Model",
        "type": "record",
        "fields": [
            {"name": "int_field", "type": "long"},
            {"name": "string_field", "type": "string"},
        ],
    }
    fastavro.parse_schema(model_avro)
    assert JsonSchema(Model.schema()).to_avro() == model_avro


def test_optional_and_default_model():
    class Model(BaseModel):
        optional_int: Optional[int]
        default_int: int = 1000

    model_avro = {
        "namespace": "base",
        "name": "Model",
        "type": "record",
        "fields": [
            {"name": "optional_int", "type": ["null", "long"], "default": None},
            {"name": "default_int", "type": "long", "default": 1000},
        ],
    }
    fastavro.parse_schema(model_avro)
    assert JsonSchema(Model.schema()).to_avro() == model_avro


def test_bytes_and_float_model():
    class Model(BaseModel):
        bytes_field: bytes
        float_field: float

    model_avro = {
        "namespace": "base",
        "name": "Model",
        "type": "record",
        "fields": [
            {"name": "bytes_field", "type": "bytes"},
            {"name": "float_field", "type": "double"},
        ],
    }
    fastavro.parse_schema(model_avro)
    assert JsonSchema(Model.schema()).to_avro() == model_avro


class StrEnum(str, Enum):
    first = "F"
    second = "S"


@pytest.mark.xfail(reason="Avro schema doesnt support enums well.")
def test_enum_model():
    class Model(BaseModel):
        string_enum: StrEnum

    model_avro = {
        "namespace": "base",
        "name": "Model",
        "type": "record",
        "fields": [
            {
                "name": "string_enum",
                "type": {
                    "name": "StrEnum",
                    "type": "enum",
                    "symbols": ["F", "S"],
                    "doc": "An enumeration.",
                },
            }
        ],
    }
    fastavro.parse_schema(model_avro)
    assert JsonSchema(Model.schema()).to_avro() == model_avro


def test_new_enum_model():
    class Model(BaseModel):
        string_enum: StrEnum

    model_avro = {
        "namespace": "base",
        "name": "Model",
        "type": "record",
        "fields": [{"name": "string_enum", "type": "string"}],
    }
    fastavro.parse_schema(model_avro)
    assert JsonSchema(Model.schema()).to_avro() == model_avro


class IntegerEnum(IntEnum):
    first = 10
    second = 20


class NoTypeEnum(Enum):
    first = 10
    second = "twenty"


@pytest.mark.xfail(reason="Avro schema doesnt support enums well.")
def test_int_enum_model():
    class Model(BaseModel):
        int_enum: IntegerEnum

    with pytest.raises(TypeError):
        JsonSchema(Model.schema()).to_avro()


def test_new_int_enum_model():
    class Model(BaseModel):
        int_enum: IntegerEnum

    model_avro = {
        "namespace": "base",
        "name": "Model",
        "type": "record",
        "fields": [{"name": "int_enum", "type": "long"}],
    }
    fastavro.parse_schema(model_avro)
    assert JsonSchema(Model.schema()).to_avro() == model_avro


def test_no_type_enum_model():
    class Model(BaseModel):
        e: NoTypeEnum

    with pytest.raises(TypeError):
        JsonSchema(Model.schema()).to_avro()


def test_list_model():
    class Model(BaseModel):
        list_of_int: List[int]

    model_avro = {
        "namespace": "base",
        "name": "Model",
        "type": "record",
        "fields": [
            {
                "name": "list_of_int",
                "type": {"type": "array", "items": {"type": "long"}},
            }
        ],
    }
    fastavro.parse_schema(model_avro)
    assert JsonSchema(Model.schema()).to_avro() == model_avro


def test_union_model():
    class Model(BaseModel):
        union_of_int_and_string: Union[int, str]

    model_avro = {
        "namespace": "base",
        "name": "Model",
        "type": "record",
        "fields": [
            {
                "name": "union_of_int_and_string",
                "type": [{"type": "long"}, {"type": "string"}],
            }
        ],
    }
    fastavro.parse_schema(model_avro)
    assert JsonSchema(Model.schema()).to_avro() == model_avro


class OneIntModel(BaseModel):
    int_field: int


def test_simple_recursive_model():
    class Model(BaseModel):
        another_model: OneIntModel

    model_avro = {
        "namespace": "base",
        "name": "Model",
        "type": "record",
        "fields": [
            {
                "name": "another_model",
                "type": {
                    "namespace": "base",
                    "name": "OneIntModel",
                    "type": "record",
                    "fields": [{"name": "int_field", "type": "long"}],
                },
            }
        ],
    }
    fastavro.parse_schema(model_avro)
    assert JsonSchema(Model.schema()).to_avro() == model_avro


def test_with_default_object_model():
    class Model(BaseModel):
        second: OneIntModel = OneIntModel(int_field=10)

    with pytest.raises(TypeError):
        print(Model.schema())
        print(JsonSchema(Model.schema()).to_avro())


class LayeredModel(BaseModel):
    one_int: OneIntModel
    string_field: str


def test_two_layer_recursive_model():
    class Model(BaseModel):
        layerd: LayeredModel

    model_avro = {
        "namespace": "base",
        "name": "Model",
        "type": "record",
        "fields": [
            {
                "name": "layerd",
                "type": {
                    "namespace": "base",
                    "name": "LayeredModel",
                    "type": "record",
                    "fields": [
                        {
                            "name": "one_int",
                            "type": {
                                "namespace": "base",
                                "name": "OneIntModel",
                                "type": "record",
                                "fields": [{"name": "int_field", "type": "long"}],
                            },
                        },
                        {"name": "string_field", "type": "string"},
                    ],
                },
            }
        ],
    }
    fastavro.parse_schema(model_avro)
    assert JsonSchema(Model.schema()).to_avro() == model_avro


def test_optional_self_reference_model():
    class Model(BaseModel):
        value: int
        next_item: Optional[Model] = None

    Model.update_forward_refs()

    model_avro = {
        "namespace": "base",
        "name": "Model",
        "type": "record",
        "fields": [
            {"name": "value", "type": "long"},
            {
                "name": "next_item",
                "type": ["null", {"name": "base.Model", "type": "record",}],
                "default": None,
            },
        ],
    }
    fastavro.parse_schema(model_avro)
    assert JsonSchema(Model.schema()).to_avro() == model_avro


def test_outside_reference():
    with pytest.raises(TypeError):
        JsonSchema({"$ref": "#address"}).to_avro()


def test_invalid_reference():
    with pytest.raises(KeyError):
        JsonSchema({"$ref": "#/definitions/Model"}).to_avro()


def test_dict_model():
    class Model(BaseModel):
        dict_field: Dict[str, float]

    model_avro = {
        "namespace": "base",
        "name": "Model",
        "type": "record",
        "fields": [
            {
                "name": "dict_field",
                "type": {"type": "map", "values": {"type": "double"}},
            }
        ],
    }
    fastavro.parse_schema(model_avro)
    assert JsonSchema(Model.schema()).to_avro() == model_avro


def test_documentation_model():
    class Model(BaseModel):
        """This is a documentation"""

        string_field: str

    model_avro = {
        "namespace": "base",
        "name": "Model",
        "type": "record",
        "doc": "This is a documentation",
        "fields": [{"name": "string_field", "type": "string"}],
    }
    fastavro.parse_schema(model_avro)
    assert JsonSchema(Model.schema()).to_avro() == model_avro


def test_logical_types_model():
    class Model(BaseModel):
        uuid_field: uuid.UUID
        date_field: datetime.date
        time_field: datetime.time
        datetime_field: datetime.datetime

    model_avro = {
        "namespace": "base",
        "name": "Model",
        "type": "record",
        "fields": [
            {"name": "uuid_field", "type": {"type": "string", "logicalType": "uuid"}},
            {"name": "date_field", "type": {"type": "int", "logicalType": "date"}},
            {
                "name": "time_field",
                "type": {"type": "long", "logicalType": "time-micros"},
            },
            {
                "name": "datetime_field",
                "type": {"type": "long", "logicalType": "timestamp-micros"},
            },
        ],
    }
    fastavro.parse_schema(model_avro)
    assert JsonSchema(Model.schema()).to_avro() == model_avro
