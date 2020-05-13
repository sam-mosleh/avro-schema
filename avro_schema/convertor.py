from typing import List, Optional


def json_to_avro(json_schema: dict, namespace: Optional[str] = None):
    result = {
        'name': json_schema['title'],
        'type': 'record',
        'fields': convert_all_json_properties_to_avro_fields(json_schema)
    }
    if namespace is not None:
        result['namespace'] = namespace
    return result


def convert_all_json_properties_to_avro_fields(json_schema: dict):
    required_field = json_schema.get('required', [])
    return [
        json_property_to_avro_field(property_name, property_data,
                                    property_name in required_field)
        for property_name, property_data in json_schema['properties'].items()
    ]


def json_property_to_avro_field(property_name: Optional[str],
                                property_data: dict, required: bool):
    type_field = property_data.get('type')
    ref_field = property_data.get('$ref')
    default_field = property_data.get('default')
    format_field = property_data.get('format')
    enum_field = property_data.get('enum')
    items_field = property_data.get('items')
    any_of_field = property_data.get('anyOf')

    avro_type = get_avro_type(type_field, format_field, enum_field,
                              any_of_field)
    if avro_type == 'enum':
        return json_enum_to_avro_field(property_name, enum_field)
    elif avro_type == 'array':
        return json_array_to_avro_field(property_name, items_field)
    elif avro_type == 'union':
        return json_union_to_avro_field(property_name, any_of_field)
    else:
        return json_primitive_type_to_avro_field(property_name, avro_type,
                                                 required, default_field)


def get_avro_type(type_field: str, format_field: Optional[str],
                  enum_field: Optional[str], any_of_field: Optional[str]):
    if type_field == 'integer':
        return 'long'
    elif type_field == 'number':
        return 'double'
    elif type_field == 'string' and format_field == 'binary':
        return 'bytes'
    elif enum_field is not None:
        if type_field == 'string':
            return 'enum'
        raise TypeError(f"Cannot have Enum of type {type_field}")
    elif any_of_field is not None:
        return 'union'
    else:
        return type_field


def json_enum_to_avro_field(property_name: Optional[str],
                            enum_list: List[str]):
    result = {'name': property_name} if property_name is not None else {}
    result['type'] = {
        'name': property_name,
        'type': 'enum',
        'symbols': enum_list
    }
    return result


def json_array_to_avro_field(property_name: Optional[str], items: dict):
    result = {'name': property_name} if property_name is not None else {}
    result['type'] = {
        'type': 'array',
        'items': json_property_to_avro_field(None, items, True)
    }
    return result


def json_union_to_avro_field(property_name: Optional[str],
                             any_of_list: List[dict]):
    result = {'name': property_name} if property_name is not None else {}
    result['type'] = [
        json_property_to_avro_field(None, item, True) for item in any_of_list
    ]
    return result


def json_primitive_type_to_avro_field(property_name: Optional[str],
                                      avro_type: str, required: bool,
                                      default_field):
    result = {'name': property_name} if property_name is not None else {}
    if default_field is None:
        result['type'] = avro_type if required else ['null', avro_type]
    else:
        result['type'] = avro_type
        result['default'] = default_field
    return result
