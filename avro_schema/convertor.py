from typing import Any, Optional


class JsonSchema:
    def __init__(self, json_schema: dict, namespace: str = 'base'):
        self.schema = json_schema
        self.namespace = namespace
        self._parsed_objects = set()

    def to_avro(self) -> dict:
        result = self._get_avro_type_and_call(self.schema)
        return result

    def _get_avro_type_and_call(self,
                                schema: dict,
                                name: Optional[str] = None,
                                required: bool = True) -> dict:
        type_field = schema.get('type')
        default = schema.get('default')
        if type_field == 'object':
            return self._json_object_to_avro_record(schema)
        elif schema.get('format') == 'binary':
            return self._json_primitive_type_to_avro_field(
                name, 'bytes', required, default)
        elif type_field == 'integer':
            return self._json_primitive_type_to_avro_field(
                name, 'long', required, default)
        elif type_field == 'number':
            return self._json_primitive_type_to_avro_field(
                name, 'double', required, default)
        elif type_field == 'array':
            return self._json_array_to_avro_array(name, schema)
        elif 'enum' in schema:
            if type_field == 'string':
                return self._json_enum_to_avro_enum(name, schema)
            raise TypeError(f"f{name} Cannot have Enum of type {type_field}")
        elif 'anyOf' in schema:
            return self._json_anyof_to_avro_union(name, schema)
        elif '$ref' in schema:
            return self._json_ref_to_avro_record(name, schema)
        elif type_field == 'string':
            return self._json_primitive_type_to_avro_field(
                name, 'string', required, default)
        else:
            raise TypeError(f"Unknown type.")

    def _json_object_to_avro_record(self, json_object: dict) -> dict:
        required_field = json_object.get('required', [])
        title = json_object['title']
        record_namespace = f"{self.namespace}.{title}"
        if record_namespace in self._parsed_objects:
            return {'type': 'record', 'name': record_namespace}
        else:
            self._parsed_objects.add(record_namespace)
        return {
            'namespace':
            self.namespace,
            'name':
            title,
            'type':
            'record',
            'fields': [
                self._get_avro_type_and_call(property_data, property_name,
                                             property_name in required_field)
                for property_name, property_data in
                json_object['properties'].items()
            ]
        }

    def _json_primitive_type_to_avro_field(
            self,
            name: Optional[str],
            object_type: str,
            required: bool,
            default: Optional[Any] = None) -> dict:
        result = {'name': name} if name is not None else {}
        if default is None:
            result['type'] = object_type if required else ['null', object_type]
        else:
            result.update({'type': object_type, 'default': default})
        return result

    def _json_array_to_avro_array(self, name: str, schema: dict):
        return {
            'name': name,
            'type': {
                'type': 'array',
                'items': self._get_avro_type_and_call(schema['items'])
            }
        }

    def _json_enum_to_avro_enum(self, name: str, schema: dict):
        return {
            'name': name,
            'type': {
                'name': name,
                'type': 'enum',
                'symbols': schema['enum']
            }
        }

    def _json_anyof_to_avro_union(self, name: str, schema: dict):
        return {
            'name':
            name,
            'type':
            [self._get_avro_type_and_call(item) for item in schema['anyOf']]
        }

    def _json_ref_to_avro_record(self, name: str, schema: dict):
        selected_schema = self._find(schema['$ref'])
        if name is not None:
            return {
                'name': name,
                'type': self._get_avro_type_and_call(selected_schema)
            }
        else:
            return self._get_avro_type_and_call(selected_schema)

    def _find(self, reference: str):
        ref_list = reference.split('/')
        selector = self.schema
        if ref_list[0] == '#':
            for specifier in ref_list[1:]:
                selector = selector[specifier]
            return selector
        else:
            raise TypeError(f"Wont resolve {ref_list[0]}")
