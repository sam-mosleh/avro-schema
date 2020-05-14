from typing import Any, Optional


class JsonSchema:
    def __init__(self, json_schema: dict, namespace: str = 'base'):
        self.schema = json_schema
        self.namespace = namespace
        self._parsed_objects = set()

    def to_avro(self) -> dict:
        return self._get_avro_type_and_call(self.schema)

    def _get_avro_type_and_call(self,
                                schema: dict,
                                name: Optional[str] = None,
                                required: bool = True,
                                namespace: Optional[str] = None) -> dict:
        type_field = schema.get('type')
        default = schema.get('default')
        if type_field == 'object':
            if 'properties' in schema:
                return self._json_object_to_avro_record(schema, namespace)
            else:
                return self._json_dict_to_avro_map(name, schema)
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
        elif type_field == 'string':
            return self._json_primitive_type_to_avro_field(
                name, 'string', required, default)
        elif 'anyOf' in schema:
            return self._json_anyof_to_avro_union(name, schema)
        elif 'allOf' in schema:
            raise TypeError(
                'Avro schema cannot have nested objects with default values.')
        elif '$ref' in schema:
            return self._json_ref_to_avro_record(name, schema, required)
        else:
            raise TypeError(f"Unknown type.")

    def _json_object_to_avro_record(self,
                                    json_object: dict,
                                    namespace: Optional[str] = None) -> dict:
        required_field = json_object.get('required', [])
        title = json_object['title']
        if namespace is None:
            namespace = self.namespace
        record_namespace = f"{namespace}.{title}"
        if record_namespace in self._parsed_objects:
            return {'type': 'record', 'name': record_namespace}
        else:
            self._parsed_objects.add(record_namespace)
        return {
            'namespace':
            namespace,
            'name':
            title,
            'type':
            'record',
            'fields': [
                self._get_avro_type_and_call(property_data, property_name,
                                             property_name in required_field,
                                             record_namespace)
                for property_name, property_data in
                json_object['properties'].items()
            ]
        }

    def _json_dict_to_avro_map(self, name: str, schema: dict):
        result = {'name': name} if name is not None else {}
        result['type'] = {
            'type': 'map',
            'values':
            self._get_avro_type_and_call(schema['additionalProperties'])
        }
        return result

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
        result = {'name': name} if name is not None else {}
        result['type'] = {
            'type': 'array',
            'items': self._get_avro_type_and_call(schema['items'])
        }
        return result

    def _json_enum_to_avro_enum(self, name: str, schema: dict):
        return {
            'name': name,
            'type': {
                'name': name,
                'type': 'enum',
                'symbols': schema['enum']
            }
        }

    def _json_anyof_to_avro_union(self, name: Optional[str], schema: dict):
        result = {'name': name} if name is not None else {}
        result['type'] = [
            self._get_avro_type_and_call(item) for item in schema['anyOf']
        ]
        return result

    def _json_ref_to_avro_record(self, name: str, schema: dict,
                                 required: bool):
        selected_schema = self._find(schema['$ref'])
        if name is not None:
            return {
                'name':
                name,
                'type':
                self._get_avro_type_and_call(selected_schema) if required else
                ['null', self._get_avro_type_and_call(selected_schema)]
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
