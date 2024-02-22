from typing import List, Tuple, Union
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import Schema as PyarrowSchema
from pyiceberg.schema import Schema as PyicebergSchema
from pyiceberg.io.pyarrow import pyarrow_to_schema
import json


def singer_to_pyarrow_schema(singer_schema: dict) -> PyarrowSchema:
    """Convert singer tap json schema to pyarrow schema."""
    def process_anyof_schema(anyOf: List) -> Tuple[List, Union[str, None]]:
        types, formats = [], []
        for val in anyOf:
            typ = val.get("type")
            if val.get("format"):
                formats.append(val["format"])
            if type(typ) is not list:
                types.append(typ)
            else:
                types.extend(typ)
        types = set(types)
        formats = list(set(formats))
        ret_type = []
        if "string" in types:
            ret_type.append("string")
        if "null" in types:
            ret_type.append("null")
        return ret_type, formats[0] if formats else None

    def get_pyarrow_schema_from_array(items: dict, level: int = 0):
        typ = items.get("type")
        any_of_types = items.get("anyOf")

        if any_of_types:
            self.logger.info("array with anyof type schema detected.")
            typ, _ = process_anyof_schema(anyOf=any_of_types)

        if "string" in typ:
            return pa.string()
        elif "integer" in typ:
            return pa.int64()
        elif "number" in typ:
            return pa.float64()
        elif "boolean" in typ:
            return pa.bool_()
        elif "array" in typ:
            return pa.list_(
                get_pyarrow_schema_from_array(items=items.get("items"), level=level)
            )
        elif "object" in typ:
            return pa.struct(
                get_pyarrow_schema_from_object(
                    properties=items.get("properties"), level=level + 1
                )
            )
        else:
            return pa.null()

    def get_pyarrow_schema_from_object(properties: dict, level: int = 0):
        fields = []
        field_id = 0
        for key, val in properties.items():
            field_id += 1
            field_metadata = {"PARQUET:field_id": f"{field_id}"}

            if "type" in val.keys():
                typ = val["type"]
                format = val.get("format")
            elif "anyOf" in val.keys():
                typ, format = process_anyof_schema(val["anyOf"])
            else:
                self.logger.info("type information not given")
                typ = ["string", "null"]

            if "integer" in typ:
                fields.append(pa.field(key, pa.int64(), metadata=field_metadata))
            elif "number" in typ:
                fields.append(pa.field(key, pa.float64(), metadata=field_metadata))
            elif "boolean" in typ:
                fields.append(pa.field(key, pa.bool_(), metadata=field_metadata))
            elif "string" in typ:
                if format and level == 0:
                    if format == "date":
                        fields.append(
                            pa.field(key, pa.date64(), metadata=field_metadata)
                        )
                    elif format == "time":
                        fields.append(
                            pa.field(key, pa.time64(), metadata=field_metadata)
                        )
                    else:
                        fields.append(
                            pa.field(
                                key,
                                pa.timestamp("us", tz="UTC"),
                                metadata=field_metadata,
                            )
                        )
                else:
                    fields.append(pa.field(key, pa.string(), metadata=field_metadata))
            elif "array" in typ:
                items = val.get("items")
                if items:
                    item_type = get_pyarrow_schema_from_array(items=items, level=level)
                    if item_type == pa.null():
                        self.logger.info(
                            f"""key: {key} is defined as list of null, while this would be
                                correct for a list of all null, it is better to define
                                exact item types for the list, if not null."""
                        )
                    fields.append(
                        pa.field(key, pa.list_(item_type), metadata=field_metadata)
                    )
                else:
                    self.logger.info(
                        f"""key: {key} is defined as a list of null, while this would be
                            correct for a list of all null, it is better to define
                            exact item types for the list, if not null."""
                    )
                    fields.append(
                        pa.field(key, pa.list_(pa.null()), metadata=field_metadata)
                    )
            elif "object" in typ:
                prop = val.get("properties")
                inner_fields = get_pyarrow_schema_from_object(
                    properties=prop, level=level + 1
                )
                if not inner_fields:
                    self.logger.info(
                        f"""key: {key} has no fields defined, this may cause
                            saving parquet failure as parquet doesn't support
                            empty/null complex types [array, structs] """
                    )
                fields.append(
                    pa.field(key, pa.struct(inner_fields), metadata=field_metadata)
                )

        return fields

    properties = singer_schema.get("properties")
    pyarrow_schema = pa.schema(get_pyarrow_schema_from_object(properties=properties))

    return pyarrow_schema


def singer_to_pyiceberg_schema(singer_schema: dict) -> PyicebergSchema:
    """Convert singer tap json schema to pyiceberg schema via pyarrow schema."""
    pyarrow_schema = singer_to_pyarrow_schema(singer_schema)
    pyiceberg_schema = pyarrow_to_schema(pyarrow_schema, name_mapping="schema.name-mapping.default")
    return pyiceberg_schema

