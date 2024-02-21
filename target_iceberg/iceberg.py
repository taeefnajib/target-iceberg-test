from typing import List, Tuple, Union
import pyarrow as pa
from pyarrow import Schema as PyarrowSchema
from pyiceberg.schema import Schema as PyicebergSchema
from pyiceberg.io.pyarrow import pyarrow_to_schema


def singer_to_pyarrow_schema(self, singer_schema: dict) -> PyarrowSchema:
    """Convert singer tap json schema to pyarrow schema."""
    
    def process_anyof_schema(anyOf: List) -> Tuple[List, Union[str, None]]:
        """This function takes in original array of anyOf's schema detected
        and reduces it to the detected schema, based on rules, right now
        just detects whether it is string or not.
        """
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
        type = items.get("type")
        any_of_types = items.get("anyOf")

        if any_of_types:
            self.logger.info("array with anyof type schema detected.")
            type, _ = process_anyof_schema(anyOf=any_of_types)

        if "string" in type:
            return pa.string()
        elif "integer" in type:
            return pa.int64()
        elif "number" in type:
            return pa.float64()
        elif "boolean" in type:
            return pa.bool_()
        elif "array" in type:
            return pa.list_(
                get_pyarrow_schema_from_array(items=items.get("items"), level=level)
            )
        elif "object" in type:
            return pa.struct(
                get_pyarrow_schema_from_object(
                    properties=items.get("properties"), level=level + 1
                )
            )
        else:
            return pa.null()

    def get_pyarrow_schema_from_object(properties: dict, level: int = 0, field_id: int = 1):
        """
        Returns schema for an object.
        """
        fields = []
        for key, val in properties.items():
            field_metadata = {"PARQUET:field_id": f"{field_id}"}
            field_id += 1

            if "type" in val.keys():
                type = val["type"]
                format = val.get("format")
            elif "anyOf" in val.keys():
                type, format = process_anyof_schema(val["anyOf"])
            else:
                self.logger.warning("type information not given")
                type = ["string", "null"]

            if "integer" in type:
                fields.append(pa.field(key, pa.int64(), metadata=field_metadata))
            elif "number" in type:
                fields.append(pa.field(key, pa.float64(), metadata=field_metadata))
            elif "boolean" in type:
                fields.append(pa.field(key, pa.bool_(), metadata=field_metadata))
            elif "string" in type:
                if format and level == 0:
                    # this is done to handle explicit datetime conversion
                    # which happens only at level 1 of a record
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
            elif "array" in type:
                items = val.get("items")
                if items:
                    item_type, field_id = get_pyarrow_schema_from_array(
                        items=items, level=level, field_id=field_id
                    )
                    fields.append(
                        pa.field(key, pa.list_(item_type), metadata=field_metadata)
                    )
                else:
                    fields.append(
                        pa.field(key, pa.list_(pa.null()), metadata=field_metadata)
                    )
            elif "object" in type:
                prop = val.get("properties")
                # Recursively handle nested properties
                inner_fields, field_id = get_pyarrow_schema_from_object(
                    properties=prop, level=level + 1, field_id=field_id
                )
                fields.append(
                    pa.field(key, pa.struct(inner_fields), metadata=field_metadata)
                )

        return fields, field_id

    properties = singer_schema.get("properties")
    pyarrow_schema_fields, _ = get_pyarrow_schema_from_object(properties=properties)

    return pa.schema(pyarrow_schema_fields)


def singer_to_pyiceberg_schema(self, singer_schema: dict) -> PyicebergSchema:
    """Convert singer tap json schema to pyiceberg schema via pyarrow schema."""
    pyarrow_schema = singer_to_pyarrow_schema(self, singer_schema)
    pyiceberg_schema = pyarrow_to_schema(pyarrow_schema)
    return pyiceberg_schema