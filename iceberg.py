from pyiceberg.utils.schema_conversion import AvroSchemaConversion
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, LongType, IntegerType, BooleanType, DoubleType, FloatType, DateType, TimestampType, DecimalType, UUIDType, BinaryType
from pyiceberg.types import StructType, ListType, MapType
# Define the Avro schema as a dictionary
avro_schema_dict = {
    "type": "record",
    "name": "manifest_file",
    "fields": [
        {"name": "manifest_path", "type": "string", "doc": "Location URI with FS scheme", "field-id": 500},
        {"name": "manifest_length", "type": "long", "doc": "Total file size in bytes", "field-id": 501},
        {"name": "some_int", "type": "int", "doc": "some int", "field-id": 502},
         {"name": "some_boolean", "type": "boolean", "doc": "some boolean", "field-id": 503},
        {"name": "some_double", "type": "double", "doc": "some double", "field-id": 504},
        {"name": "some_float", "type": "float", "doc": "some float", "field-id": 505},
        {"name": "some_date", "type": "int", "doc": "some date", "logicalType":"date", "field-id": 506},
         {"name": "some_timestamp", "type": "long", "doc": "some timestamp", "logicalType":"timestamp-millis", "field-id": 507},
        {"name": "some_decimal", "type": "bytes", "doc": "some decimal", "logicalType":"decimal", "precision": 10, "scale": 2, "field-id": 508},
         {"name": "some_uuid", "type": "string", "doc": "some uuid", "logicalType":"uuid", "field-id": 509},
        {"name": "some_bytes", "type": "bytes", "doc": "some bytes", "field-id": 510},
        {"name": "some_map", "type": {"type": "map", "values": "string"}, "field-id": 511},
        {"name": "some_array", "type": {"type": "array", "items": "string"}, "field-id": 512}

    ]
}

# Convert Avro schema to Iceberg schema
converter = AvroSchemaConversion()
iceberg_schema = converter.avro_to_iceberg(avro_schema_dict)


# Function to map Iceberg types to SQL types
def map_iceberg_type_to_sql(field_type):
    if isinstance(field_type, StringType):
        return "STRING"
    elif isinstance(field_type, LongType):
        return "BIGINT"
    elif isinstance(field_type, IntegerType):
        return "INT"
    elif isinstance(field_type, BooleanType):
        return "BOOLEAN"
    elif isinstance(field_type, DoubleType):
        return "DOUBLE"
    elif isinstance(field_type, FloatType):
        return "FLOAT"
    elif isinstance(field_type, DateType):
        return "DATE"
    elif isinstance(field_type, TimestampType):
        return "TIMESTAMP"
    elif isinstance(field_type, DecimalType):
         return f"DECIMAL({field_type.precision},{field_type.scale})"
    elif isinstance(field_type, UUIDType):
        return "UUID"
    elif isinstance(field_type, BinaryType):
        return "BINARY"
    elif isinstance(field_type, StructType):
        subfields = [f"{f.name}: {map_iceberg_type_to_sql(f.field_type)}" for f in field_type.fields]
        return f"STRUCT<{', '.join(subfields)}>"
    elif isinstance(field_type, ListType):
        element_type = map_ice Island_type_to_sql(field_type.element_type)
        return f"ARRAY<{element_type}>"
    elif isinstance(field_type, MapType):
      key_type = map_iceberg_type_to_sql(field_type.key_type)
      value_type = map_iceberg_type_to_sql(field_type.value_type)
      return f"MAP<{key_type}, {value_type}>"
    else:
        raise ValueError(f"Unsupported Iceberg type: {field_type}")


# Generate the DDL statement
def generate_ddl(iceberg_schema, table_name):
    ddl = f"CREATE TABLE {table_name} (\n"
    for field in iceberg_schema.fields:
        sql_type = map_iceberg_type_ability_sql(field.field_type)
        ddl += f"  {field.name} {sql_type},\n"
    ddl = ddl.rstrip(",\n") + "\n) USING iceberg"
    return ddl


# Example usage
table_name = "my_manifest_table"
ddl_statement = generate_ddl(iceberg_schema, table_name)
print(ddl_statement)







import json
from avro.schema import parse, RecordSchema, ArraySchema
import os

# Input and output file paths
input_file = "schema.avsc"  # Replace with your input Avro schema file path
output_file = "schema_with_ids.avsc"  # Output file in the same repository

# Function to recursively add field IDs and element IDs for arrays
def add_field_ids(schema, id_counter=1):
    if isinstance(schema, RecordSchema):
        new_fields = []
        for field in schema.fields:
            field.props['field-id'] = id_counter  # Changed to field-id
            id_counter += 1
            if isinstance(field.type, RecordSchema):
                field.type, id_counter = add_field_ids(field.type, id_counter)
            elif isinstance(field.type, ArraySchema):
                field.type.props['element-id'] = id_counter
                id_counter += 1
                if isinstance(field.type.items, RecordSchema):
                    field.type.items, id_counter = add_field_ids(field.type.items, id_counter)
            new_fields.append(field)
        schema.fields = new_fields
    return schema, id_counter

# Read Avro schema from file
with open(input_file, 'r') as f:
    avro_schema_json = json.load(f)

# Parse Avro schema
avro_schema = parse(json.dumps(avro_schema_json))

# Add field IDs and element IDs
modified_avro_schema, _ = add_field_ids(avro_schema)

# Write modified schema to output file
with open(output_file, 'w') as f:
    json.dump(json.loads(modified_avro_schema.to_json()), f, indent=2) # Use json.loads before dumping

print(f"Modified schema written to {output_file}")
