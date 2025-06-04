from pyspark.sql.types import *

from file_streaming.utils.exceptions import IllegalArgumentException


def schema_from_config(fields) -> StructType:
    schema = StructType()

    for field in fields:
        schema.add(
            StructField(
                field['name'],
                convert_to_sql_type(field),
                nullable=True
            )
        )

    return schema


def convert_to_sql_type(field) -> DateType:
    data_type = field['type']
    if data_type == 'array' or data_type == 'list':
        sql_type = ArrayType(StringType())
    elif data_type == 'date':
        sql_type = DateType()
    elif data_type == 'date-time' or data_type == 'time' or data_type == 'timestamp':
        sql_type = TimestampType()
    elif data_type == 'string':
        sql_type = StringType()
    elif data_type == 'int' or data_type == 'bigint' or data_type == 'integer':
        sql_type = IntegerType()
    elif data_type == 'double':
        sql_type = DoubleType()
    elif data_type == 'boolean':
        sql_type = BooleanType()
    elif data_type == 'struct':
        sql_type = schema_from_config(field['properties'])
    else:
        raise IllegalArgumentException("Requested type=" + data_type + " not found")

    return sql_type
