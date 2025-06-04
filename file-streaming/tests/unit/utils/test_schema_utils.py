from pyspark.sql.types import *
import pytest

from file_streaming.utils.exceptions import IllegalArgumentException
from file_streaming.utils.schema_utils import schema_from_config


def test_simple_schema():
    fields = [
        {'name': 'channel', 'type': 'string'},
        {'name': 'receivedAt', 'type': 'timestamp'}
    ]

    schema = schema_from_config(fields)

    assert schema == StructType([
        StructField('channel', StringType(), nullable=True),
        StructField('receivedAt', TimestampType(), nullable=True)
    ])


def test_nested_schema():
    fields = [
        {'name': 'channel', 'type': 'string'},
        {'name': 'channel', 'type': 'string'},
        {'name': 'context', 'type': 'struct', 'properties': [
            {'name': 'library', 'type': 'struct', 'properties': [
                {'name': 'name', 'type': 'string'},
                {'name': 'version', 'type': 'string'}
            ]}
        ]},
        {'name': 'receivedAt', 'type': 'timestamp'}
    ]

    schema = schema_from_config(fields)

    assert schema == StructType([
        StructField('channel', StringType(), nullable=True),
        StructField('context', StructType([
            StructField('library', StructType([
                StructField('name', StringType(), nullable=True),
                StructField('version', StringType(), nullable=True)]
            ), nullable=True)
        ]), nullable=True),
        StructField('receivedAt', TimestampType(), nullable=True)
    ])


def test_unsupported_data_type():
    fields = [
        {'name': 'channel', 'type': 'string'},
        {'name': 'receivedAt', 'type': 'binary'}
    ]
    with pytest.raises(IllegalArgumentException, match="Requested type=binary not found"):
        return schema_from_config(fields)
