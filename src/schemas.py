"""Iceberg schemas and partition specifications."""

from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform
from pyiceberg.types import (
    BooleanType,
    DateType,
    DoubleType,
    ListType,
    LongType,
    NestedField,
    StringType,
    StructType,
    TimestamptzType,
)

RAW_QUERY_EVENTS_SCHEMA = Schema(
    NestedField(field_id=1, name="query_id", field_type=StringType(), required=True),
    NestedField(field_id=2, name="user_name", field_type=StringType(), required=True),
    NestedField(field_id=3, name="create_time", field_type=TimestamptzType(), required=True),
    NestedField(field_id=4, name="end_time", field_type=TimestamptzType(), required=False),
    NestedField(field_id=5, name="query_text", field_type=StringType(), required=False),
    NestedField(field_id=6, name="query_state", field_type=StringType(), required=True),
    NestedField(field_id=7, name="cpu_time_ms", field_type=LongType(), required=False),
    NestedField(field_id=8, name="wall_time_ms", field_type=LongType(), required=False),
    NestedField(field_id=9, name="peak_memory_bytes", field_type=LongType(), required=False),
    NestedField(field_id=10, name="processed_bytes", field_type=LongType(), required=False),
    NestedField(field_id=11, name="processed_rows", field_type=LongType(), required=False),
    NestedField(
        field_id=12,
        name="referenced_tables",
        field_type=ListType(
            element_id=13,
            element=StructType(
                NestedField(field_id=14, name="catalog_name", field_type=StringType(), required=True),
                NestedField(field_id=15, name="schema_name", field_type=StringType(), required=True),
                NestedField(field_id=16, name="table_name", field_type=StringType(), required=True),
                NestedField(field_id=17, name="input_bytes", field_type=LongType(), required=False),
                NestedField(field_id=18, name="input_rows", field_type=LongType(), required=False),
            ),
            element_required=True,
        ),
        required=True,
    ),
)

RAW_QUERY_EVENTS_PARTITION_SPEC = PartitionSpec(
    PartitionField(
        source_id=3,
        field_id=1000,
        transform=DayTransform(),
        name="create_date",
    )
)

DIM_USER_SCHEMA = Schema(
    NestedField(field_id=1, name="user_key", field_type=StringType(), required=True),
    NestedField(field_id=2, name="user_name", field_type=StringType(), required=True),
    NestedField(field_id=3, name="team_key", field_type=StringType(), required=True),
    NestedField(field_id=4, name="valid_from", field_type=TimestamptzType(), required=True),
    NestedField(field_id=5, name="valid_to", field_type=TimestamptzType(), required=False),
)

DIM_TEAM_SCHEMA = Schema(
    NestedField(field_id=1, name="team_key", field_type=StringType(), required=True),
    NestedField(field_id=2, name="team_name", field_type=StringType(), required=True),
)

DIM_TABLE_SCHEMA = Schema(
    NestedField(field_id=1, name="table_key", field_type=StringType(), required=True),
    NestedField(field_id=2, name="catalog_name", field_type=StringType(), required=True),
    NestedField(field_id=3, name="schema_name", field_type=StringType(), required=True),
    NestedField(field_id=4, name="table_name", field_type=StringType(), required=True),
    NestedField(field_id=5, name="owner", field_type=StringType(), required=True),
    NestedField(field_id=6, name="domain", field_type=StringType(), required=True),
    NestedField(field_id=7, name="source_type", field_type=StringType(), required=True),
    NestedField(field_id=8, name="is_iceberg", field_type=BooleanType(), required=True),
)

FACT_TABLE_ACCESS_SCHEMA = Schema(
    NestedField(field_id=1, name="query_id", field_type=StringType(), required=True),
    NestedField(field_id=2, name="table_key", field_type=StringType(), required=True),
    NestedField(field_id=3, name="user_key", field_type=StringType(), required=True),
    NestedField(field_id=4, name="team_key", field_type=StringType(), required=True),
    NestedField(field_id=5, name="event_date", field_type=DateType(), required=True),
    NestedField(field_id=6, name="create_time", field_type=TimestamptzType(), required=True),
    NestedField(
        field_id=7,
        name="allocated_cpu_time_ms",
        field_type=DoubleType(),
        required=False,
    ),
    NestedField(
        field_id=8,
        name="allocated_wall_time_ms",
        field_type=DoubleType(),
        required=False,
    ),
    NestedField(
        field_id=9,
        name="allocated_processed_bytes",
        field_type=DoubleType(),
        required=False,
    ),
    NestedField(
        field_id=10,
        name="allocated_processed_rows",
        field_type=DoubleType(),
        required=False,
    ),
    NestedField(
        field_id=11,
        name="table_input_bytes",
        field_type=DoubleType(),
        required=False,
    ),
    NestedField(
        field_id=12,
        name="table_input_rows",
        field_type=DoubleType(),
        required=False,
    ),
    NestedField(
        field_id=13,
        name="is_multi_table_query",
        field_type=BooleanType(),
        required=True,
    ),
    NestedField(field_id=14, name="is_success", field_type=BooleanType(), required=True),
)

FACT_TABLE_ACCESS_PARTITION_SPEC = PartitionSpec(
    PartitionField(
        source_id=5,
        field_id=1000,
        transform=DayTransform(),
        name="event_day",
    )
)

AGG_TABLE_USAGE_DAILY_SCHEMA = Schema(
    NestedField(field_id=1, name="event_date", field_type=DateType(), required=True),
    NestedField(field_id=2, name="table_key", field_type=StringType(), required=True),
    NestedField(field_id=3, name="team_key", field_type=StringType(), required=True),
    NestedField(field_id=4, name="distinct_users", field_type=LongType(), required=True),
    NestedField(field_id=5, name="query_count", field_type=LongType(), required=True),
    NestedField(field_id=6, name="cpu_seconds", field_type=DoubleType(), required=False),
    NestedField(field_id=7, name="wall_time_seconds", field_type=DoubleType(), required=False),
    NestedField(field_id=8, name="bytes_processed", field_type=DoubleType(), required=False),
    NestedField(field_id=9, name="rows_processed", field_type=DoubleType(), required=False),
)

AGG_TABLE_USAGE_DAILY_PARTITION_SPEC = PartitionSpec(
    PartitionField(
        source_id=1,
        field_id=1000,
        transform=DayTransform(),
        name="event_day",
    )
)
