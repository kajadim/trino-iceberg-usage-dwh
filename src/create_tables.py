"""Create the Iceberg tables used by the usage model."""

from pyiceberg.table import Table

from src.config import (
    DIM_TABLE_TABLE,
    DIM_TEAM_TABLE,
    DIM_USER_TABLE,
    AGG_TABLE_USAGE_DAILY_TABLE,
    FACT_TABLE_ACCESS_TABLE,
    NAMESPACE,
    RAW_QUERY_EVENTS_TABLE,
    load_project_catalog,
)
from src.schemas import (
    AGG_TABLE_USAGE_DAILY_PARTITION_SPEC,
    AGG_TABLE_USAGE_DAILY_SCHEMA,
    DIM_TABLE_SCHEMA,
    DIM_TEAM_SCHEMA,
    DIM_USER_SCHEMA,
    FACT_TABLE_ACCESS_PARTITION_SPEC,
    FACT_TABLE_ACCESS_SCHEMA,
    RAW_QUERY_EVENTS_PARTITION_SPEC,
    RAW_QUERY_EVENTS_SCHEMA,
)


def create_raw_query_events_table() -> Table:
    """Create the raw query-events table if it does not already exist."""
    catalog = load_project_catalog()
    catalog.create_namespace_if_not_exists(NAMESPACE)
    return catalog.create_table_if_not_exists(
        RAW_QUERY_EVENTS_TABLE,
        schema=RAW_QUERY_EVENTS_SCHEMA,
        partition_spec=RAW_QUERY_EVENTS_PARTITION_SPEC,
    )


def create_all_tables() -> dict[str, Table]:
    """Create the raw, dimension, and fact tables if they do not exist."""
    catalog = load_project_catalog()
    catalog.create_namespace_if_not_exists(NAMESPACE)
    return {
        RAW_QUERY_EVENTS_TABLE: catalog.create_table_if_not_exists(
            RAW_QUERY_EVENTS_TABLE,
            schema=RAW_QUERY_EVENTS_SCHEMA,
            partition_spec=RAW_QUERY_EVENTS_PARTITION_SPEC,
        ),
        DIM_USER_TABLE: catalog.create_table_if_not_exists(
            DIM_USER_TABLE,
            schema=DIM_USER_SCHEMA,
        ),
        DIM_TEAM_TABLE: catalog.create_table_if_not_exists(
            DIM_TEAM_TABLE,
            schema=DIM_TEAM_SCHEMA,
        ),
        DIM_TABLE_TABLE: catalog.create_table_if_not_exists(
            DIM_TABLE_TABLE,
            schema=DIM_TABLE_SCHEMA,
        ),
        FACT_TABLE_ACCESS_TABLE: catalog.create_table_if_not_exists(
            FACT_TABLE_ACCESS_TABLE,
            schema=FACT_TABLE_ACCESS_SCHEMA,
            partition_spec=FACT_TABLE_ACCESS_PARTITION_SPEC,
        ),
        AGG_TABLE_USAGE_DAILY_TABLE: catalog.create_table_if_not_exists(
            AGG_TABLE_USAGE_DAILY_TABLE,
            schema=AGG_TABLE_USAGE_DAILY_SCHEMA,
            partition_spec=AGG_TABLE_USAGE_DAILY_PARTITION_SPEC,
        ),
    }


def main() -> None:
    tables = create_all_tables()
    for identifier, table in tables.items():
        print(f"Table is ready: {identifier}")
        print(f"Metadata location: {table.metadata_location}")


if __name__ == "__main__":
    main()
