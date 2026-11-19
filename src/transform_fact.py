"""Transform raw query events into query-table access facts."""

import argparse
from pathlib import Path

import duckdb
import pyarrow as pa

from src.config import (
    DIM_TABLE_TABLE,
    DIM_TEAM_TABLE,
    DIM_USER_TABLE,
    FACT_TABLE_ACCESS_TABLE,
    RAW_QUERY_EVENTS_TABLE,
    PROJECT_ROOT,
    load_project_catalog,
)
from src.create_tables import create_all_tables
from src.date_ranges import DateRange, add_date_range_arguments, date_range_from_args
from src.duckdb_utils import iceberg_connection
from src.iceberg_utils import (
    OUTPUT_ROW_COUNT_PROPERTY,
    fingerprint_files,
    matching_snapshot_row_count,
    replace_date_range,
)
from src.schemas import FACT_TABLE_ACCESS_SCHEMA

FACT_SQL_PATH = PROJECT_ROOT / "sql" / "fact_table_access.sql"
DIMENSION_KEY_COLUMNS = ("table_key", "user_key", "team_key")
ALLOCATED_METRIC_COLUMNS = (
    "allocated_cpu_time_ms",
    "allocated_wall_time_ms",
    "allocated_processed_bytes",
    "allocated_processed_rows",
)


def transform_fact(
    date_range: DateRange,
    sql_path: Path = FACT_SQL_PATH,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> int:
    """Replace fact rows in a date range from the current raw and dimension snapshots."""
    tables = create_all_tables()
    catalog = load_project_catalog()
    raw_metadata_location = catalog.load_table(RAW_QUERY_EVENTS_TABLE).metadata_location
    dim_table_metadata_location = catalog.load_table(DIM_TABLE_TABLE).metadata_location
    dim_user_metadata_location = catalog.load_table(DIM_USER_TABLE).metadata_location
    dim_team_metadata_location = catalog.load_table(DIM_TEAM_TABLE).metadata_location
    fact_table = tables[FACT_TABLE_ACCESS_TABLE]
    snapshot_properties = {
        "pipeline.operation": "refresh_fact_date_range",
        "start_date": date_range.start_date.isoformat(),
        "end_date_exclusive": date_range.end_date.isoformat(),
        "pipeline.source.raw_metadata_location": raw_metadata_location,
        "pipeline.source.dim_table_metadata_location": dim_table_metadata_location,
        "pipeline.source.dim_user_metadata_location": dim_user_metadata_location,
        "pipeline.source.dim_team_metadata_location": dim_team_metadata_location,
        "pipeline.transform_fingerprint": fingerprint_files(
            Path(__file__),
            sql_path,
            Path(__file__).with_name("schemas.py"),
            Path(__file__).with_name("iceberg_utils.py"),
        ),
    }
    cached_row_count = matching_snapshot_row_count(fact_table, snapshot_properties)
    if cached_row_count is not None:
        return cached_row_count

    metadata_locations = [
        raw_metadata_location,
        date_range.start_timestamp,
        date_range.end_timestamp,
        dim_table_metadata_location,
        dim_user_metadata_location,
        dim_team_metadata_location,
    ]
    query = sql_path.read_text(encoding="utf-8")
    with iceberg_connection(connection) as active_connection:
        fact_rows = active_connection.sql(
            query,
            params=metadata_locations,
        ).to_arrow_table()

    if fact_rows.num_rows == 0:
        raise ValueError("The fact transformation produced no table-access rows")
    _validate_dimension_keys(fact_rows)
    _validate_metric_reconciliation(fact_rows)

    replace_date_range(
        fact_table,
        _conform_to_iceberg_schema(fact_rows),
        date_range,
        {
            **snapshot_properties,
            OUTPUT_ROW_COUNT_PROPERTY: str(fact_rows.num_rows),
        },
    )
    return fact_rows.num_rows


def _validate_dimension_keys(fact_rows: pa.Table) -> None:
    for column_name in DIMENSION_KEY_COLUMNS:
        null_count = fact_rows.column(column_name).null_count
        if null_count:
            raise ValueError(
                f"{null_count} fact rows have no matching {column_name} dimension key"
            )


def _validate_metric_reconciliation(fact_rows: pa.Table) -> None:
    source_columns = {
        "allocated_cpu_time_ms": "source_cpu_time_ms",
        "allocated_wall_time_ms": "source_wall_time_ms",
        "allocated_processed_bytes": "source_processed_bytes",
        "allocated_processed_rows": "source_processed_rows",
    }
    query_totals: dict[str, dict[str, float | None]] = {}
    source_values: dict[str, dict[str, float | None]] = {}

    validation_columns = ["query_id", *ALLOCATED_METRIC_COLUMNS, *source_columns.values()]
    for row in fact_rows.select(validation_columns).to_pylist():
        query_id = row["query_id"]
        allocated = query_totals.setdefault(query_id, {})
        source = source_values.setdefault(query_id, {})
        for allocated_column, source_column in source_columns.items():
            value = row[allocated_column]
            if value is not None:
                allocated[allocated_column] = allocated.get(allocated_column, 0.0) + value
            else:
                allocated.setdefault(allocated_column, None)
            source[source_column] = row[source_column]

    for query_id, allocated in query_totals.items():
        source = source_values[query_id]
        for allocated_column, source_column in source_columns.items():
            expected = source[source_column]
            actual = allocated[allocated_column]
            if expected is None:
                if actual is not None:
                    raise ValueError(
                        f"Unexpected {allocated_column} allocation for query {query_id}"
                    )
                continue
            tolerance = max(1e-6, abs(expected) * 1e-9)
            if actual is None or abs(actual - expected) > tolerance:
                raise ValueError(
                    f"{allocated_column} does not reconcile for query {query_id}: "
                    f"expected {expected}, allocated {actual}"
                )


def _conform_to_iceberg_schema(fact_rows: pa.Table) -> pa.Table:
    expected_schema = FACT_TABLE_ACCESS_SCHEMA.as_arrow()
    missing_columns = set(expected_schema.names) - set(fact_rows.schema.names)
    if missing_columns:
        raise ValueError(f"Fact query is missing columns: {sorted(missing_columns)}")
    return fact_rows.select(expected_schema.names).cast(expected_schema)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    add_date_range_arguments(parser)
    parser.add_argument("--sql", type=Path, default=FACT_SQL_PATH)
    args = parser.parse_args()

    try:
        date_range = date_range_from_args(args)
    except ValueError as error:
        parser.error(str(error))
    row_count = transform_fact(date_range, args.sql)
    print(
        f"Refreshed {row_count} query-table rows in {FACT_TABLE_ACCESS_TABLE} "
        f"for [{date_range.start_date}, {date_range.end_date})"
    )


if __name__ == "__main__":
    main()
