"""Rebuild daily table/team aggregates for a UTC date range."""

import argparse
from pathlib import Path

import duckdb
import pyarrow as pa

from src.config import (
    AGG_TABLE_USAGE_DAILY_TABLE,
    DIM_USER_TABLE,
    FACT_TABLE_ACCESS_TABLE,
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
from src.schemas import AGG_TABLE_USAGE_DAILY_SCHEMA

AGG_SQL_PATH = PROJECT_ROOT / "sql" / "agg_table_usage_daily.sql"
AGGREGATE_KEY_COLUMNS = ("event_date", "table_key", "team_key")


def transform_daily_agg(
    date_range: DateRange,
    sql_path: Path = AGG_SQL_PATH,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> int:
    """Replace daily aggregate rows for the requested date range."""
    tables = create_all_tables()
    catalog = load_project_catalog()
    fact_metadata = catalog.load_table(FACT_TABLE_ACCESS_TABLE).metadata_location
    user_metadata = catalog.load_table(DIM_USER_TABLE).metadata_location
    aggregate_table = tables[AGG_TABLE_USAGE_DAILY_TABLE]
    snapshot_properties = {
        "pipeline.operation": "refresh_daily_usage_aggregate",
        "start_date": date_range.start_date.isoformat(),
        "end_date_exclusive": date_range.end_date.isoformat(),
        "pipeline.source.fact_metadata_location": fact_metadata,
        "pipeline.source.dim_user_metadata_location": user_metadata,
        "pipeline.transform_fingerprint": fingerprint_files(
            Path(__file__),
            sql_path,
            Path(__file__).with_name("schemas.py"),
            Path(__file__).with_name("iceberg_utils.py"),
        ),
    }
    cached_row_count = matching_snapshot_row_count(aggregate_table, snapshot_properties)
    if cached_row_count is not None:
        return cached_row_count

    query = sql_path.read_text(encoding="utf-8")

    with iceberg_connection(connection) as active_connection:
        aggregate_rows = active_connection.sql(
            query,
            params=[
                fact_metadata,
                user_metadata,
                date_range.start_date,
                date_range.end_date,
            ],
        ).to_arrow_table()

    _validate_aggregate_keys(aggregate_rows)
    expected_schema = AGG_TABLE_USAGE_DAILY_SCHEMA.as_arrow()
    missing_columns = set(expected_schema.names) - set(aggregate_rows.schema.names)
    if missing_columns:
        raise ValueError(f"Aggregate query is missing columns: {sorted(missing_columns)}")

    replace_date_range(
        aggregate_table,
        aggregate_rows.select(expected_schema.names).cast(expected_schema),
        date_range,
        {
            **snapshot_properties,
            OUTPUT_ROW_COUNT_PROPERTY: str(aggregate_rows.num_rows),
        },
    )
    return aggregate_rows.num_rows


def _validate_aggregate_keys(aggregate_rows: pa.Table) -> None:
    if aggregate_rows.num_rows == 0:
        return
    keys = aggregate_rows.select(AGGREGATE_KEY_COLUMNS).to_pylist()
    unique_keys = {
        tuple(row[column_name] for column_name in AGGREGATE_KEY_COLUMNS)
        for row in keys
    }
    if len(unique_keys) != aggregate_rows.num_rows:
        raise ValueError("Daily aggregate contains duplicate date/table/team keys")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    add_date_range_arguments(parser)
    parser.add_argument("--sql", type=Path, default=AGG_SQL_PATH)
    args = parser.parse_args()

    try:
        date_range = date_range_from_args(args)
    except ValueError as error:
        parser.error(str(error))
    row_count = transform_daily_agg(date_range, args.sql)
    print(
        f"Refreshed {row_count} daily aggregate rows in {AGG_TABLE_USAGE_DAILY_TABLE} "
        f"for [{date_range.start_date}, {date_range.end_date})"
    )


if __name__ == "__main__":
    main()
