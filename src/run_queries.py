"""Run the example usage reports against Iceberg fact and dimension tables."""

import argparse
from datetime import date
from pathlib import Path

import duckdb

from src.config import (
    DIM_TABLE_TABLE,
    DIM_TEAM_TABLE,
    DIM_USER_TABLE,
    FACT_TABLE_ACCESS_TABLE,
    PROJECT_ROOT,
    load_project_catalog,
)

QUERY_DIR = PROJECT_ROOT / "sql" / "queries"
REPORT_FILES = {
    "distinct-users": "distinct_users_per_table.sql",
    "team-resources": "team_resource_usage.sql",
    "weekly-trend": "usage_trend_weekly.sql",
}


def run_report(
    report: str,
    start_date: date,
    end_date: date,
    table_key: str | None = None,
    team_name: str | None = None,
) -> tuple[list[str], list[tuple[object, ...]]]:
    """Execute one parameterized report and return its columns and rows."""
    if report not in REPORT_FILES:
        raise ValueError(f"Unknown report: {report}")
    if end_date <= start_date:
        raise ValueError("end_date must be later than start_date")
    if report == "distinct-users" and not table_key:
        raise ValueError("table_key is required for the distinct-users report")
    if report == "team-resources" and not team_name:
        raise ValueError("team_name is required for the team-resources report")

    catalog = load_project_catalog()
    metadata_locations = {
        FACT_TABLE_ACCESS_TABLE: _metadata_path(
            catalog.load_table(FACT_TABLE_ACCESS_TABLE).metadata_location
        ),
        DIM_TABLE_TABLE: _metadata_path(
            catalog.load_table(DIM_TABLE_TABLE).metadata_location
        ),
        DIM_USER_TABLE: _metadata_path(
            catalog.load_table(DIM_USER_TABLE).metadata_location
        ),
        DIM_TEAM_TABLE: _metadata_path(
            catalog.load_table(DIM_TEAM_TABLE).metadata_location
        ),
    }
    parameters: list[object]
    if report == "distinct-users":
        parameters = [
            metadata_locations[FACT_TABLE_ACCESS_TABLE],
            metadata_locations[DIM_TABLE_TABLE],
            metadata_locations[DIM_USER_TABLE],
            start_date,
            end_date,
            table_key,
        ]
    elif report == "team-resources":
        parameters = [
            metadata_locations[FACT_TABLE_ACCESS_TABLE],
            metadata_locations[DIM_TEAM_TABLE],
            start_date,
            end_date,
            team_name,
        ]
    else:
        parameters = [
            metadata_locations[FACT_TABLE_ACCESS_TABLE],
            metadata_locations[DIM_TABLE_TABLE],
            metadata_locations[DIM_USER_TABLE],
            start_date,
            end_date,
        ]

    query = (QUERY_DIR / REPORT_FILES[report]).read_text(encoding="utf-8")
    with duckdb.connect() as connection:
        connection.execute("INSTALL iceberg; LOAD iceberg;")
        result = connection.execute(query, parameters)
        column_names = [description[0] for description in result.description]
        rows = result.fetchall()
    return column_names, rows


def _metadata_path(metadata_location: str) -> str:
    return str(Path(metadata_location.removeprefix("file://")).resolve())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", choices=sorted(REPORT_FILES))
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    parser.add_argument("--table-key")
    parser.add_argument("--team")
    args = parser.parse_args()

    if args.end_date <= args.start_date:
        parser.error("--end-date must be later than --start-date")
    if args.report == "distinct-users" and not args.table_key:
        parser.error("--table-key is required for distinct-users")
    if args.report == "team-resources" and not args.team:
        parser.error("--team is required for team-resources")

    columns, rows = run_report(
        args.report,
        args.start_date,
        args.end_date,
        table_key=args.table_key,
        team_name=args.team,
    )
    print("\t".join(columns))
    for row in rows:
        print("\t".join("" if value is None else str(value) for value in row))


if __name__ == "__main__":
    main()
