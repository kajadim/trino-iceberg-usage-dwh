"""Load SCD type 2 user/team, team, and table dimensions."""

import argparse
import csv
import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.table import Table

from src.config import (
    DIM_TABLE_TABLE,
    DIM_TEAM_TABLE,
    DIM_USER_TABLE,
    ICEBERG_SOURCE_TYPES,
    RAW_QUERY_EVENTS_TABLE,
    TABLE_CATALOG_PATH,
    USER_TEAM_MAPPING_PATH,
    load_project_catalog,
)
from src.create_tables import create_all_tables
from src.iceberg_utils import (
    OUTPUT_ROW_COUNT_PROPERTY,
    fingerprint_files,
    matching_snapshot_row_count,
)
from src.schemas import DIM_TABLE_SCHEMA, DIM_TEAM_SCHEMA, DIM_USER_SCHEMA

UNKNOWN_TEAM = "UNKNOWN"


@dataclass(frozen=True)
class UserTeamMapping:
    user_name: str
    team_name: str
    valid_from: datetime
    valid_to: datetime | None


def read_user_team_mappings(path: Path = USER_TEAM_MAPPING_PATH) -> list[UserTeamMapping]:
    """Read and validate SCD type 2 user/team mapping intervals."""
    required_columns = {"user_name", "team_name", "valid_from", "valid_to"}
    with path.open(newline="", encoding="utf-8") as mapping_file:
        reader = csv.DictReader(mapping_file)
        if reader.fieldnames is None or not required_columns.issubset(reader.fieldnames):
            raise ValueError(f"{path} must contain columns {sorted(required_columns)}")

        mappings = []
        for line_number, row in enumerate(reader, start=2):
            user_name = (row["user_name"] or "").strip()
            team_name = (row["team_name"] or "").strip()
            if not user_name or not team_name:
                raise ValueError(f"Empty user_name or team_name on line {line_number} in {path}")
            valid_from = _parse_timestamp(row["valid_from"], "valid_from", line_number)
            valid_to = (
                _parse_timestamp(row["valid_to"], "valid_to", line_number)
                if row["valid_to"]
                else None
            )
            if valid_to is not None and valid_to <= valid_from:
                raise ValueError(f"valid_to must be later than valid_from on line {line_number}")
            mappings.append(UserTeamMapping(user_name, team_name, valid_from, valid_to))

    _validate_non_overlapping_intervals(mappings, path)
    return mappings


def build_user_dimension_rows(
    mappings: list[UserTeamMapping],
    observed_users: list[tuple[str, datetime]],
) -> list[dict[str, Any]]:
    """Build mapped versions and event-time fallbacks for unmapped users."""
    mappings_by_user: dict[str, list[UserTeamMapping]] = {}
    rows_by_key: dict[str, dict[str, Any]] = {}
    for mapping in mappings:
        mappings_by_user.setdefault(mapping.user_name, []).append(mapping)
        row = _user_dimension_row(
            mapping.user_name,
            mapping.team_name,
            mapping.valid_from,
            mapping.valid_to,
        )
        rows_by_key[row["user_key"]] = row

    for user_name, create_time in observed_users:
        matching_mapping = any(
            mapping.valid_from <= create_time
            and (mapping.valid_to is None or create_time < mapping.valid_to)
            for mapping in mappings_by_user.get(user_name, [])
        )
        if not matching_mapping:
            row = _user_dimension_row(
                user_name,
                UNKNOWN_TEAM,
                create_time,
                create_time + timedelta(microseconds=1),
            )
            rows_by_key[row["user_key"]] = row

    return sorted(rows_by_key.values(), key=lambda row: (row["user_name"], row["valid_from"]))


def load_dimensions(
    user_mapping_path: Path = USER_TEAM_MAPPING_PATH,
    table_catalog_path: Path = TABLE_CATALOG_PATH,
) -> dict[str, int]:
    """Refresh dimension snapshots when their raw inputs or seeds have changed."""
    tables = create_all_tables()
    catalog = load_project_catalog()
    mappings = read_user_team_mappings(user_mapping_path)
    raw_table = catalog.load_table(RAW_QUERY_EVENTS_TABLE)
    source_properties = {
        "pipeline.source.raw_metadata_location": raw_table.metadata_location,
        "pipeline.source.user_mapping_sha256": hashlib.sha256(
            user_mapping_path.read_bytes()
        ).hexdigest(),
        "pipeline.source.table_catalog_sha256": hashlib.sha256(
            table_catalog_path.read_bytes()
        ).hexdigest(),
        "pipeline.transform_fingerprint": fingerprint_files(
            Path(__file__),
            Path(__file__).with_name("schemas.py"),
            Path(__file__).with_name("iceberg_utils.py"),
        ),
    }
    cached_row_counts = {
        table_name: matching_snapshot_row_count(tables[table_name], source_properties)
        for table_name in (DIM_USER_TABLE, DIM_TEAM_TABLE, DIM_TABLE_TABLE)
    }
    if all(row_count is not None for row_count in cached_row_counts.values()):
        return {
            table_name: row_count
            for table_name, row_count in cached_row_counts.items()
            if row_count is not None
        }

    raw_events = (
        raw_table
        .scan(selected_fields=("user_name", "create_time", "referenced_tables"))
        .to_arrow()
        .to_pylist()
    )
    observed_users = [
        (event["user_name"], event["create_time"])
        for event in raw_events
    ]
    user_rows = build_user_dimension_rows(mappings, observed_users)
    team_names = {mapping.team_name for mapping in mappings}
    team_names.add(UNKNOWN_TEAM)
    team_names.update(row["team_key"] for row in user_rows)
    team_rows = [
        {"team_key": team_name, "team_name": team_name}
        for team_name in sorted(team_names)
    ]
    table_rows = _build_table_dimension_rows(table_catalog_path, raw_events)

    _overwrite_table(tables[DIM_USER_TABLE], DIM_USER_SCHEMA, user_rows, source_properties)
    _overwrite_table(tables[DIM_TEAM_TABLE], DIM_TEAM_SCHEMA, team_rows, source_properties)
    _overwrite_table(
        tables[DIM_TABLE_TABLE],
        DIM_TABLE_SCHEMA,
        table_rows,
        source_properties,
    )
    return {
        DIM_USER_TABLE: len(user_rows),
        DIM_TEAM_TABLE: len(team_rows),
        DIM_TABLE_TABLE: len(table_rows),
    }


def _build_table_dimension_rows(
    path: Path,
    raw_events: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    required_columns = {
        "catalog_name",
        "schema_name",
        "table_name",
        "owner",
        "domain",
        "source_type",
    }
    table_rows: dict[str, dict[str, Any]] = {}
    with path.open(newline="", encoding="utf-8") as table_file:
        reader = csv.DictReader(table_file)
        if reader.fieldnames is None or not required_columns.issubset(reader.fieldnames):
            raise ValueError(f"{path} must contain columns {sorted(required_columns)}")

        for line_number, row in enumerate(reader, start=2):
            catalog_name = (row["catalog_name"] or "").strip()
            schema_name = (row["schema_name"] or "").strip()
            table_name = (row["table_name"] or "").strip()
            source_type = (row["source_type"] or "").strip().lower()
            if not all((catalog_name, schema_name, table_name, source_type)):
                raise ValueError(f"Missing table identifier or source_type on line {line_number}")
            table_key = _table_key(catalog_name, schema_name, table_name)
            if table_key in table_rows:
                raise ValueError(f"Duplicate table identifier {table_key!r} in {path}")
            table_rows[table_key] = {
                "table_key": table_key,
                "catalog_name": catalog_name,
                "schema_name": schema_name,
                "table_name": table_name,
                "owner": (row["owner"] or "").strip() or "UNKNOWN",
                "domain": (row["domain"] or "").strip() or "UNKNOWN",
                "source_type": source_type,
                "is_iceberg": source_type in ICEBERG_SOURCE_TYPES,
            }

    for event in raw_events:
        for reference in event["referenced_tables"]:
            catalog_name = reference["catalog_name"]
            schema_name = reference["schema_name"]
            table_name = reference["table_name"]
            table_key = _table_key(catalog_name, schema_name, table_name)
            table_rows.setdefault(
                table_key,
                {
                    "table_key": table_key,
                    "catalog_name": catalog_name,
                    "schema_name": schema_name,
                    "table_name": table_name,
                    "owner": "UNKNOWN",
                    "domain": "UNKNOWN",
                    "source_type": catalog_name.lower(),
                    "is_iceberg": catalog_name.lower() in ICEBERG_SOURCE_TYPES,
                },
            )
    return [table_rows[key] for key in sorted(table_rows)]


def _overwrite_table(
    table: Table,
    schema: Schema,
    rows: list[dict[str, Any]],
    source_properties: dict[str, str],
) -> None:
    arrow_table = pa.Table.from_pylist(rows, schema=schema.as_arrow())
    snapshot_properties = {
        **source_properties,
        OUTPUT_ROW_COUNT_PROPERTY: str(arrow_table.num_rows),
    }
    if table.current_snapshot() is None:
        table.append(arrow_table, snapshot_properties=snapshot_properties)
    else:
        table.overwrite(arrow_table, snapshot_properties=snapshot_properties)


def _user_dimension_row(
    user_name: str,
    team_name: str,
    valid_from: datetime,
    valid_to: datetime | None,
) -> dict[str, Any]:
    key_material = f"{user_name}\0{valid_from.isoformat()}".encode("utf-8")
    return {
        "user_key": hashlib.sha256(key_material).hexdigest(),
        "user_name": user_name,
        "team_key": team_name,
        "valid_from": valid_from,
        "valid_to": valid_to,
    }


def _table_key(catalog_name: str, schema_name: str, table_name: str) -> str:
    return f"{catalog_name}.{schema_name}.{table_name}"


def _parse_timestamp(value: str | None, field_name: str, line_number: int) -> datetime:
    if not value:
        raise ValueError(f"{field_name} is required on line {line_number}")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(
            f"Invalid {field_name} timestamp on line {line_number}: {value!r}"
        ) from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field_name} must include a timezone on line {line_number}")
    return parsed.astimezone(timezone.utc)


def _validate_non_overlapping_intervals(
    mappings: list[UserTeamMapping],
    path: Path,
) -> None:
    mappings_by_user: dict[str, list[UserTeamMapping]] = {}
    for mapping in mappings:
        mappings_by_user.setdefault(mapping.user_name, []).append(mapping)

    for user_name, user_mappings in mappings_by_user.items():
        ordered = sorted(user_mappings, key=lambda mapping: mapping.valid_from)
        for previous, current in zip(ordered, ordered[1:]):
            if previous.valid_to is None or current.valid_from < previous.valid_to:
                raise ValueError(f"Overlapping team intervals for {user_name!r} in {path}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--user-mapping", type=Path, default=USER_TEAM_MAPPING_PATH)
    parser.add_argument("--table-catalog", type=Path, default=TABLE_CATALOG_PATH)
    args = parser.parse_args()

    counts = load_dimensions(args.user_mapping, args.table_catalog)
    for table_name, count in counts.items():
        print(f"Loaded {count} rows into {table_name}")


if __name__ == "__main__":
    main()
