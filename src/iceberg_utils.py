"""Shared Iceberg write helpers."""

import hashlib
from pathlib import Path

import pyarrow as pa
from pyiceberg.table import Table

from src.date_ranges import DateRange

OUTPUT_ROW_COUNT_PROPERTY = "pipeline.output_row_count"


def fingerprint_files(*paths: Path) -> str:
    """Return a stable digest for the transformation code and SQL inputs."""
    digest = hashlib.sha256()
    for path in paths:
        name = path.name.encode("utf-8")
        content = path.read_bytes()
        digest.update(len(name).to_bytes(4, byteorder="big"))
        digest.update(name)
        digest.update(len(content).to_bytes(8, byteorder="big"))
        digest.update(content)
    return digest.hexdigest()


def matching_snapshot_row_count(
    table: Table,
    expected_properties: dict[str, str],
) -> int | None:
    """Return the recorded output size when the latest snapshot matches its inputs."""
    snapshot = table.current_snapshot()
    if snapshot is None:
        return None

    summary = snapshot.summary
    if any(summary.get(key) != value for key, value in expected_properties.items()):
        return None

    row_count = summary.get(OUTPUT_ROW_COUNT_PROPERTY)
    if row_count is None:
        return None
    try:
        parsed_row_count = int(row_count)
    except ValueError:
        return None
    return parsed_row_count if parsed_row_count >= 0 else None


def replace_date_range(
    table: Table,
    rows: pa.Table,
    date_range: DateRange,
    snapshot_properties: dict[str, str],
) -> None:
    """Atomically replace rows in a date range, including an empty replacement."""
    if table.current_snapshot() is None:
        existing_rows = 0
    else:
        existing_rows = (
            table.scan(
                row_filter=date_range.iceberg_filter,
                selected_fields=("event_date",),
            )
            .to_arrow()
            .num_rows
        )

    if existing_rows == 0:
        if rows.num_rows:
            table.append(rows, snapshot_properties=snapshot_properties)
        return
    table.overwrite(
        rows,
        overwrite_filter=date_range.iceberg_filter,
        snapshot_properties=snapshot_properties,
    )
