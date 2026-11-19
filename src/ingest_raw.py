"""Append JSON Lines query events to the raw Iceberg table."""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa

from src.config import RAW_QUERY_EVENTS_TABLE, SAMPLE_EVENTS_PATH, load_project_catalog
from src.schemas import RAW_QUERY_EVENTS_SCHEMA


def read_events(input_path: Path) -> list[dict[str, Any]]:
    """Read JSON Lines events and normalize their timestamps to UTC."""
    events: list[dict[str, Any]] = []
    seen_query_ids: set[str] = set()
    with input_path.open(encoding="utf-8") as input_file:
        for line_number, line in enumerate(input_file, start=1):
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(
                    f"Invalid JSON in {input_path} on line {line_number}"
                ) from error
            if not isinstance(event, dict):
                raise ValueError(f"Expected a JSON object on line {line_number}")
            query_id = event.get("query_id")
            if not isinstance(query_id, str) or not query_id:
                raise ValueError(f"query_id must be a non-empty string on line {line_number}")
            if query_id in seen_query_ids:
                raise ValueError(f"Duplicate query_id {query_id!r} on line {line_number}")
            seen_query_ids.add(query_id)
            event["create_time"] = _parse_timestamp(
                event.get("create_time"), "create_time", line_number
            )
            if event.get("end_time") is not None:
                event["end_time"] = _parse_timestamp(
                    event["end_time"], "end_time", line_number
                )
            events.append(event)

    if not events:
        raise ValueError(f"No query events found in {input_path}")

    return events


def events_to_arrow(events: list[dict[str, Any]]) -> pa.Table:
    """Convert normalized event records to the Iceberg schema."""
    return pa.Table.from_pylist(events, schema=RAW_QUERY_EVENTS_SCHEMA.as_arrow())


def unseen_events(
    events: list[dict[str, Any]],
    existing_query_ids: set[str],
) -> list[dict[str, Any]]:
    """Return records whose query IDs are not already present in the raw table."""
    return [event for event in events if event["query_id"] not in existing_query_ids]


def ingest_events(input_path: Path = SAMPLE_EVENTS_PATH) -> int:
    """Append only previously unseen query IDs and return the inserted row count."""
    events = read_events(input_path)
    table = load_project_catalog().load_table(RAW_QUERY_EVENTS_TABLE)
    if table.current_snapshot() is None:
        existing_query_ids: set[str] = set()
    else:
        existing_query_ids = set(
            table.scan(selected_fields=("query_id",))
            .to_arrow()
            .column("query_id")
            .to_pylist()
        )
    new_events = unseen_events(events, existing_query_ids)
    if new_events:
        table.append(events_to_arrow(new_events))
    return len(new_events)


def _parse_timestamp(value: Any, field_name: str, line_number: int) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be an ISO-8601 string on line {line_number}")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(
            f"Invalid {field_name} timestamp on line {line_number}: {value!r}"
        ) from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field_name} must include a timezone on line {line_number}")
    return parsed.astimezone(timezone.utc)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=SAMPLE_EVENTS_PATH)
    args = parser.parse_args()

    count = ingest_events(args.input)
    print(f"Appended {count} new events to {RAW_QUERY_EVENTS_TABLE}")


if __name__ == "__main__":
    main()
