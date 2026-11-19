"""Generate deterministic synthetic Trino query events."""

import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from src.config import SAMPLE_EVENTS_PATH

ICEBERG_TABLES = [
    ("iceberg", "analytics", "orders"),
    ("iceberg", "analytics", "customers"),
    ("iceberg", "analytics", "payments"),
    ("iceberg", "analytics", "shipments"),
    ("iceberg", "analytics", "products"),
    ("iceberg", "finance", "invoices"),
    ("iceberg", "finance", "transactions"),
    ("iceberg", "finance", "budgets"),
    ("iceberg", "marketing", "campaigns"),
    ("iceberg", "marketing", "clicks"),
    ("iceberg", "marketing", "impressions"),
    ("iceberg", "operations", "inventory"),
    ("iceberg", "operations", "warehouses"),
    ("iceberg", "support", "tickets"),
    ("iceberg", "support", "agents"),
]
EXTERNAL_TABLE = ("postgres", "legacy", "customer_archive")
SAMPLE_USER_COUNT = 20
REQUIRED_EDGE_CASE_COUNT = 4


def generate_events(
    count: int = 3000,
    days: int = 60,
    seed: int = 42,
    as_of: datetime | None = None,
) -> list[dict[str, Any]]:
    """Create query events spanning the requested number of days."""
    if count < REQUIRED_EDGE_CASE_COUNT:
        raise ValueError(f"count must be at least {REQUIRED_EDGE_CASE_COUNT}")
    if days < 1:
        raise ValueError("days must be at least 1")

    end_time = as_of or datetime.now(timezone.utc)
    if end_time.tzinfo is None or end_time.utcoffset() is None:
        raise ValueError("as_of must include a timezone")
    end_time = end_time.astimezone(timezone.utc)

    rng = random.Random(seed)
    start_time = end_time - timedelta(days=days)
    available_seconds = int((end_time - start_time).total_seconds()) - 900
    events: list[dict[str, Any]] = []

    for index in range(count):
        created_at = start_time + timedelta(seconds=rng.randint(0, available_seconds))
        duration_ms = rng.randint(50, 900_000)
        table_count = rng.choices([1, 2, 3], weights=[65, 28, 7], k=1)[0]
        selected_tables = rng.sample(ICEBERG_TABLES, table_count)
        referenced_tables = [
            _table_reference(rng, table) for table in selected_tables
        ]
        state = rng.choices(["FINISHED", "FAILED"], weights=[95, 5], k=1)[0]
        events.append(
            _make_event(
                rng=rng,
                index=index,
                user_name=f"user_{rng.randint(1, SAMPLE_USER_COUNT):02d}",
                created_at=created_at,
                duration_ms=duration_ms,
                query_state=state,
                referenced_tables=referenced_tables,
            )
        )

    events[0] = _make_event(
        rng,
        0,
        "user_01",
        start_time + timedelta(hours=1),
        1200,
        "FINISHED",
        [
            _table_reference(rng, ICEBERG_TABLES[0]),
            _table_reference(rng, ICEBERG_TABLES[1]),
        ],
    )
    events[1] = _make_event(
        rng,
        1,
        "user_02",
        start_time + timedelta(days=1, hours=2),
        2500,
        "FINISHED",
        [_table_reference(rng, EXTERNAL_TABLE)],
    )
    events[2] = _make_event(
        rng,
        2,
        "user_03",
        start_time + timedelta(days=2, hours=3),
        300,
        "FAILED",
        [_table_reference(rng, ICEBERG_TABLES[2])],
    )
    events[3] = _make_event(
        rng,
        3,
        "unknown_user_01",
        start_time + timedelta(days=3, hours=4),
        5000,
        "FINISHED",
        [_table_reference(rng, ICEBERG_TABLES[3])],
    )
    return events


def write_events(events: list[dict[str, Any]], output_path: Path) -> None:
    """Write events to a JSON Lines file with ISO-8601 timestamps."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="\n") as output_file:
        for event in events:
            output_file.write(json.dumps(event, separators=(",", ":")) + "\n")


def _table_reference(
    rng: random.Random,
    table: tuple[str, str, str],
) -> dict[str, Any]:
    catalog_name, schema_name, table_name = table
    return {
        "catalog_name": catalog_name,
        "schema_name": schema_name,
        "table_name": table_name,
        "input_bytes": rng.randint(0, 5_000_000_000),
        "input_rows": rng.randint(0, 2_000_000),
    }


def _make_event(
    rng: random.Random,
    index: int,
    user_name: str,
    created_at: datetime,
    duration_ms: int,
    query_state: str,
    referenced_tables: list[dict[str, Any]],
) -> dict[str, Any]:
    table_names = [
        f"{table['catalog_name']}.{table['schema_name']}.{table['table_name']}"
        for table in referenced_tables
    ]
    source_clause = ", ".join(table_names)
    return {
        "query_id": f"query-{index:08d}",
        "user_name": user_name,
        "create_time": created_at.isoformat(),
        "end_time": (created_at + timedelta(milliseconds=duration_ms)).isoformat(),
        "query_text": f"SELECT * FROM {source_clause}",
        "query_state": query_state,
        "cpu_time_ms": rng.randint(1, max(duration_ms, 1)),
        "wall_time_ms": duration_ms,
        "peak_memory_bytes": rng.randint(1_048_576, 8_589_934_592),
        "processed_bytes": sum(table["input_bytes"] for table in referenced_tables),
        "processed_rows": sum(table["input_rows"] for table in referenced_tables),
        "referenced_tables": referenced_tables,
    }


def parse_as_of(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("as-of timestamp must include a timezone")
    return parsed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=3000)
    parser.add_argument("--days", type=int, default=60)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--as-of", type=parse_as_of)
    parser.add_argument("--output", type=Path, default=SAMPLE_EVENTS_PATH)
    args = parser.parse_args()

    events = generate_events(args.count, args.days, args.seed, args.as_of)
    write_events(events, args.output)
    print(f"Wrote {len(events)} sample events to {args.output}")


if __name__ == "__main__":
    main()
