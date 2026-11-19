"""Run raw ingestion and refresh facts and aggregates for a UTC date range."""

import argparse
from pathlib import Path

from src.config import SAMPLE_EVENTS_PATH
from src.create_tables import create_all_tables
from src.date_ranges import add_date_range_arguments, date_range_from_args
from src.duckdb_utils import iceberg_connection
from src.generate_sample_events import generate_events, parse_as_of, write_events
from src.ingest_raw import ingest_events
from src.load_dimensions import load_dimensions
from src.transform_daily_agg import transform_daily_agg
from src.transform_fact import transform_fact


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    add_date_range_arguments(parser)
    parser.add_argument("--count", type=int, default=3000)
    parser.add_argument("--days", type=int, default=60)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--as-of", type=parse_as_of)
    parser.add_argument("--output", type=Path, default=SAMPLE_EVENTS_PATH)
    args = parser.parse_args()

    try:
        date_range = date_range_from_args(args)
    except ValueError as error:
        parser.error(str(error))

    create_all_tables()
    events = generate_events(args.count, args.days, args.seed, args.as_of)
    write_events(events, args.output)
    appended = ingest_events(args.output)
    dimensions = load_dimensions()
    with iceberg_connection() as connection:
        fact_rows = transform_fact(date_range, connection=connection)
        aggregate_rows = transform_daily_agg(date_range, connection=connection)
    print(f"Raw ingestion added {appended} new events")
    print(f"Dimension row counts: {dimensions}")
    print(f"Fact rows refreshed: {fact_rows}")
    print(f"Daily aggregate rows refreshed: {aggregate_rows}")


if __name__ == "__main__":
    main()
