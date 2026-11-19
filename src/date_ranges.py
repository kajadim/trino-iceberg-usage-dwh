"""UTC date-window helpers shared by the fact and aggregate transforms."""

import argparse
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from pyiceberg.expressions import And, BooleanExpression, GreaterThanOrEqual, LessThan


@dataclass(frozen=True)
class DateRange:
    """A start-inclusive, end-exclusive range of UTC event dates."""

    start_date: date
    end_date: date

    def __post_init__(self) -> None:
        if self.end_date <= self.start_date:
            raise ValueError("end_date must be later than start_date")

    @property
    def start_timestamp(self) -> datetime:
        return datetime.combine(self.start_date, time.min, tzinfo=timezone.utc)

    @property
    def end_timestamp(self) -> datetime:
        return datetime.combine(self.end_date, time.min, tzinfo=timezone.utc)

    @property
    def iceberg_filter(self) -> BooleanExpression:
        return And(
            GreaterThanOrEqual("event_date", self.start_date),
            LessThan("event_date", self.end_date),
        )


def add_date_range_arguments(parser: argparse.ArgumentParser) -> None:
    """Add single-day, explicit-range, and recent-lookback options to a CLI."""
    parser.add_argument("--date", type=date.fromisoformat)
    parser.add_argument("--start-date", type=date.fromisoformat)
    parser.add_argument("--end-date", type=date.fromisoformat)
    parser.add_argument("--lookback-days", type=int, default=3)


def date_range_from_args(args: Any, today: date | None = None) -> DateRange:
    """Resolve CLI date arguments or default to the most recent three UTC dates."""
    if args.lookback_days < 1:
        raise ValueError("lookback_days must be at least 1")
    if args.date is not None:
        if args.start_date is not None or args.end_date is not None:
            raise ValueError("--date cannot be combined with --start-date or --end-date")
        return DateRange(args.date, args.date + timedelta(days=1))
    if (args.start_date is None) != (args.end_date is None):
        raise ValueError("--start-date and --end-date must be provided together")
    if args.start_date is not None and args.end_date is not None:
        return DateRange(args.start_date, args.end_date)

    current_date = today or datetime.now(timezone.utc).date()
    return DateRange(
        current_date - timedelta(days=args.lookback_days - 1),
        current_date + timedelta(days=1),
    )
