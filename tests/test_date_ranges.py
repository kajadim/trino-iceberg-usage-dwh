"""Tests for incremental UTC date-range selection."""

import unittest
from argparse import Namespace
from datetime import date, datetime, timezone

from src.date_ranges import DateRange, date_range_from_args


class DateRangeTests(unittest.TestCase):
    def test_single_date_is_a_one_day_half_open_range(self) -> None:
        args = Namespace(
            date=date(2026, 11, 19),
            start_date=None,
            end_date=None,
            lookback_days=3,
        )

        selected = date_range_from_args(args)

        self.assertEqual(selected, DateRange(date(2026, 11, 19), date(2026, 11, 20)))
        self.assertEqual(
            selected.start_timestamp,
            datetime(2026, 11, 19, tzinfo=timezone.utc),
        )

    def test_default_lookback_includes_today_and_previous_dates(self) -> None:
        args = Namespace(
            date=None,
            start_date=None,
            end_date=None,
            lookback_days=3,
        )

        selected = date_range_from_args(args, today=date(2026, 11, 19))

        self.assertEqual(selected, DateRange(date(2026, 11, 17), date(2026, 11, 20)))

    def test_partial_or_invalid_ranges_are_rejected(self) -> None:
        args = Namespace(
            date=None,
            start_date=date(2026, 11, 19),
            end_date=None,
            lookback_days=3,
        )

        with self.assertRaisesRegex(ValueError, "provided together"):
            date_range_from_args(args)


if __name__ == "__main__":
    unittest.main()
