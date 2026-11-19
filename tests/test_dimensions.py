"""Tests for SCD type 2 user/team dimension behavior."""

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from src.load_dimensions import (
    build_user_dimension_rows,
    read_user_team_mappings,
)


class DimensionTests(unittest.TestCase):
    def test_scd2_mapping_and_unmapped_user_are_resolved_at_event_time(self) -> None:
        mappings = read_user_team_mappings()
        observed_users = [
            ("user_01", datetime(2026, 10, 19, tzinfo=timezone.utc)),
            ("user_01", datetime(2026, 10, 21, tzinfo=timezone.utc)),
            ("unknown_user_01", datetime(2026, 11, 1, tzinfo=timezone.utc)),
        ]

        rows = build_user_dimension_rows(mappings, observed_users)

        def assigned_team(user_name: str, event_time: datetime) -> str:
            return next(
                row["team_key"]
                for row in rows
                if row["user_name"] == user_name
                and row["valid_from"] <= event_time
                and (row["valid_to"] is None or event_time < row["valid_to"])
            )

        self.assertEqual(
            assigned_team("user_01", observed_users[0][1]),
            "analytics",
        )
        self.assertEqual(
            assigned_team("user_01", observed_users[1][1]),
            "platform",
        )
        self.assertEqual(
            assigned_team("unknown_user_01", observed_users[2][1]),
            "UNKNOWN",
        )
        self.assertTrue(any(row["user_name"] == "unknown_user_01" for row in rows))

    def test_overlapping_scd2_intervals_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "mapping.csv"
            path.write_text(
                "user_name,team_name,valid_from,valid_to\n"
                "user_01,analytics,2026-01-01T00:00:00+00:00,\n"
                "user_01,platform,2026-01-01T00:00:00+00:00,\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "Overlapping"):
                read_user_team_mappings(path)


if __name__ == "__main__":
    unittest.main()
