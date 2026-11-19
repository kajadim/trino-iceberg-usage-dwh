"""Tests for source-aware Iceberg snapshot reuse."""

import unittest
from types import SimpleNamespace
from typing import cast

from pyiceberg.table import Table

from src.iceberg_utils import matching_snapshot_row_count


class SnapshotTableStub:
    def __init__(self, summary: dict[str, str]) -> None:
        self.summary = summary

    def current_snapshot(self) -> SimpleNamespace:
        return SimpleNamespace(summary=self.summary)


class IcebergUtilityTests(unittest.TestCase):
    def test_matching_snapshot_returns_recorded_row_count(self) -> None:
        table = cast(
            Table,
            SnapshotTableStub(
                {
                    "pipeline.source.raw": "snapshot-1",
                    "pipeline.output_row_count": "17",
                }
            ),
        )

        self.assertEqual(
            matching_snapshot_row_count(table, {"pipeline.source.raw": "snapshot-1"}),
            17,
        )

    def test_changed_sources_or_missing_row_count_force_recomputation(self) -> None:
        stale_table = cast(
            Table,
            SnapshotTableStub(
                {
                    "pipeline.source.raw": "snapshot-1",
                    "pipeline.output_row_count": "17",
                }
            ),
        )
        incomplete_table = cast(
            Table,
            SnapshotTableStub({"pipeline.source.raw": "snapshot-1"}),
        )

        self.assertIsNone(
            matching_snapshot_row_count(stale_table, {"pipeline.source.raw": "snapshot-2"})
        )
        self.assertIsNone(
            matching_snapshot_row_count(
                incomplete_table,
                {"pipeline.source.raw": "snapshot-1"},
            )
        )


if __name__ == "__main__":
    unittest.main()
