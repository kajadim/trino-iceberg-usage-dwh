"""Tests for sample event generation and raw ingestion conversion."""

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from src.generate_sample_events import generate_events, write_events
from src.ingest_raw import events_to_arrow, read_events, unseen_events


class RawEventTests(unittest.TestCase):
    def setUp(self) -> None:
        self.as_of = datetime(2026, 11, 19, 12, 0, tzinfo=timezone.utc)

    def test_sample_includes_required_edge_cases(self) -> None:
        events = generate_events(count=4, days=60, seed=7, as_of=self.as_of)

        self.assertEqual(len(events[0]["referenced_tables"]), 2)
        self.assertEqual(events[1]["referenced_tables"][0]["catalog_name"], "postgres")
        self.assertEqual(events[2]["query_state"], "FAILED")
        self.assertEqual(events[3]["user_name"], "unknown_user_01")

    def test_jsonl_round_trip_preserves_rows_and_arrow_types(self) -> None:
        events = generate_events(count=4, days=60, seed=7, as_of=self.as_of)
        with tempfile.TemporaryDirectory() as temporary_directory:
            output_path = Path(temporary_directory) / "events.jsonl"
            write_events(events, output_path)
            loaded_events = read_events(output_path)

        arrow_table = events_to_arrow(loaded_events)
        self.assertEqual(arrow_table.num_rows, 4)
        self.assertEqual(
            str(arrow_table.schema.field("create_time").type),
            "timestamp[us, tz=UTC]",
        )
        first_reference = arrow_table.column("referenced_tables").to_pylist()[0][0]
        self.assertEqual(first_reference["table_name"], "orders")

    def test_generator_requires_space_for_edge_cases(self) -> None:
        with self.assertRaises(ValueError):
            generate_events(count=3, days=60, seed=7, as_of=self.as_of)

    def test_ingestion_rejects_duplicate_query_ids_in_a_batch(self) -> None:
        event = generate_events(count=4, days=60, seed=7, as_of=self.as_of)[0]
        with tempfile.TemporaryDirectory() as temporary_directory:
            output_path = Path(temporary_directory) / "duplicate_events.jsonl"
            write_events([event, event], output_path)
            with self.assertRaisesRegex(ValueError, "Duplicate query_id"):
                read_events(output_path)

    def test_ingestion_skips_query_ids_already_in_raw_table(self) -> None:
        events = generate_events(count=4, days=60, seed=7, as_of=self.as_of)

        remaining = unseen_events(events, {events[0]["query_id"], events[2]["query_id"]})

        self.assertEqual(
            [event["query_id"] for event in remaining],
            [events[1]["query_id"], events[3]["query_id"]],
        )


if __name__ == "__main__":
    unittest.main()
