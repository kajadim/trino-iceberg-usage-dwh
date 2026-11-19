# Meta DWH Data Model

## Tables, grain, and relationships

The raw event layer is retained separately from the reporting model. Transformations can therefore be corrected and rerun without losing the source query events.

| Table | Grain | Key relationships |
| --- | --- | --- |
| `raw_query_events` | One row per Trino query event (`query_id`) | Source for the query-table fact |
| `dim_user` | One row per user and team-mapping validity interval | `user_key` is the SCD version key; `user_name` is the stable user identity |
| `dim_team` | One row per team | Referenced by `dim_user` and `fact_table_access` |
| `dim_table` | One row per canonical `catalog.schema.table` | Includes source type and `is_iceberg` |
| `fact_table_access` | One row per query and canonical referenced table | References user, team, and table dimensions |
| `agg_table_usage_daily` | One row per date, table, and team | Derived from successful fact rows |

```text
dim_user ─┐
dim_team ─┼── fact_table_access ─── agg_table_usage_daily
dim_table ┘             ↑
                   raw_query_events
```

All six tables are created by `src/create_tables.py`. Their Iceberg schemas and partition specifications are in `src/schemas.py`.

## Raw events and partitioning

`raw_query_events` stores `query_id`, `user_name`, `create_time`, nullable `end_time`, query text and state, query-level CPU/wall/peak-memory/processed-byte/processed-row metrics, and `referenced_tables`. Each table reference has catalog/schema/table identifiers and nullable input bytes and rows.

The raw table is partitioned by `day(create_time)`. Query-period filters can prune partitions without storing a duplicate date. The sample and ingestion code assume `query_id` is unique across the source Trino cluster; a multi-cluster deployment should include a cluster identifier in that key.

## User and team mapping

`data/seed/user_team_mapping.csv` has `user_name`, `team_name`, `valid_from`, and nullable `valid_to`. Each row is an SCD type 2 interval; the interval is start-inclusive and end-exclusive. Overlaps are rejected. `dim_user.user_key` identifies a mapping version, while `user_name` remains stable across versions. Facts resolve the version valid at query creation time.

An observed user without a covering mapping gets an event-time dimension row assigned to the `UNKNOWN` team. The user's identity is retained rather than collapsing all unmapped users into a single unknown user, so distinct-user counts remain meaningful.

`dim_team` includes every mapped team and `UNKNOWN`. `data/seed/table_catalog.csv` provides table owner, domain, and source type. Referenced tables missing from the seed are still added to `dim_table` with unknown owner/domain. The configured source types `iceberg` and `s3` are classified as Iceberg; other sources, such as the sample `postgres` catalog, are retained with `is_iceberg = false`.

## Query-table fact and multi-source queries

`fact_table_access` is partitioned by `day(event_date)` and has one row per query and canonical table reference. Repeated references to the same table in one query are grouped before fact rows are emitted. Its additive metrics are `allocated_cpu_time_ms`, `allocated_wall_time_ms`, `allocated_processed_bytes`, and `allocated_processed_rows`; `table_input_bytes` and `table_input_rows` preserve observed per-table input metrics. `is_multi_table_query` and `is_success` support reporting filters.

All source references, including external catalogs, are retained in `dim_table` and `fact_table_access`. This keeps allocated resources reconcilable across a mixed-source query. Iceberg-table reports filter `dim_table.is_iceberg`; team resource reports include all sources unless explicitly filtered. Failed queries remain in raw and fact data for audit, while the example usage reports and daily aggregate select successful queries.

## Resource metrics and allocation

Trino provides CPU time, wall time, processed bytes, and processed rows at query grain, not per table. Because the fact grain is query × table, the transformation allocates each additive query metric:

1. If every referenced table has an input-byte measurement and their sum is positive, each metric is allocated in proportion to input bytes.
2. If any per-table input-byte value is missing, or the sum is zero, each metric is divided evenly among the distinct referenced tables.
3. Peak memory is not allocated or summed across tables. It remains a query-level metric in raw events and can be reported with a query-grain aggregation such as `MAX`.
4. Queries with no referenced tables remain query-level raw events and do not produce fact rows.

Allocated fact metrics are floating-point estimates. The transformation checks that allocations reconcile to query totals within a small numerical tolerance; rounding belongs in presentation, after aggregation. These values are not directly measured per-table resource consumption.

## Daily aggregate and distinct users

`agg_table_usage_daily` has one row per `event_date`, `table_key`, and `team_key`, with daily `distinct_users`, `query_count`, `cpu_seconds`, `wall_time_seconds`, `bytes_processed`, and `rows_processed`. CPU and wall time are converted from milliseconds to seconds. It is rebuilt from successful fact rows for the requested date range.

Daily distinct-user counts are non-additive. A multi-day report must recalculate `COUNT(DISTINCT user_name)` over the requested period; it must not sum daily distinct counts. This preserves one identity even when a user changes teams and receives a new SCD version key. Exact period-wide counts use the fact table; an approximate HLL sketch or a date/table/user bridge can be considered if that becomes too expensive.

## Date-range updates, late events, and snapshots

`src.run_pipeline.py`, `src.transform_fact.py`, and `src.transform_daily_agg.py` accept either `--date`, a half-open `--start-date`/`--end-date` range, or a default three-UTC-day lookback. Fact and aggregate rows for the selected dates are replaced with Iceberg overwrite operations. A late event is handled by rerunning its event date; the default lookback catches recent late arrivals.

Each replacement that changes data creates an Iceberg snapshot. Capture the prior snapshot ID before a refresh. To inspect that version, use `table.scan(snapshot_id=prior_snapshot_id)`. To restore it as the current version:

```python
table.manage_snapshots().rollback_to_snapshot(prior_snapshot_id).commit()
```

Snapshot expiration must be configured so that snapshots needed for rollback remain available. Raw ingestion is append-only and skips known query IDs; it currently scans all existing IDs, which is not an efficient large-scale deduplication strategy.

## Reporting queries

The executable SQL is in `sql/queries/` and can be run with `src/run_queries.py`:

- **Distinct users of an Iceberg table in a period:** join `fact_table_access` to `dim_table` and `dim_user`, filter `is_iceberg`, successful queries, and the half-open date range, then count distinct `user_name`.
- **Resources consumed by a team:** join the fact to `dim_team`, filter the half-open date range and team, and sum allocated CPU, wall-time, processed-byte, and processed-row metrics.
- **Weekly usage trend:** group successful Iceberg table accesses by week and table, recomputing distinct users per week rather than summing daily values.
