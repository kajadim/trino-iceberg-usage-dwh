# Iteration Log

## Iteration 1: raw event layer

- **Hypothesis:** an append-only event table partitioned by query creation day would preserve the source data and support time-range scans.
- **Observed:** PyArrow parsed the initial absolute `file:///D:/...` warehouse URI as an invalid `/D:/...` path on Windows.
- **Change:** changed the local warehouse to `./warehouse`; the raw Iceberg table then created and appended successfully.
- **Why:** the relative local path is portable across developer checkouts when commands run from the repository root.
- **Observed:** a partitioned append on PyIceberg 0.12.0 required the optional `pyiceberg-core` dependency for day transforms.
- **Change:** added the `pyiceberg-core` extra to `requirements.txt`.
- **Why:** table creation alone did not exercise the partition transform; the dependency is needed during writes.

## Iteration 2: dimensions, query-table fact, and reports

- **Hypothesis:** a query × table fact would make table access counts and per-table resource reporting straightforward.
- **Observed:** multi-table query metrics are query-level, and SCD2 version keys change when a user changes teams.
- **Change:** allocated additive query metrics across distinct referenced tables by per-table input bytes, falling back to an even split when those weights are incomplete or zero. Kept stable `user_name` in the user dimension for distinct counts across SCD versions.
- **Why:** this supports table-level and team-level reporting while documenting the allocation as an estimate rather than a direct measurement.
- **Hypothesis:** external catalog references could be dropped because the primary use case is Iceberg.
- **Change:** retained and flagged external references in the dimensions and fact instead of dropping them.
- **Why:** mixed-source query allocations then reconcile across the whole query; Iceberg-specific reports filter `dim_table.is_iceberg`.
- **Observed:** one sampled unmapped user would be lost or conflated with other users if mapped to a single generic `UNKNOWN` user.
- **Change:** retained each observed unmapped identity with a time-valid `dim_user` row assigned to team `UNKNOWN`.
- **Why:** missing team metadata should not discard or merge distinct users.
- **Observed:** DuckDB treats `references` as a reserved word in the `UNNEST` alias, and DuckDB 2.0 exposes Arrow output through `to_arrow_table()`.
- **Change:** used a non-reserved alias and the supported Arrow result API.
- **Why:** both behaviors were confirmed by executing the transformation on the sample Iceberg tables.
- **Observed:** proportional allocations use floating-point values and can display tiny fractional-byte artifacts.
- **Change:** retained `DOUBLE` allocations and added per-query reconciliation checks with a numerical tolerance.
- **Why:** presentation can round aggregated values without losing conservation in the stored fact.

The sample produced 3,000 raw query rows and 4,242 query-table fact rows. The three reporting queries ran against the resulting Iceberg tables.

## Iteration 3: idempotent date-range refresh and daily aggregate

- **Hypothesis:** replacing fact and aggregate rows for a half-open UTC date range would make reruns safe and allow recent late-arriving events to be picked up.
- **Change:** added `--date`, `--start-date`/`--end-date`, and a default three-day lookback. Both transforms now use Iceberg overwrite filters and snapshot properties; raw ingestion skips existing query IDs.
- **Why:** rerunning the same date range now replaces its partitions instead of accumulating duplicate fact or aggregate rows.
- **Observed:** the daily fact has one row per query/table, so daily `query_count` needs distinct query IDs, and a user changing teams can have multiple SCD keys.
- **Change:** the daily aggregate groups by date/table/team, counts distinct query IDs and stable `user_name`, and sums allocated resources for successful queries.
- **Why:** daily distinct-user values are correct for each group but cannot be summed to produce a multi-day distinct count; exact period reports continue to query the fact.
- **Observed:** for `iceberg.analytics.orders`, summing daily distinct-user values produced 243, while the exact period query against the fact returned 20.
- **Change:** kept period-wide distinct-user reports on the fact instead of deriving them from daily counts.
- **Why:** daily users repeat across dates and teams; summing their counts overstates the number of people.
- **Observed:** an empty aggregate table emitted a warning when an overwrite attempted to delete a date partition that had no files.
- **Change:** check the target date range first, appending when it has no existing rows and overwriting only when rows need replacement.
- **Why:** this avoids unnecessary delete snapshots and warnings for first-time date loads while retaining replacement semantics.

### Local performance baseline

Before date-range refreshes were added, one local full-range measurement on the sample (3,000 raw events, 4,242 fact rows; Windows, Python 3.11, PyIceberg 0.12.0, DuckDB 2.0.0, PyArrow 26.0.1) reported:

| Operation | Elapsed time |
| --- | ---: |
| Projected raw scan used by dimension loading | 119.73 ms |
| Full-range DuckDB fact SQL and Arrow conversion | 90.17 ms |
| Distinct-user report including connection and Iceberg metadata setup | 185.94 ms |

After the refresh changes, a full fixed-sample pipeline rerun took 2,818 ms and retained 3,000 raw, 4,242 fact, and 2,420 aggregate rows. It includes process startup, sample generation, query-ID deduplication, dimension loading, fact refresh, and daily aggregation; it was measured on a warm local warehouse.

These are single-run local observations, not a controlled benchmark; timings vary with cache state and hardware.

### Known scaling opportunities

- Raw duplicate detection currently materializes all existing query IDs. Use source offsets, a batch manifest, or a scalable Iceberg merge/deduplication strategy for larger event volumes.
- Dimension loading currently materializes raw users and table references in Python. Move distinct-key extraction to DuckDB or load dimensions incrementally.
- A persistent DuckDB connection and cached Iceberg metadata can reduce per-report setup overhead. Connection reuse is now implemented for the fact and aggregate stages of a pipeline run; standalone transform commands still open their own connection.
- Floating-point resource allocation can produce fractional bytes. Use exact decimal/largest-remainder allocation if integer conservation is required.
- Iceberg compaction and snapshot-retention policies should be tuned as files and history grow.
- If exact multi-day distinct counts become expensive, evaluate HLL sketches; daily distinct counts must not be summed.

## Iteration 4: shared DuckDB connection and source-aware refresh reuse

- **Hypothesis:** repeated full runs spend time reloading the same Iceberg extension and recomputing outputs even when none of their inputs changed.
- **Change:** the pipeline now uses one configured DuckDB connection for both the fact and daily aggregate transformations. Each dimension, fact, and aggregate snapshot records its source metadata locations, relevant seed-file or transformation fingerprints, and output row count.
- **Change:** if the latest snapshot matches the requested date range and all recorded inputs, the corresponding stage returns its recorded row count without rescanning, rerunning SQL, or writing another snapshot.
- **Why:** upstream metadata changes, seed-file changes, SQL/code/schema changes, or a different date range invalidate the matching signature and cause normal recomputation. Existing standalone transform commands remain supported and create their own DuckDB connections.
- **Observed:** repeated runs with the same sample inputs left all dimension, fact, and aggregate snapshot IDs unchanged. The reported counts remained 3,000 raw, 4,242 fact, and 2,420 aggregate rows.

### Iteration 4 local measurements

Measurements used the fixed 3,000-event sample, a warm local warehouse, Windows, Python 3.11, PyIceberg 0.12.0, DuckDB 2.0.0, and PyArrow 26.0.1. Values below are medians unless noted.

| Operation | Previous approach | Updated approach | Observed change |
| --- | ---: | ---: | ---: |
| Fact + daily aggregate refresh | 1,592.47 ms with separate DuckDB connections | 1,378.77 ms with one shared connection | 13.4% faster |
| Full pipeline rerun with unchanged inputs | 2,818 ms, one earlier warm run | 1,333.13 ms, median of five warm reruns | 52.7% lower elapsed time |
| Dimension / fact / aggregate unchanged-input stage | Always rebuilt | 41.19 / 45.77 / 59.14 ms | No SQL transform or Iceberg write |

The full-pipeline comparison is directional rather than a controlled benchmark: the earlier result was a single run, while the updated value is a five-run median. It includes process startup, sample generation, JSONL writing, raw-ID deduplication, and stage setup. The unchanged-input fast path does not speed up a first-time or invalidated full recomputation; the shared connection provides the measured improvement for that case. Raw duplicate detection still scans existing query IDs and remains the main scaling opportunity.

### Conclusion

The source-aware snapshot check and shared DuckDB connection improve repeated runs without weakening refresh correctness: input or transformation changes invalidate the cache, and a forced SQL-fingerprint change was verified to recompute the fact and aggregate while preserving their row counts. This is a useful optimization for the current batch workflow, but it does not replace scalable raw ingestion deduplication or controlled benchmarking at production volumes.
