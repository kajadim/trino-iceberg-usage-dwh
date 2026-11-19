# Trino Iceberg Usage DWH

This project collects Trino query events and builds a historical usage model for tables and teams. It includes an append-only raw layer, SCD type 2 dimensions, a query-table fact, date-range refreshes, a daily aggregate, and example reporting queries.

## Setup and full sample run

Run commands from the repository root:

```powershell
python -m pip install -r requirements.txt
python -m src.run_pipeline --count 3000 --days 60 --seed 42 --as-of 2026-11-19T12:00:00+00:00 --start-date 2026-09-20 --end-date 2026-11-20
python -m unittest discover -s tests
```

The pipeline creates the Iceberg tables, generates seeded JSON Lines events under `data/sample/`, skips already-ingested query IDs, refreshes the dimensions, and replaces fact and daily aggregate rows for the selected date range. The default range is the most recent three UTC dates, including today. Use `--date YYYY-MM-DD` for one date, or `--start-date YYYY-MM-DD --end-date YYYY-MM-DD` for a start-inclusive, end-exclusive range.

To reprocess an existing raw dataset without generating sample data:

```powershell
python -m src.load_dimensions
python -m src.transform_fact --date 2026-11-19
python -m src.transform_daily_agg --date 2026-11-19
```

For late-arriving events older than the default lookback window, pass a range covering their event dates. Generated sample data and local Iceberg warehouse files are ignored by Git.

## Example reports

```powershell
python -m src.run_queries distinct-users --start-date 2026-09-20 --end-date 2026-11-20 --table-key iceberg.analytics.orders
python -m src.run_queries team-resources --start-date 2026-09-20 --end-date 2026-11-20 --team UNKNOWN
python -m src.run_queries weekly-trend --start-date 2026-09-20 --end-date 2026-11-20
```

The distinct-user report counts stable user identities, not SCD version keys. Team resource reports include all referenced catalogs; filter the table dimension to `is_iceberg` when only Iceberg-attributed resources are needed. Daily distinct-user counts are non-additive and must not be summed across dates.

## Design and operational notes

The table grains, relationships, mapping assumptions, resource allocation, snapshot rollback, and current performance observations are documented in [docs/data_model.md](docs/data_model.md) and [docs/iteration_log.md](docs/iteration_log.md).

Raw events are append-only and duplicate `query_id` values are skipped. Fact and daily aggregate transforms replace the selected date range in Iceberg, so rerunning the same range does not add duplicate rows. The raw deduplication currently scans existing query IDs; this is suitable for the sample but should be replaced by an indexed/incremental ingestion mechanism at larger scale.

The pipeline reuses one DuckDB connection for fact and aggregate transforms. When the latest dimension, fact, or aggregate snapshot records the same source snapshots, seeds, transform code, SQL, and date range, that stage skips recomputation and writes. Changes to those inputs trigger a refresh. Local measurements and limitations are recorded in [docs/iteration_log.md](docs/iteration_log.md).

The local SQL catalog is configured in `.pyiceberg.yaml`. Its warehouse path is relative, so run commands from the repository root.
