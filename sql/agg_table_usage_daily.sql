SELECT
    fact.event_date,
    fact.table_key,
    fact.team_key,
    COUNT(DISTINCT user_dim.user_name) AS distinct_users,
    COUNT(DISTINCT fact.query_id) AS query_count,
    SUM(fact.allocated_cpu_time_ms) / 1000.0 AS cpu_seconds,
    SUM(fact.allocated_wall_time_ms) / 1000.0 AS wall_time_seconds,
    SUM(fact.allocated_processed_bytes) AS bytes_processed,
    SUM(fact.allocated_processed_rows) AS rows_processed
FROM iceberg_scan(?) AS fact
JOIN iceberg_scan(?) AS user_dim
    ON user_dim.user_key = fact.user_key
WHERE fact.event_date >= ?
  AND fact.event_date < ?
  AND fact.is_success
GROUP BY
    fact.event_date,
    fact.table_key,
    fact.team_key;
