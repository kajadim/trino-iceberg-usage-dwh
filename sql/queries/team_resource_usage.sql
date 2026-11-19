SELECT
    team_dim.team_name,
    SUM(fact.allocated_cpu_time_ms) AS allocated_cpu_time_ms,
    SUM(fact.allocated_wall_time_ms) AS allocated_wall_time_ms,
    SUM(fact.allocated_processed_bytes) AS allocated_processed_bytes,
    SUM(fact.allocated_processed_rows) AS allocated_processed_rows
FROM iceberg_scan(?) AS fact
JOIN iceberg_scan(?) AS team_dim
    ON team_dim.team_key = fact.team_key
WHERE fact.event_date >= CAST(? AS DATE)
  AND fact.event_date < CAST(? AS DATE)
  AND team_dim.team_name = ?
  AND fact.is_success
GROUP BY team_dim.team_name;
