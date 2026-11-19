SELECT
    table_dim.catalog_name,
    table_dim.schema_name,
    table_dim.table_name,
    COUNT(DISTINCT user_dim.user_name) AS distinct_users
FROM iceberg_scan(?) AS fact
JOIN iceberg_scan(?) AS table_dim
    ON table_dim.table_key = fact.table_key
JOIN iceberg_scan(?) AS user_dim
    ON user_dim.user_key = fact.user_key
WHERE fact.event_date >= CAST(? AS DATE)
  AND fact.event_date < CAST(? AS DATE)
  AND fact.table_key = ?
  AND table_dim.is_iceberg
  AND fact.is_success
GROUP BY
    table_dim.catalog_name,
    table_dim.schema_name,
    table_dim.table_name
ORDER BY
    table_dim.catalog_name,
    table_dim.schema_name,
    table_dim.table_name;
