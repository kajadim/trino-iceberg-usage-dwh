WITH raw_events AS (
    SELECT
        query_id,
        user_name,
        create_time,
        query_state,
        cpu_time_ms,
        wall_time_ms,
        processed_bytes,
        processed_rows,
        referenced_tables
    FROM iceberg_scan(?) AS raw
    WHERE raw.create_time >= ?
      AND raw.create_time < ?
),
flattened_references AS (
    SELECT
        raw.query_id,
        raw.user_name,
        raw.create_time,
        CAST(raw.create_time AT TIME ZONE 'UTC' AS DATE) AS event_date,
        raw.query_state,
        raw.cpu_time_ms,
        raw.wall_time_ms,
        raw.processed_bytes,
        raw.processed_rows,
        table_ref.catalog_name,
        table_ref.schema_name,
        table_ref.table_name,
        table_ref.input_bytes AS input_bytes,
        table_ref.input_rows AS input_rows,
        table_ref.catalog_name || '.' || table_ref.schema_name || '.' || table_ref.table_name AS table_key
    FROM raw_events AS raw
    CROSS JOIN UNNEST(raw.referenced_tables) AS unnested(table_ref)
),
query_table AS (
    SELECT
        query_id,
        user_name,
        create_time,
        event_date,
        query_state,
        cpu_time_ms,
        wall_time_ms,
        processed_bytes,
        processed_rows,
        table_key,
        SUM(CAST(input_bytes AS DOUBLE)) AS table_input_bytes,
        SUM(CAST(input_rows AS DOUBLE)) AS table_input_rows,
        COUNT(input_bytes) = COUNT(*) AS all_table_input_bytes_present
    FROM flattened_references
    GROUP BY
        query_id,
        user_name,
        create_time,
        event_date,
        query_state,
        cpu_time_ms,
        wall_time_ms,
        processed_bytes,
        processed_rows,
        table_key
),
query_weights AS (
    SELECT
        query_id,
        COUNT(*) AS referenced_table_count,
        SUM(table_input_bytes) AS query_input_bytes,
        BOOL_AND(all_table_input_bytes_present) AS all_query_input_bytes_present
    FROM query_table
    GROUP BY query_id
)
SELECT
    query_table.query_id,
    dim_table.table_key,
    dim_user.user_key,
    dim_team.team_key,
    query_table.event_date,
    query_table.create_time,
    CASE
        WHEN query_table.cpu_time_ms IS NULL THEN NULL
        WHEN query_weights.all_query_input_bytes_present
             AND query_weights.query_input_bytes > 0
            THEN query_table.cpu_time_ms
                 * query_table.table_input_bytes
                 / query_weights.query_input_bytes
        ELSE query_table.cpu_time_ms / query_weights.referenced_table_count
    END AS allocated_cpu_time_ms,
    CASE
        WHEN query_table.wall_time_ms IS NULL THEN NULL
        WHEN query_weights.all_query_input_bytes_present
             AND query_weights.query_input_bytes > 0
            THEN query_table.wall_time_ms
                 * query_table.table_input_bytes
                 / query_weights.query_input_bytes
        ELSE query_table.wall_time_ms / query_weights.referenced_table_count
    END AS allocated_wall_time_ms,
    CASE
        WHEN query_table.processed_bytes IS NULL THEN NULL
        WHEN query_weights.all_query_input_bytes_present
             AND query_weights.query_input_bytes > 0
            THEN query_table.processed_bytes
                 * query_table.table_input_bytes
                 / query_weights.query_input_bytes
        ELSE query_table.processed_bytes / query_weights.referenced_table_count
    END AS allocated_processed_bytes,
    CASE
        WHEN query_table.processed_rows IS NULL THEN NULL
        WHEN query_weights.all_query_input_bytes_present
             AND query_weights.query_input_bytes > 0
            THEN query_table.processed_rows
                 * query_table.table_input_bytes
                 / query_weights.query_input_bytes
        ELSE query_table.processed_rows / query_weights.referenced_table_count
    END AS allocated_processed_rows,
    query_table.table_input_bytes,
    query_table.table_input_rows,
    query_weights.referenced_table_count > 1 AS is_multi_table_query,
    query_table.query_state = 'FINISHED' AS is_success,
    query_table.cpu_time_ms AS source_cpu_time_ms,
    query_table.wall_time_ms AS source_wall_time_ms,
    query_table.processed_bytes AS source_processed_bytes,
    query_table.processed_rows AS source_processed_rows
FROM query_table
JOIN query_weights USING (query_id)
LEFT JOIN iceberg_scan(?) AS dim_table
    ON dim_table.table_key = query_table.table_key
LEFT JOIN iceberg_scan(?) AS dim_user
    ON dim_user.user_name = query_table.user_name
    AND query_table.create_time >= dim_user.valid_from
    AND (dim_user.valid_to IS NULL OR query_table.create_time < dim_user.valid_to)
LEFT JOIN iceberg_scan(?) AS dim_team
    ON dim_team.team_key = dim_user.team_key
