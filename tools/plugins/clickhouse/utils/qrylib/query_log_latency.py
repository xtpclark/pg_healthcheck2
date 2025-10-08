QUERY_LOG_LATENCY = """
SELECT
    query_kind,
    query,
    query_duration_ms,
    event_time
FROM system.query_log
WHERE event_time > now() - INTERVAL 1 HOUR
  AND (query_kind = 'Select' OR query_kind = 'Insert')
  AND query_duration_ms > 100
ORDER BY query_duration_ms DESC
LIMIT 10
"""
