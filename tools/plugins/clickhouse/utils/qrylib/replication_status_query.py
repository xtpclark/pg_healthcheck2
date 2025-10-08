replication_status_query = """
SELECT
    database,
    table,
    is_leader,
    is_readonly,
    is_session_expired,
    future_parts,
    parts_to_check,
    active,
    last_queue_update
FROM system.replicas
WHERE active = 0 OR is_session_expired = 1
ORDER BY database, table
"""