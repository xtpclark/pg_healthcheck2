queue_backlog_query = """
SELECT
    database,
    table,
    replica_name,
    queue_size,
    last_queue_update
FROM system.replicas
WHERE queue_size > 0
ORDER BY queue_size DESC
"""