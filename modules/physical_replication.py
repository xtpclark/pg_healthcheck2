def run_physical_replication(cursor, settings, execute_query, execute_pgbouncer):
    content = ["=== Physical Replication Status", "Analyzes physical replication status for primary and replica nodes to ensure data consistency and minimal lag."]
    
    if settings['show_qry'] == 'true':
        content.append("Physical replication queries:")
        content.append("[,sql]\n----")
        content.append("SELECT pid, usename, application_name, state, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn)) AS sent_lag, pg_size_pretty(pg_wal_lsn_diff(sent_lsn, write_lsn)) AS write_lag, pg_size_pretty(pg_wal_lsn_diff(write_lsn, flush_lsn)) AS flush_lag, pg_size_pretty(pg_wal_lsn_diff(flush_lsn, replay_lsn)) AS replay_lag FROM pg_stat_replication WHERE state != 'startup';")
        content.append("SELECT pid, status, pg_size_pretty(pg_wal_lsn_diff(written_lsn, flushed_lsn)) AS write_flush_lag, pg_size_pretty(pg_wal_lsn_diff(flushed_lsn, latest_end_lsn)) AS flush_end_lag, received_tli, last_msg_send_time, last_msg_receipt_time FROM pg_stat_wal_receiver WHERE status IS NOT NULL;")
        content.append("----")

    queries = [
        ("Replication Status (Primary)", "SELECT pid, usename, application_name, state, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn)) AS sent_lag, pg_size_pretty(pg_wal_lsn_diff(sent_lsn, write_lsn)) AS write_lag, pg_size_pretty(pg_wal_lsn_diff(write_lsn, flush_lsn)) AS flush_lag, pg_size_pretty(pg_wal_lsn_diff(flush_lsn, replay_lsn)) AS replay_lag FROM pg_stat_replication WHERE state != 'startup';", True),
        ("WAL Receiver Status (Replica)", "SELECT pid, status, pg_size_pretty(pg_wal_lsn_diff(written_lsn, flushed_lsn)) AS write_flush_lag, pg_size_pretty(pg_wal_lsn_diff(flushed_lsn, latest_end_lsn)) AS flush_end_lag, received_tli, last_msg_send_time, last_msg_receipt_time FROM pg_stat_wal_receiver WHERE status IS NOT NULL;", not settings['is_aurora'] == 'true')
    ]

    for title, query, condition in queries:
        if not condition:
            content.append(f"{title}\n[NOTE]\n====\n{'Aurora manages replication internally; WAL receiver not applicable.' if 'wal_receiver' in query else 'Query not applicable.'}\n====")
            continue
        params = None  # No named placeholders in these queries
        result = execute_query(query, params=params)
        if "[ERROR]" in result or "[NOTE]" in result:
            content.append(f"{title}\n{result}")
        else:
            content.append(title)
            content.append(result)
    
    content.append("[TIP]\n====\nMonitor replication lag (sent_lag, write_lag, flush_lag, replay_lag) to ensure minimal delays. High lag may indicate network issues or insufficient resources on replicas. For Aurora, use CloudWatch metrics (e.g., ReplicaLag) to monitor replication performance.\n====")
    if settings['is_aurora'] == 'true':
        content.append("[NOTE]\n====\nAWS RDS Aurora uses managed physical replication for read replicas. Check Aurora ReplicaLag in CloudWatch for lag monitoring.\n====")
    
    return "\n".join(content)
