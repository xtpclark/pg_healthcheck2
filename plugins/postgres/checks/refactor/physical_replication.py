def run_physical_replication(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes physical replication status for primary and replica nodes to ensure data consistency and minimal lag.
    """
    adoc_content = ["Analyzes physical replication status for primary and replica nodes to ensure data consistency and minimal lag.\n"]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    if settings['show_qry'] == 'true':
        adoc_content.append("Physical replication queries:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("SELECT pid, usename, application_name, state, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn)) AS sent_lag, pg_size_pretty(pg_wal_lsn_diff(sent_lsn, write_lsn)) AS write_lag, pg_size_pretty(pg_wal_lsn_diff(flush_lsn, replay_lsn)) AS flush_lag, pg_size_pretty(pg_wal_lsn_diff(flush_lsn, replay_lsn)) AS replay_lag FROM pg_stat_replication WHERE state != 'startup';")
        adoc_content.append("SELECT pid, status, pg_size_pretty(pg_wal_lsn_diff(written_lsn, flushed_lsn)) AS write_flush_lag, pg_size_pretty(pg_wal_lsn_diff(flushed_lsn, latest_end_lsn)) AS flush_end_lag, received_tli, last_msg_send_time, last_msg_receipt_time FROM pg_stat_wal_receiver WHERE status IS NOT NULL;")
        adoc_content.append("----")

    queries = [
        (
            "Replication Status (Primary)", 
            "SELECT pid, usename, application_name, state, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn)) AS sent_lag, pg_size_pretty(pg_wal_lsn_diff(sent_lsn, write_lsn)) AS write_lag, pg_size_pretty(pg_wal_lsn_diff(write_lsn, flush_lsn)) AS flush_lag, pg_size_pretty(pg_wal_lsn_diff(flush_lsn, replay_lsn)) AS replay_lag FROM pg_stat_replication WHERE state != 'startup';", 
            True,
            "replication_status_primary" # Data key
        ),
        (
            "WAL Receiver Status (Replica)", 
            "SELECT pid, status, pg_size_pretty(pg_wal_lsn_diff(written_lsn, flushed_lsn)) AS write_flush_lag, pg_size_pretty(pg_wal_lsn_diff(flushed_lsn, latest_end_lsn)) AS flush_end_lag, received_tli, last_msg_send_time, last_msg_receipt_time FROM pg_stat_wal_receiver WHERE status IS NOT NULL;", 
            settings['is_aurora'] == 'false', # Condition: not applicable for Aurora
            "wal_receiver_status_replica" # Data key
        )
    ]

    for title, query, condition, data_key in queries:
        if not condition:
            note_msg = 'Aurora manages replication internally; WAL receiver not applicable.' if 'wal_receiver' in query else 'Query not applicable.'
            adoc_content.append(f"{title}\n[NOTE]\n====\n{note_msg}\n====\n")
            structured_data[data_key] = {"status": "not_applicable", "reason": note_msg}
            continue
        
        # Standardized parameter passing pattern:
        # No named placeholders in these queries, so params_for_query will be None.
        params_for_query = None 
        
        formatted_result, raw_result = execute_query(query, params=params_for_query, return_raw=True)
        
        if "[ERROR]" in formatted_result:
            adoc_content.append(f"{title}\n{formatted_result}")
            structured_data[data_key] = {"status": "error", "details": raw_result}
        else:
            adoc_content.append(title)
            adoc_content.append(formatted_result)
            structured_data[data_key] = {"status": "success", "data": raw_result} # Store raw data
    
    adoc_content.append("[TIP]\n====\nMonitor replication lag (sent_lag, write_lag, flush_lag, replay_lag) to ensure minimal delays. High lag may indicate network issues or insufficient resources on replicas. For Aurora, use CloudWatch metrics (e.g., ReplicaLag) to monitor replication performance.\n====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\nAWS RDS Aurora uses managed physical replication for read replicas. Check Aurora ReplicaLag in CloudWatch for lag monitoring.\n====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data

