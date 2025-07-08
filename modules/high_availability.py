def run_high_availability(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes high availability (HA) configurations, including standby settings,
    failover mechanisms, and cloud-specific HA features.
    """
    adoc_content = ["=== High Availability Analysis", "Analyzes high availability configurations to ensure database resilience and continuity.\n"]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    if settings['show_qry'] == 'true':
        adoc_content.append("High Availability queries (conceptual, as direct HA checks vary greatly by setup):")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("-- Check pg_is_in_recovery() for standby status (self-hosted)")
        adoc_content.append("SELECT pg_is_in_recovery();")
        adoc_content.append("-- Check current timeline (relevant for recovery)")
        adoc_content.append("SELECT pg_current_wal_lsn(); -- Or pg_current_xlog_location() for older versions")
        adoc_content.append("----")

    queries = [
        (
            "Database Recovery Status (for Standby/Replica)", 
            "SELECT pg_is_in_recovery();", 
            settings['is_aurora'] == 'false', # More relevant for self-hosted standbys
            "is_in_recovery_status" # Data key
        ),
        (
            "Current WAL LSN", 
            "SELECT pg_current_wal_lsn();", 
            True, # Always relevant for primary and standby
            "current_wal_lsn" # Data key
        )
    ]

    for title, query, condition, data_key in queries:
        if not condition:
            note_msg = "Query not applicable for this setup (e.g., Aurora manages recovery internally)."
            adoc_content.append(f"{title}\n[NOTE]\n====\n{note_msg}\n====\n")
            structured_data[data_key] = {"status": "not_applicable", "reason": note_msg}
            continue
        
        # Standardized parameter passing pattern:
        params_for_query = None # No named parameters in these queries
        
        formatted_result, raw_result = execute_query(query, params=params_for_query, return_raw=True)
        
        if "[ERROR]" in formatted_result:
            adoc_content.append(f"{title}\n{formatted_result}")
            structured_data[data_key] = {"status": "error", "details": raw_result}
        else:
            adoc_content.append(title)
            adoc_content.append(formatted_result)
            structured_data[data_key] = {"status": "success", "data": raw_result} # Store raw data
    
    adoc_content.append("[TIP]\n====\n"
                   "High Availability is crucial for minimizing downtime and ensuring business continuity. "
                   "For self-hosted setups, ensure proper configuration of streaming replication, `wal_level`, `max_wal_senders`, and `hot_standby`. "
                   "Regularly test failover procedures to verify HA readiness. "
                   "Consider external tools like Patroni or repmgr for automated failover and cluster management.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\n"
                       "AWS RDS Aurora provides built-in high availability with its distributed, fault-tolerant storage system and automatic failover to Aurora Replicas. "
                       "Monitor `AuroraReplicaLag` in CloudWatch to ensure replicas are in sync. "
                       "Ensure you have sufficient Aurora Replicas in different Availability Zones for multi-AZ resilience. "
                       "Regularly review your RTO (Recovery Time Objective) and RPO (Recovery Point Objective) targets.\n"
                       "====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data

