def run_pg_locks_analysis(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes pg_locks for blocking and contention issues.
    This module identifies lock conflicts, blocking sessions, and provides recommendations for lock management.
    """
    adoc_content = ["=== pg_locks Analysis", "Analyzes pg_locks for blocking and contention issues.\n"]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    # Get PostgreSQL version for compatibility
    version_query = "SELECT version();"
    version_result, _ = execute_query(version_query, return_raw=True)
    
    # Extract version number for compatibility checks
    pg_version = None
    if version_result and not isinstance(version_result, str):
        try:
            version_str = version_result[0]['version'] if isinstance(version_result, list) and version_result else str(version_result)
            # Extract version number (e.g., "PostgreSQL 15.3" -> 15)
            import re
            version_match = re.search(r'PostgreSQL (\d+)', version_str)
            if version_match:
                pg_version = int(version_match.group(1))
        except (IndexError, AttributeError, ValueError):
            pg_version = 13  # Default to minimum supported version
    
    # Query for current lock conflicts and blocking
    lock_conflicts_query = """
        SELECT
            blocked_locks.pid     AS blocked_pid,
            blocked_activity.usename  AS blocked_user,
            blocking_locks.pid     AS blocking_pid,
            blocking_activity.usename AS blocking_user,
            blocked_activity.query    AS blocked_statement,
            blocking_activity.query   AS current_statement_in_blocking_process,
            blocked_activity.application_name AS blocked_application,
            blocking_activity.application_name AS blocking_application,
            blocked_activity.query_start,
            age(now(), blocked_activity.query_start) AS blocked_duration
        FROM  pg_catalog.pg_locks         blocked_locks
        JOIN pg_catalog.pg_stat_activity blocked_activity  ON blocked_activity.pid = blocked_locks.pid
        JOIN pg_catalog.pg_locks         blocking_locks 
            ON blocking_locks.locktype = blocked_locks.locktype
            AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
            AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
            AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
            AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
            AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
            AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
            AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
            AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
            AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
            AND blocking_locks.pid != blocked_locks.pid
        JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
        WHERE NOT blocked_locks.granted
          AND blocked_activity.datname = %(database)s
        ORDER BY blocked_duration DESC
        LIMIT %(limit)s;
    """
    
    # Query for lock statistics by type
    lock_stats_query = """
        SELECT 
            l.locktype,
            l.mode,
            l.granted,
            COUNT(*) as lock_count,
            COUNT(DISTINCT l.pid) as session_count
        FROM pg_locks l
        JOIN pg_stat_activity a ON l.pid = a.pid
        WHERE a.datname = %(database)s
            AND a.query NOT LIKE %(autovacuum_pattern)s
        GROUP BY l.locktype, l.mode, l.granted
        ORDER BY lock_count DESC, l.locktype, l.mode;
    """
    
    # Query for long-running transactions that might be holding locks
    long_running_txns_query = """
        SELECT 
            a.pid,
            a.usename,
            a.application_name,
            a.client_addr,
            a.state,
            LEFT(a.query, 100) as query_preview,
            EXTRACT(EPOCH FROM (NOW() - a.query_start)) as query_duration_seconds,
            EXTRACT(EPOCH FROM (NOW() - a.xact_start)) as transaction_duration_seconds,
            COUNT(l.pid) as lock_count
        FROM pg_stat_activity a
        LEFT JOIN pg_locks l ON a.pid = l.pid
        WHERE a.datname = %(database)s
            AND a.query NOT LIKE %(autovacuum_pattern)s
            AND a.state IN ('active', 'idle in transaction')
            AND EXTRACT(EPOCH FROM (NOW() - a.query_start)) > 60  -- Only show queries running > 1 minute
        GROUP BY a.pid, a.usename, a.application_name, a.client_addr, a.state, a.query, a.query_start, a.xact_start
        ORDER BY transaction_duration_seconds DESC
        LIMIT %(limit)s;
    """
    
    # Query for lock wait configuration
    lock_wait_config_query = """
        SELECT 
            name, 
            setting, 
            unit, 
            short_desc 
        FROM pg_settings 
        WHERE name IN ('lock_timeout', 'deadlock_timeout', 'log_lock_waits')
        ORDER BY name;
    """
    
    if settings['show_qry'] == 'true':
        adoc_content.append("pg_locks analysis queries:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append(lock_conflicts_query)
        adoc_content.append(lock_stats_query)
        adoc_content.append(long_running_txns_query)
        adoc_content.append(lock_wait_config_query)
        adoc_content.append("----")

    # Execute lock conflicts analysis
    params_for_conflicts = {
        'database': settings['database'],
        'limit': settings['row_limit'],
        'autovacuum_pattern': 'autovacuum:%'
    }
    formatted_conflicts_result, raw_conflicts_result = execute_query(lock_conflicts_query, params=params_for_conflicts, return_raw=True)
    
    if "[ERROR]" in formatted_conflicts_result:
        adoc_content.append(f"Lock Conflicts Analysis\n{formatted_conflicts_result}")
        structured_data["lock_conflicts"] = {"status": "error", "details": raw_conflicts_result}
    else:
        adoc_content.append("=== Lock Conflicts Analysis")
        adoc_content.append(formatted_conflicts_result)
        structured_data["lock_conflicts"] = {"status": "success", "data": raw_conflicts_result}
        
        # Analyze blocking chains for critical issues
        if raw_conflicts_result and isinstance(raw_conflicts_result, list):
            critical_blocking = []
            long_blocking = []
            
            for conflict in raw_conflicts_result:
                duration = conflict.get('blocked_duration_seconds', 0)
                if duration > 300:  # 5 minutes
                    critical_blocking.append(f"PID {conflict.get('blocked_pid')} blocked for {duration:.1f}s by PID {conflict.get('blocking_pid')}")
                elif duration > 60:  # 1 minute
                    long_blocking.append(f"PID {conflict.get('blocked_pid')} blocked for {duration:.1f}s by PID {conflict.get('blocking_pid')}")
            
            if critical_blocking:
                adoc_content.append("\n[WARNING]\n====\n**CRITICAL BLOCKING ISSUES (>5 minutes):**\n")
                for issue in critical_blocking:
                    adoc_content.append(f"* {issue}\n")
                adoc_content.append("====\n")
            
            if long_blocking:
                adoc_content.append("\n[CAUTION]\n====\n**LONG BLOCKING ISSUES (>1 minute):**\n")
                for issue in long_blocking:
                    adoc_content.append(f"* {issue}\n")
                adoc_content.append("====\n")

    # Execute lock statistics
    params_for_stats = {
        'database': settings['database'],
        'autovacuum_pattern': 'autovacuum:%'
    }
    formatted_stats_result, raw_stats_result = execute_query(lock_stats_query, params=params_for_stats, return_raw=True)
    
    if "[ERROR]" in formatted_stats_result:
        adoc_content.append(f"Lock Statistics\n{formatted_stats_result}")
        structured_data["lock_statistics"] = {"status": "error", "details": raw_stats_result}
    else:
        adoc_content.append("=== Lock Statistics")
        adoc_content.append(formatted_stats_result)
        structured_data["lock_statistics"] = {"status": "success", "data": raw_stats_result}

    # Execute long-running transactions analysis
    params_for_txns = {
        'database': settings['database'],
        'limit': settings['row_limit'],
        'autovacuum_pattern': 'autovacuum:%'
    }
    formatted_txns_result, raw_txns_result = execute_query(long_running_txns_query, params=params_for_txns, return_raw=True)
    
    if "[ERROR]" in formatted_txns_result:
        adoc_content.append(f"Long-Running Transactions\n{formatted_txns_result}")
        structured_data["long_running_transactions"] = {"status": "error", "details": raw_txns_result}
    else:
        adoc_content.append("=== Long-Running Transactions")
        adoc_content.append(formatted_txns_result)
        structured_data["long_running_transactions"] = {"status": "success", "data": raw_txns_result}

    # Execute lock wait configuration
    formatted_config_result, raw_config_result = execute_query(lock_wait_config_query, return_raw=True)
    
    if "[ERROR]" in formatted_config_result:
        adoc_content.append(f"Lock Wait Configuration\n{formatted_config_result}")
        structured_data["lock_wait_configuration"] = {"status": "error", "details": raw_config_result}
    else:
        adoc_content.append("=== Lock Wait Configuration")
        adoc_content.append(formatted_config_result)
        structured_data["lock_wait_configuration"] = {"status": "success", "data": raw_config_result}

    # Add recommendations
    adoc_content.append("\n=== Lock Management Recommendations")
    
    adoc_content.append("\n[TIP]\n====\n**Lock Management Best Practices:**\n")
    adoc_content.append("* **Set appropriate `lock_timeout`** to prevent indefinite blocking\n")
    adoc_content.append("* **Use `log_lock_waits`** to monitor lock contention\n")
    adoc_content.append("* **Keep transactions short** to minimize lock hold time\n")
    adoc_content.append("* **Use appropriate isolation levels** (READ COMMITTED vs SERIALIZABLE)\n")
    adoc_content.append("* **Order table access consistently** to prevent deadlocks\n")
    adoc_content.append("* **Monitor long-running transactions** that may hold locks\n")
    adoc_content.append("* **Use `pg_terminate_backend()`** for problematic sessions\n")
    adoc_content.append("====\n")
    
    adoc_content.append("\n[WARNING]\n====\n**Common Lock Issues:**\n")
    adoc_content.append("* **Exclusive locks on tables** during DDL operations\n")
    adoc_content.append("* **Long-running transactions** holding locks\n")
    adoc_content.append("* **Deadlocks** from inconsistent access patterns\n")
    adoc_content.append("* **Lock escalation** in high-concurrency environments\n")
    adoc_content.append("* **Missing indexes** causing table-level locks\n")
    adoc_content.append("====\n")
    
    if settings['is_aurora'] == 'true':
        adoc_content.append("\n[NOTE]\n====\n**AWS RDS Aurora Considerations:**\n")
        adoc_content.append("* Aurora has enhanced lock monitoring through CloudWatch\n")
        adoc_content.append("* Use Performance Insights to analyze lock contention\n")
        adoc_content.append("* Consider read replicas to reduce lock pressure\n")
        adoc_content.append("* Monitor `AuroraLockWaits` metric for lock wait time\n")
        adoc_content.append("====\n")
    
    # Add version-specific recommendations
    if pg_version and pg_version >= 14:
        adoc_content.append("\n[NOTE]\n====\n**PostgreSQL 14+ Lock Features:**\n")
        adoc_content.append("* **Enhanced lock monitoring** with better visibility\n")
        adoc_content.append("* **Improved deadlock detection** and reporting\n")
        adoc_content.append("* **Better lock timeout handling** with more granular control\n")
        adoc_content.append("====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data 