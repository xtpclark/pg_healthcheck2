def run_wait_event_analysis(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes PostgreSQL wait events to identify performance bottlenecks.
    
    Based on comprehensive wait event analysis techniques from PostgreSQL documentation
    and community best practices. This module helps identify where queries are spending
    time waiting and provides actionable recommendations for optimization.
    
    Args:
        cursor: Database cursor for direct queries
        settings: Configuration settings dictionary
        execute_query: Function to execute queries with error handling
        execute_pgbouncer: Function to execute queries against PgBouncer
        all_structured_findings: Dictionary to store all structured findings
    
    Returns:
        tuple: (formatted_asciidoc_content, structured_data)
    """
    adoc_content = ["=== Wait Event Analysis", "Analyzes PostgreSQL wait events to identify performance bottlenecks and optimization opportunities.\n"]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    # Show queries if requested
    if settings['show_qry'] == 'true':
        adoc_content.append("Wait Event Analysis queries:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("-- Current wait events by type")
        adoc_content.append("SELECT wait_event_type, wait_event, COUNT(*) as session_count FROM pg_stat_activity WHERE wait_event_type IS NOT NULL GROUP BY wait_event_type, wait_event;")
        adoc_content.append("-- Sessions currently waiting")
        adoc_content.append("SELECT pid, usename, state, wait_event_type, wait_event, query FROM pg_stat_activity WHERE state = 'waiting';")
        adoc_content.append("-- Lock wait events")
        adoc_content.append("SELECT pid, usename, wait_event_type, wait_event, query FROM pg_stat_activity WHERE wait_event_type = 'Lock';")
        adoc_content.append("----")

    # Define comprehensive wait event analysis queries
    queries = [
        (
            "Current Wait Events Overview", 
            """
            SELECT 
                wait_event_type,
                wait_event,
                COUNT(*) as session_count,
                ROUND(AVG(EXTRACT(EPOCH FROM (NOW() - query_start))), 2) as avg_wait_duration_seconds
            FROM pg_stat_activity 
            WHERE datname = %(database)s
                AND wait_event_type IS NOT NULL
                AND wait_event IS NOT NULL
                AND query NOT LIKE %(autovacuum_pattern)s
            GROUP BY wait_event_type, wait_event
            ORDER BY session_count DESC, avg_wait_duration_seconds DESC
            LIMIT %(limit)s;
            """, 
            True,
            "wait_events_overview"
        ),
        (
            "Sessions Currently Waiting", 
            """
            SELECT 
                pid,
                usename,
                state,
                wait_event_type,
                wait_event,
                LEFT(query, 100) as query_preview,
                ROUND(EXTRACT(EPOCH FROM (NOW() - query_start)), 2) as wait_duration_seconds,
                application_name
            FROM pg_stat_activity 
            WHERE datname = %(database)s
                AND state = 'waiting'
                AND query NOT LIKE %(autovacuum_pattern)s
            ORDER BY wait_duration_seconds DESC
            LIMIT %(limit)s;
            """, 
            True,
            "sessions_waiting"
        ),
        (
            "Lock Wait Events Analysis", 
            """
            SELECT 
                pid,
                usename,
                wait_event_type,
                wait_event,
                LEFT(query, 100) as query_preview,
                ROUND(EXTRACT(EPOCH FROM (NOW() - query_start)), 2) as wait_duration_seconds,
                application_name
            FROM pg_stat_activity 
            WHERE datname = %(database)s
                AND wait_event_type = 'Lock'
                AND query NOT LIKE %(autovacuum_pattern)s
            ORDER BY wait_duration_seconds DESC
            LIMIT %(limit)s;
            """, 
            True,
            "lock_wait_events"
        ),
        (
            "I/O and Buffer Wait Events", 
            """
            SELECT 
                wait_event_type,
                wait_event,
                COUNT(*) as session_count,
                ROUND(AVG(EXTRACT(EPOCH FROM (NOW() - query_start))), 2) as avg_wait_duration_seconds
            FROM pg_stat_activity 
            WHERE datname = %(database)s
                AND wait_event_type IN ('IO', 'BufferPin', 'BufferIO')
                AND query NOT LIKE %(autovacuum_pattern)s
            GROUP BY wait_event_type, wait_event
            ORDER BY session_count DESC, avg_wait_duration_seconds DESC
            LIMIT %(limit)s;
            """, 
            True,
            "io_buffer_wait_events"
        ),
        (
            "LWLock Wait Events", 
            """
            SELECT 
                pid,
                usename,
                wait_event_type,
                wait_event,
                LEFT(query, 100) as query_preview,
                ROUND(EXTRACT(EPOCH FROM (NOW() - query_start)), 2) as wait_duration_seconds
            FROM pg_stat_activity 
            WHERE datname = %(database)s
                AND wait_event_type = 'LWLock'
                AND query NOT LIKE %(autovacuum_pattern)s
            ORDER BY wait_duration_seconds DESC
            LIMIT %(limit)s;
            """, 
            True,
            "lwlock_wait_events"
        )
    ]

    # Process each query
    for title, query, condition, data_key in queries:
        if not condition:
            adoc_content.append(f"{title}\n[NOTE]\n====\nQuery not applicable.\n====\n")
            structured_data[data_key] = {"status": "not_applicable", "reason": "Query not applicable due to condition."}
            continue
        
        # Standardized parameter passing pattern:
        params_for_query = {}
        if '%(limit)s' in query:
            params_for_query['limit'] = settings['row_limit']
        if '%(database)s' in query:
            params_for_query['database'] = settings['database']
        if '%(autovacuum_pattern)s' in query:
            params_for_query['autovacuum_pattern'] = 'autovacuum:%'
        if not params_for_query:
            params_for_query = None
        
        formatted_result, raw_result = execute_query(query, params=params_for_query, return_raw=True)
        
        if "[ERROR]" in formatted_result:
            adoc_content.append(f"{title}\n{formatted_result}")
            structured_data[data_key] = {"status": "error", "details": raw_result}
        else:
            adoc_content.append(title)
            adoc_content.append(formatted_result)
            structured_data[data_key] = {"status": "success", "data": raw_result}
    
    # Add comprehensive analysis and recommendations based on the guide
    adoc_content.append("\n=== Wait Event Analysis and Recommendations")
    
    adoc_content.append("\n[TIP]\n====\n**Understanding Wait Events:**\n")
    adoc_content.append("* **Lock waits**: Indicate transaction contention - consider shorter transactions or better indexing\n")
    adoc_content.append("* **I/O waits**: Suggest disk performance issues - consider SSD, better I/O configuration\n")
    adoc_content.append("* **BufferPin waits**: Indicate memory pressure - consider increasing shared_buffers\n")
    adoc_content.append("* **LWLock waits**: Suggest internal contention - monitor checkpoint and WAL activity\n")
    adoc_content.append("* **ClientRead/ClientWrite**: Network or client-side issues\n")
    adoc_content.append("====\n")
    
    adoc_content.append("\n[WARNING]\n====\n**Common Performance Issues by Wait Event Type:**\n")
    adoc_content.append("* **High Lock waits**: Review transaction isolation levels, add indexes, optimize queries\n")
    adoc_content.append("* **Frequent I/O waits**: Check disk performance, consider read replicas, optimize queries\n")
    adoc_content.append("* **BufferPin contention**: Increase shared_buffers, review checkpoint settings\n")
    adoc_content.append("* **LWLockNamed waits**: Monitor checkpoint frequency, adjust WAL settings\n")
    adoc_content.append("====\n")
    
    adoc_content.append("\n[NOTE]\n====\n**Optimization Strategies:**\n")
    adoc_content.append("* **For Lock waits**: Use `pg_stat_activity` to identify blocking queries\n")
    adoc_content.append("* **For I/O waits**: Monitor `pg_stat_bgwriter` and disk I/O metrics\n")
    adoc_content.append("* **For Buffer waits**: Check `shared_buffers` and `effective_cache_size` settings\n")
    adoc_content.append("* **For LWLock waits**: Review `checkpoint_segments` and WAL configuration\n")
    adoc_content.append("====\n")
    
    # Add Aurora-specific considerations
    if settings['is_aurora'] == 'true':
        adoc_content.append("\n[NOTE]\n====\n**AWS RDS Aurora Wait Event Considerations:**\n")
        adoc_content.append("* Aurora has optimized I/O handling - monitor `AuroraIOPS` and `AuroraVolumeReadIOPs`\n")
        adoc_content.append("* Use Performance Insights to correlate wait events with query performance\n")
        adoc_content.append("* Aurora read replicas can help reduce lock contention\n")
        adoc_content.append("* Monitor `AuroraLockWaits` metric for lock-specific performance issues\n")
        adoc_content.append("====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data
