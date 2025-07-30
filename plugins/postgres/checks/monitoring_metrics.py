from plugins.postgres.utils.qrylib.monitoring_metrics import (
    get_database_activity_stats_query,
    get_overall_transaction_buffer_stats_query,
    get_bgwriter_checkpoint_summary_query
)

def get_weight():
    """Returns the importance score for this module."""
    return 8 # High importance for core metrics

def run_monitoring_metrics(connector, settings):
    """
    Gathers key performance metrics for overall database health monitoring using
    the modern, refactored structure.
    """
    adoc_content = ["=== Core Monitoring Metrics", "Gathers key performance metrics for overall database health monitoring."]
    structured_data = {}

    queries_to_run = [
        (
            "Database Activity Statistics",
            get_database_activity_stats_query,
            "database_activity_stats"
        ),
        (
            "Overall Database Transaction & Buffer Stats",
            get_overall_transaction_buffer_stats_query,
            "overall_transaction_buffer_stats"
        ),
        (
            "Background Writer & Checkpoint Summary",
            get_bgwriter_checkpoint_summary_query,
            "bgwriter_checkpoint_summary"
        )
    ]

    for title, query_func, data_key in queries_to_run:
        try:
            query = query_func(connector)
            params = {'database': settings.get('database')} if '%(database)s' in query else None

            formatted_result, raw_result = connector.execute_query(query, params=params, return_raw=True)
            
            adoc_content.append(f"\n==== {title}")
            if "[ERROR]" in formatted_result:
                adoc_content.append(formatted_result)
                structured_data[data_key] = {"status": "error", "details": raw_result}
            else:
                adoc_content.append(formatted_result)
                structured_data[data_key] = {"status": "success", "data": raw_result}
        
        except Exception as e:
            adoc_content.append(f"\n==== {title}\n[ERROR]\n====\nCould not execute check: {e}\n====\n")
            structured_data[data_key] = {"status": "error", "details": str(e)}

    adoc_content.append("\n[TIP]\n====\n"
                   "Regularly monitoring these general metrics provides a high-level view of database activity. "
                   "High `xact_rollback` counts can indicate application errors or contention. "
                   "Compare `blks_read` vs `blks_hit` to understand cache efficiency.\n"
                   "====\n")
    if settings.get('is_aurora'):
        adoc_content.append("[NOTE]\n====\n"
                       "AWS RDS Aurora provides many of these metrics via CloudWatch (e.g., `DatabaseConnections`, `BufferCacheHitRatio`). "
                       "Use these PostgreSQL internal views for more granular details within the database instance itself.\n"
                       "====\n")
    
    return "\n".join(adoc_content), structured_data
