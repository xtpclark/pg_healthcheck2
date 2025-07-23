# Import the centralized compatibility module
from .postgresql_version_compatibility import get_postgresql_version, get_pg_stat_statements_query, validate_postgresql_version

def run_hot_queries(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes top queries by total execution time from pg_stat_statements,
    providing both a detailed list of the top offenders and a high-level summary of the overall query workload.
    This module is version-aware.
    """
    adoc_content = ["=== Top Queries by Execution Time (Hot Queries)\nIdentifies the most resource-intensive queries based on their total execution time.\n"]
    structured_data = {}

    # --- Get PostgreSQL Version and Check pg_stat_statements ---
    try:
        compatibility = get_postgresql_version(cursor, execute_query)
        is_supported, error_msg = validate_postgresql_version(compatibility)
        if not is_supported:
            adoc_content.append(f"[ERROR]\n====\n{error_msg}\n====\n")
            return "\n".join(adoc_content), {"status": "error", "details": error_msg}
        
        if settings['has_pgstat'] != 't':
            adoc_content.append("[NOTE]\n====\n`pg_stat_statements` extension is not installed or enabled. Hot query analysis cannot be performed.\n====\n")
            return "\n".join(adoc_content), {"status": "not_applicable", "reason": "pg_stat_statements not enabled."}
    except Exception as e:
        adoc_content.append(f"[ERROR]\n====\nCould not determine PostgreSQL version or pg_stat_statements status: {e}\n====\n")
        return "\n".join(adoc_content), {"status": "error", "details": str(e)}

    # --- Define Queries ---
    # Query for the top N most time-consuming queries
    top_queries_query = get_pg_stat_statements_query(compatibility, 'standard') + " LIMIT %(limit)s;"
    
    # NEW: Query for summary statistics across all queries
    total_time_column = "total_exec_time" if compatibility['is_pg13_or_newer'] else "total_time"
    query_summary_query = f"""
        SELECT
            COUNT(*) AS total_queries_tracked,
            SUM({total_time_column}) AS total_execution_time_all_queries_ms
        FROM pg_stat_statements;
    """

    if settings['show_qry'] == 'true':
        adoc_content.append("Hot query analysis queries:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append(top_queries_query)
        adoc_content.append(query_summary_query)
        adoc_content.append("----")
    
    # --- Execute Queries ---
    
    # 1. Top N "Hot" Queries
    params = {'limit': settings['row_limit']}
    formatted_result, raw_result = execute_query(top_queries_query, params=params, return_raw=True)
    adoc_content.append("Top 'Hot' Queries by Total Execution Time")
    adoc_content.append(formatted_result)
    structured_data["top_hot_queries"] = {"status": "success", "data": raw_result}

    # 2. Query Workload Summary
    formatted_result, raw_result_summary = execute_query(query_summary_query, return_raw=True)
    adoc_content.append("\nOverall Query Workload Summary")
    adoc_content.append(formatted_result)
    structured_data["hot_query_summary"] = {"status": "success", "data": raw_result_summary}
    
    adoc_content.append("\n[TIP]\n====\nAnalyze the 'Overall Query Workload Summary'. If the top queries contribute a large percentage of the total execution time, optimizing them will have a significant impact on database performance.\n====")

    return "\n".join(adoc_content), structured_data
