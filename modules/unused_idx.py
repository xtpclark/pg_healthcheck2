def run_unused_idx(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Identifies potentially unused indexes in the PostgreSQL database.
    """
    adoc_content = ["Identifies potentially unused indexes in the PostgreSQL database.\n"]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    if settings['show_qry'] == 'true':
        adoc_content.append("Unused indexes query:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("SELECT schemaname||'.'||relname AS table_name, indexrelname AS index_name, ")
        adoc_content.append("       idx_scan, idx_tup_read, idx_tup_fetch, ")
        adoc_content.append("       pg_size_pretty(pg_relation_size(indexrelid)) AS index_size ")
        adoc_content.append("FROM pg_stat_user_indexes ")
        adoc_content.append("WHERE idx_scan = 0 ")
        adoc_content.append("ORDER BY pg_relation_size(indexrelid) DESC ")
        adoc_content.append("LIMIT %(limit)s;")
        adoc_content.append("----")

    query = """
SELECT schemaname||'.'||relname AS table_name, indexrelname AS index_name, 
       idx_scan, idx_tup_read, idx_tup_fetch, 
       pg_size_pretty(pg_relation_size(indexrelid)) AS index_size 
FROM pg_stat_user_indexes 
WHERE idx_scan = 0 
ORDER BY pg_relation_size(indexrelid) DESC 
LIMIT %(limit)s;
"""
    
    # Standardized parameter passing pattern:
    params_for_query = {'limit': settings['row_limit']} if '%(limit)s' in query else None
    
    formatted_result, raw_result = execute_query(query, params=params_for_query, return_raw=True)
    
    if "[ERROR]" in formatted_result:
        adoc_content.append(f"Unused Indexes\n{formatted_result}")
        structured_data["unused_indexes"] = {"status": "error", "details": raw_result}
    else:
        adoc_content.append("Unused Indexes")
        adoc_content.append(formatted_result)
        structured_data["unused_indexes"] = {"status": "success", "data": raw_result} # Store raw data

    # Add important warnings about read replicas
    adoc_content.append("\n[IMPORTANT]\n====\n")
    adoc_content.append("**⚠️ CRITICAL WARNING: Read Replica Considerations**\n\n")
    adoc_content.append("The indexes listed above appear unused on **this node only**. ")
    adoc_content.append("However, in environments with read replicas:\n\n")
    adoc_content.append("- **Primary/Master nodes** typically handle writes (INSERT, UPDATE, DELETE)\n")
    adoc_content.append("- **Read replicas** handle SELECT queries and read operations\n")
    adoc_content.append("- **Index usage statistics** are tracked separately on each node\n")
    adoc_content.append("- An index that appears unused on the primary may be heavily used on replicas\n\n")
    adoc_content.append("**Before removing any index:**\n")
    adoc_content.append("1. Check index usage on ALL read replicas\n")
    adoc_content.append("2. Verify the index doesn't support any constraints (unique, foreign key)\n")
    adoc_content.append("3. Test removal in a staging environment\n")
    adoc_content.append("4. Monitor performance impact after removal\n")
    adoc_content.append("====\n")

    if settings.get('is_aurora', False):
        adoc_content.append("[NOTE]\n====\n")
        adoc_content.append("**AWS RDS Aurora Environment:**\n")
        adoc_content.append("- Aurora read replicas may have completely different index usage patterns\n")
        adoc_content.append("- Check index usage on ALL Aurora read replicas before removal\n")
        adoc_content.append("- Use Performance Insights to analyze query patterns across nodes\n")
        adoc_content.append("- Monitor `ReadIOPS` and `WriteIOPS` after index changes\n")
        adoc_content.append("====\n")

    adoc_content.append("[TIP]\n====\n")
    adoc_content.append("**Best Practice:** When in doubt, keep the index. ")
    adoc_content.append("The storage cost is usually minimal compared to the performance benefit. ")
    adoc_content.append("It's much easier to remove an index later than to recreate it if you discover it was needed.\n")
    adoc_content.append("====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data

