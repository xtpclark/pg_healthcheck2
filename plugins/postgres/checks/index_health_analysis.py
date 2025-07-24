def run_index_health_analysis(connector, settings):
    """
    Performs a comprehensive analysis of index health, identifying unused,
    duplicate, and invalid indexes that can harm performance.
    """
    adoc_content = ["=== Index Health and Maintenance", "Provides a consolidated to-do list for index maintenance, identifying issues that can consume resources and slow down write operations.\n"]
    structured_data = {}

    # --- Unused Indexes ---
    try:
        adoc_content.append("==== Unused Indexes")
        unused_idx_query = """
            SELECT
                schemaname AS schema_name,
                i.relname AS table_name,
                indexrelname AS index_name,
                pg_size_pretty(pg_relation_size(i.indexrelid)) AS index_size,
                idx_scan AS index_scans
            FROM pg_stat_user_indexes i
            JOIN pg_class c ON i.indexrelid = c.oid
            WHERE idx_scan < 100 AND pg_relation_size(i.indexrelid) > 1048576 -- Scanned < 100 times and > 1MB
            ORDER BY pg_relation_size(i.indexrelid) DESC
            LIMIT %(limit)s;
        """
        params = {'limit': settings.get('row_limit', 10)}
        formatted, raw = connector.execute_query(unused_idx_query, params=params, return_raw=True)
        if "[ERROR]" in formatted:
            adoc_content.append(formatted)
        elif not raw:
            adoc_content.append("[NOTE]\n====\nNo significantly large and unused indexes found. This is a healthy sign.\n====\n")
        else:
            adoc_content.append("""
[IMPORTANT]
====
*CRITICAL CONSIDERATION FOR READ REPLICAS*

Indexes that appear 'unused' on the primary/writer node may be **heavily used** on read replicas. This is because index usage statistics (`pg_stat_user_indexes`) are instance-specific.

**Before dropping any index listed below, you MUST verify its usage on ALL read replicas.**

Dropping an index that is active on a replica will cause it to be dropped on the replica as well, which can lead to severe performance degradation for read queries.
====
""")
            adoc_content.append("Large indexes with very few scans consume disk space and slow down writes. After verifying usage across all replicas, consider dropping indexes that are truly unused.\n")
            adoc_content.append(formatted)
        structured_data["unused_indexes"] = {"status": "success", "data": raw}
    except Exception as e:
        adoc_content.append(f"[ERROR]\n====\nCould not analyze unused indexes: {e}\n====\n")


    # --- Duplicate Indexes ---
    try:
        adoc_content.append("\n==== Duplicate Indexes")
        duplicate_idx_query = """
            SELECT n.nspname || '.' || t.relname AS table_name,
            pg_size_pretty(SUM(pg_relation_size(pi.indexrelid))::bigint) AS total_wasted_size,
            array_agg(i.relname ORDER BY i.relname) AS redundant_indexes
             FROM
           pg_index AS pi
           JOIN pg_class AS i ON i.oid = pi.indexrelid
           JOIN pg_class AS t ON t.oid = pi.indrelid
           JOIN pg_namespace AS n ON n.oid = t.relnamespace
            WHERE
            n.nspname NOT IN ('pg_catalog', 'information_schema')
    AND pi.indisprimary = false
GROUP BY
    -- Group by the core properties that define a duplicate index
    pi.indrelid, 
    pi.indkey, 
    pi.indclass, 
    pi.indpred,
    n.nspname,
    t.relname
HAVING
    COUNT(*) > 1
ORDER BY
    SUM(pg_relation_size(pi.indexrelid)) DESC
LIMIT %(limit)s;
        """
        params = {'limit': settings.get('row_limit', 10)}
        formatted, raw = connector.execute_query(duplicate_idx_query, params=params, return_raw=True)
        if "[ERROR]" in formatted:
            adoc_content.append(formatted)
        elif not raw:
            adoc_content.append("[NOTE]\n====\nNo duplicate indexes found.\n====\n")
        else:
            adoc_content.append("[IMPORTANT]\n====\nDuplicate indexes provide no performance benefit and double the overhead on write operations. You can safely drop one of the indexes from each group listed below.\n====\n")
            adoc_content.append(formatted)
        structured_data["duplicate_indexes"] = {"status": "success", "data": raw}
    except Exception as e:
        adoc_content.append(f"[ERROR]\n====\nCould not analyze duplicate indexes: {e}\n====\n")

    # --- Invalid Indexes ---
    try:
        adoc_content.append("\n==== Invalid Indexes")
        invalid_idx_query = "SELECT n.nspname AS schema_name, c.relname AS table_name, i.relname AS index_name FROM pg_class c, pg_index ix, pg_class i, pg_namespace n WHERE ix.indisvalid = false AND ix.indexrelid = i.oid AND ix.indrelid = c.oid AND i.relnamespace = n.oid;"
        formatted, raw = connector.execute_query(invalid_idx_query, return_raw=True)
        if "[ERROR]" in formatted:
            adoc_content.append(formatted)
        elif not raw:
            adoc_content.append("[NOTE]\n====\nNo invalid indexes found.\n====\n")
        else:
            adoc_content.append("[IMPORTANT]\n====\nInvalid indexes are unusable by the planner and still consume space. They must be rebuilt. Use `REINDEX INDEX CONCURRENTLY <index_name>;` to fix them without locking the table.\n====\n")
            adoc_content.append(formatted)
        structured_data["invalid_indexes"] = {"status": "success", "data": raw}
    except Exception as e:
        adoc_content.append(f"[ERROR]\n====\nCould not analyze invalid indexes: {e}\n====\n")

    return "\n".join(adoc_content), structured_data
