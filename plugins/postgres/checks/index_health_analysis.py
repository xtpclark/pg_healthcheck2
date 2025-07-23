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
                relname AS table_name,
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
            adoc_content.append("[IMPORTANT]\n====\nLarge indexes with very few scans consume disk space and slow down writes. Consider dropping them after verifying they are not for infrequent but critical reports (e.g., year-end reporting).\n====\n")
            adoc_content.append(formatted)
        structured_data["unused_indexes"] = {"status": "success", "data": raw}
    except Exception as e:
        adoc_content.append(f"[ERROR]\n====\nCould not analyze unused indexes: {e}\n====\n")

    # --- Duplicate Indexes ---
    try:
        adoc_content.append("\n==== Duplicate Indexes")
        duplicate_idx_query = """
            SELECT
                pg_size_pretty(SUM(pg_relation_size(idx.indexrelid))::bigint) AS total_wasted_size,
                MAX(schemaname) || '.' || MAX(tablename) AS table_name,
                array_agg(indexname ORDER BY indexname) AS redundant_indexes
            FROM pg_indexes
            WHERE schemaname <> 'pg_catalog'
            GROUP BY indrelid, indkey, indclass, indpred
            HAVING COUNT(*) > 1
            ORDER BY SUM(pg_relation_size(idx.indexrelid)) DESC
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
