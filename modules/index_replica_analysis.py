#!/usr/bin/env python3
"""
Index Replica Analysis Module

This module provides guidance and analysis for index usage in environments with read replicas,
where indexes that appear unused on the primary may actually be heavily used on replicas.
"""

def run_index_replica_analysis(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Provides clear guidance for analyzing index usage in environments with read replicas.
    """
    adoc_content = ["Provides clear guidance for analyzing index usage in environments with read replicas.\n"]
    structured_data = {}

    # Add comprehensive guidance about read replica index considerations
    adoc_content.append("[IMPORTANT]\n====\n")
    adoc_content.append("**🔍 CRITICAL: Index Usage Analysis in Read Replica Environments**\n\n")
    adoc_content.append("**The Problem:** Index usage statistics are tracked separately on each database instance. ")
    adoc_content.append("An index that appears 'unused' on your primary/master node may actually be heavily used on read replicas.\n\n")
    adoc_content.append("**Why This Matters:** Removing an index that's used on replicas can cause:\n")
    adoc_content.append("- Performance degradation on read replicas\n")
    adoc_content.append("- Increased query execution time\n")
    adoc_content.append("- Higher CPU usage on replica nodes\n")
    adoc_content.append("- Potential application timeouts\n")
    adoc_content.append("====\n")

    adoc_content.append("**📊 Understanding Query Distribution**\n\n")
    adoc_content.append("In typical read replica setups:\n")
    adoc_content.append("- **Primary/Master Node**: Handles all write operations (INSERT, UPDATE, DELETE)\n")
    adoc_content.append("- **Read Replicas**: Handle SELECT queries and read operations\n")
    adoc_content.append("- **Application Routing**: Queries are often automatically routed based on operation type\n\n")

    adoc_content.append("**📈 Key Index Statistics to Monitor**\n\n")
    adoc_content.append("- `idx_scan`: Number of times this index has been scanned (per-instance)\n")
    adoc_content.append("- `idx_tup_read`: Number of index entries returned by scans\n")
    adoc_content.append("- `idx_tup_fetch`: Number of live table rows fetched by simple index scans\n\n")

    adoc_content.append("**🔍 Step-by-Step Analysis Process**\n\n")
    adoc_content.append("**Step 1: Check Index Usage on Primary Node**\n")
    adoc_content.append("```sql\n")
    adoc_content.append("SELECT schemaname||'.'||relname AS table_name, indexrelname AS index_name, ")
    adoc_content.append("       idx_scan, idx_tup_read, idx_tup_fetch ")
    adoc_content.append("FROM pg_stat_user_indexes ")
    adoc_content.append("WHERE idx_scan = 0 ")
    adoc_content.append("ORDER BY schemaname, relname, indexrelname;\n")
    adoc_content.append("```\n\n")

    adoc_content.append("**Step 2: Check Index Usage on ALL Read Replicas**\n")
    adoc_content.append("⚠️ **CRITICAL**: Run the exact same query on every read replica in your cluster.\n")
    adoc_content.append("This is the most important step - don't skip it!\n\n")

    adoc_content.append("**Step 3: Aggregate Results**\n")
    adoc_content.append("Combine the results from all nodes. Only consider an index 'unused' if:\n")
    adoc_content.append("- `idx_scan = 0` on ALL nodes (primary + all replicas)\n")
    adoc_content.append("- The index doesn't support any constraints (unique, foreign key, etc.)\n")
    adoc_content.append("- You've verified no application queries use it\n\n")

    adoc_content.append("**Step 4: Verify No Constraints**\n")
    adoc_content.append("```sql\n")
    adoc_content.append("-- Check for unique constraints\n")
    adoc_content.append("SELECT conname, conrelid::regclass, contype ")
    adoc_content.append("FROM pg_constraint ")
    adoc_content.append("WHERE contype = 'u' AND conrelid::regclass::text LIKE '%your_table%';\n\n")
    adoc_content.append("-- Check for foreign key constraints\n")
    adoc_content.append("SELECT conname, conrelid::regclass, confrelid::regclass ")
    adoc_content.append("FROM pg_constraint ")
    adoc_content.append("WHERE contype = 'f' AND conrelid::regclass::text LIKE '%your_table%';\n")
    adoc_content.append("```\n\n")

    if settings.get('is_aurora', False):
        adoc_content.append("**☁️ Aurora-Specific Considerations**\n\n")
        adoc_content.append("**AWS RDS Aurora Environment:**\n")
        adoc_content.append("- Aurora read replicas may have completely different index usage patterns than the writer\n")
        adoc_content.append("- Check index usage on ALL Aurora read replicas before any removal\n")
        adoc_content.append("- Monitor `ReadIOPS` and `WriteIOPS` after index changes\n")
        adoc_content.append("- Use Performance Insights to analyze query patterns across nodes\n")
        adoc_content.append("- Consider the impact on Aurora's storage optimization\n\n")
    else:
        adoc_content.append("**🖥️ Standard PostgreSQL Considerations**\n\n")
        adoc_content.append("**Standard PostgreSQL with Replicas:**\n")
        adoc_content.append("- Check index usage on all replica nodes\n")
        adoc_content.append("- Use `pg_stat_statements` to identify which queries use which indexes\n")
        adoc_content.append("- Use `EXPLAIN ANALYZE` to verify index usage in query plans\n")
        adoc_content.append("- Monitor replica lag during index operations\n\n")

    adoc_content.append("**🛡️ Safe Index Removal Checklist**\n\n")
    adoc_content.append("Before removing ANY index, verify:\n")
    adoc_content.append("✅ **Usage on ALL nodes**: `idx_scan = 0` on primary + all replicas\n")
    adoc_content.append("✅ **No unique constraints**: Index doesn't support UNIQUE constraints\n")
    adoc_content.append("✅ **No foreign key constraints**: Index doesn't support FK relationships\n")
    adoc_content.append("✅ **Application analysis**: No application queries use this index\n")
    adoc_content.append("✅ **Staging test**: Tested removal in staging environment\n")
    adoc_content.append("✅ **Low-traffic window**: Removal scheduled during maintenance window\n")
    adoc_content.append("✅ **Monitoring plan**: Performance monitoring before/after removal\n\n")

    adoc_content.append("**📋 Useful Monitoring Queries**\n\n")
    adoc_content.append("```sql\n")
    adoc_content.append("-- Check index sizes (largest unused indexes first)\n")
    adoc_content.append("SELECT schemaname||'.'||relname AS table_name, indexrelname AS index_name, ")
    adoc_content.append("       pg_size_pretty(pg_relation_size(indexrelid)) AS index_size ")
    adoc_content.append("FROM pg_stat_user_indexes ")
    adoc_content.append("WHERE idx_scan = 0 ")
    adoc_content.append("ORDER BY pg_relation_size(indexrelid) DESC;\n\n")
    adoc_content.append("-- Find queries that might use specific tables\n")
    adoc_content.append("SELECT query, calls, total_exec_time, mean_exec_time ")
    adoc_content.append("FROM pg_stat_statements ")
    adoc_content.append("WHERE query LIKE '%table_name%' ")
    adoc_content.append("ORDER BY total_exec_time DESC;\n")
    adoc_content.append("```\n\n")

    adoc_content.append("[TIP]\n====\n")
    adoc_content.append("**Golden Rule**: When in doubt, keep the index. ")
    adoc_content.append("The storage cost is usually minimal compared to the performance benefit. ")
    adoc_content.append("It's much easier to remove an index later than to recreate it if you discover it was needed.\n")
    adoc_content.append("====\n")

    # Store structured data
    structured_data["index_replica_analysis"] = {
        "status": "success",
        "data": {
            "guidance_provided": True,
            "read_replica_considerations": True,
            "aurora_specific": settings.get('is_aurora', False)
        }
    }

    return "\n".join(adoc_content), structured_data 