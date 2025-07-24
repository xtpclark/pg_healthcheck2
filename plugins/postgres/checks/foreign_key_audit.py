import re
from plugins.postgres.utils.postgresql_version_compatibility import (
    get_missing_fk_indexes_query,
    get_fk_summary_query
)

def run_foreign_key_audit(connector, settings):
    """
    Audits foreign key constraints, providing a summary and identifying unindexed
    foreign keys that can cause write amplification.
    """
    adoc_content = ["=== Foreign Key Integrity and Performance Audit", "Audits foreign key constraints to identify potential write-amplification issues caused by unindexed foreign keys.\n"]
    structured_data = {}
    params = {'limit': settings.get('row_limit', 10)}

    # --- 1. Get the High-Level Summary for AI Analysis ---
    try:
        summary_query = get_fk_summary_query(connector)
        _, summary_raw = connector.execute_query(summary_query, return_raw=True)
        structured_data["foreign_key_summary"] = {"status": "success", "data": summary_raw[0] if summary_raw else {}}
    except Exception as e:
        adoc_content.append(f"\n[ERROR]\n====\nCould not retrieve foreign key summary: {e}\n====\n")
        structured_data["foreign_key_summary"] = {"status": "error", "details": str(e)}

    # --- 2. Find and Display Unindexed Foreign Keys ---
    missing_fk_indexes_raw = []
    try:
        adoc_content.append("\n==== Unindexed Foreign Keys (Write Performance Risk)")
        missing_fk_query = get_missing_fk_indexes_query(connector)
        formatted, raw = connector.execute_query(missing_fk_query, params=params, return_raw=True)
        missing_fk_indexes_raw = raw if isinstance(raw, list) else []

        if "[ERROR]" in formatted:
            adoc_content.append(formatted)
        elif not missing_fk_indexes_raw:
            adoc_content.append("[NOTE]\n====\nNo unindexed foreign keys found. This is excellent for write performance and data integrity.\n====\n")
        else:
            # --- Restored and Enhanced Explanatory Note ---
            adoc_content.append("[CRITICAL]\n====\n"
                                "**Action Required: Unindexed Foreign Keys Found**\n\n"
                                "When a row in a parent table is `DELETED` or its key is `UPDATED`, PostgreSQL must check the child table for referencing rows. Without an index on the foreign key column(s), this check requires a **full sequential scan** on the child table.\n\n"
                                "This leads to severe **write amplification**, causing excessive I/O, high CPU usage, and potential locking issues. The queries below list the foreign keys that are missing a necessary index.\n"
                                "====\n")
            adoc_content.append(formatted)
        structured_data["missing_fk_indexes_details"] = {"status": "success", "data": missing_fk_indexes_raw}
    except Exception as e:
        adoc_content.append(f"\n[ERROR]\n====\nCould not analyze for missing foreign key indexes: {e}\n====\n")

    # --- 3. Generate Recommended SQL ---
    if missing_fk_indexes_raw:
        adoc_content.append("\n==== Recommended SQL to Create Missing Indexes")
        adoc_content.append("[IMPORTANT]\n====\nThe following `CREATE INDEX CONCURRENTLY` statements are recommended to resolve the performance issues. Always test these changes in a staging environment before applying to production.\n====\n")
        
        sql_to_run = []
        for fk_info in missing_fk_indexes_raw:
            child_table = fk_info.get('child_table', '')
            fk_cols = fk_info.get('fk_col_names', [])
            
            if child_table and fk_cols:
                sanitized_cols = '_'.join(re.sub(r'[^a-zA-Z0-9_]+', '', col) for col in fk_cols)
                sanitized_table = re.sub(r'[^a-zA-Z0-9_]+', '', child_table.split('.')[-1])
                index_name = f"idx_{sanitized_table}_{sanitized_cols}_fk"
                
                sql_to_run.append(f"CREATE INDEX CONCURRENTLY {index_name} ON {child_table} ({', '.join(fk_cols)});")
        
        if sql_to_run:
            adoc_content.append("[,sql]\n----")
            adoc_content.append("\n".join(sql_to_run))
            adoc_content.append("----")
            
    # --- Restored Aurora-specific note ---
    if settings.get('is_aurora'):
        adoc_content.append("\n[NOTE]\n====\n"
                       "**Aurora Specifics:** In AWS RDS Aurora, write amplification from unindexed foreign keys directly contributes to higher `WriteIOPS` and `CPUUtilization` in CloudWatch, which can increase costs and lead to performance throttling. "
                       "Regularly auditing foreign key indexes is crucial for maintaining optimal write performance in Aurora.\n"
                       "====\n")

    return "\n".join(adoc_content), structured_data
