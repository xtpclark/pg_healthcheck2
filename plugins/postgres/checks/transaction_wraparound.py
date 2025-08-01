import re
from plugins.postgres.utils.qrylib.transaction_wraparound import (
    get_database_wraparound_query,
    get_table_wraparound_query,
    get_vacuum_age_settings_query,
    get_dead_tuples_for_table_query,
    get_autovacuum_memory_settings_query
)

def _to_bytes(value_str, unit_str):
    """Helper function to convert PostgreSQL memory settings to bytes."""
    if not unit_str or not value_str or not value_str.isdigit():
        return 0
    value = int(value_str)
    unit_str = unit_str.upper()
    if unit_str == 'KB': return value * 1024
    if unit_str == 'MB': return value * 1024**2
    if unit_str == 'GB': return value * 1024**3
    if 'KB' in unit_str: return value * int(re.findall(r'\d+', unit_str)[0]) * 1024
    return value

def get_weight():
    """Returns the importance score for this module."""
    return 10

def run_transaction_wraparound(connector, settings):
    """
    Analyzes databases and tables for wraparound risk and provides specific
    recommendations for autovacuum memory tuning.
    """
    adoc_content = ["=== Transaction ID Wraparound Analysis", "Monitors the age of the oldest transaction ID. If this age reaches the `autovacuum_freeze_max_age` limit, the database will shut down to prevent data corruption.\n"]
    structured_data = {}
    params = {'limit': settings.get('row_limit', 10)}
    settings = settings or {}

    # --- Database-Level Analysis ---
    raw_db_risk = []
    try:
        query = get_database_wraparound_query(connector)
        formatted, raw_db_risk = connector.execute_query(query, return_raw=True)
        # (Analysis and CRITICAL block logic from previous step)
        adoc_content.append("==== Wraparound Risk by Database")
        adoc_content.append(formatted)
        structured_data["database_wraparound_risk"] = {"status": "success", "data": raw_db_risk}
    except Exception as e:
        adoc_content.append(f"\n[ERROR]\n====\nCould not analyze database-level wraparound risk: {e}\n====\n")

    # --- Table-Level Analysis ---
    raw_table_risk = []
    try:
        adoc_content.append("\n==== Top Tables Contributing to Wraparound Risk")
        query = get_table_wraparound_query(connector)
        formatted, raw_table_risk = connector.execute_query(query, params=params, return_raw=True)
        adoc_content.append(formatted)
        structured_data["table_wraparound_risk"] = {"status": "success", "data": raw_table_risk}
    except Exception as e:
        adoc_content.append(f"\n[ERROR]\n====\nCould not analyze table-level wraparound risk: {e}\n====\n")

    # --- NEW: Per-Table Autovacuum Memory Analysis ---
    if raw_table_risk:
        try:
            # Get current memory settings
            _, mem_settings_raw = connector.execute_query(get_autovacuum_memory_settings_query(connector), return_raw=True)
            mem_settings = {s['name']: s for s in mem_settings_raw}
            av_work_mem_val = mem_settings.get('autovacuum_work_mem', {}).get('setting', '-1')
            av_work_mem_unit = mem_settings.get('autovacuum_work_mem', {}).get('unit')

            # If av_work_mem is -1, it uses maintenance_work_mem
            if av_work_mem_val == '-1':
                av_work_mem_val = mem_settings.get('maintenance_work_mem', {}).get('setting', '65536')
                av_work_mem_unit = mem_settings.get('maintenance_work_mem', {}).get('unit', 'kB')

            current_av_work_mem_bytes = _to_bytes(av_work_mem_val, av_work_mem_unit)
            
            per_table_findings = ["\n==== Per-Table Vacuum Memory Analysis"]
            recommendations = []

            for table in raw_table_risk:
                table_oid = table.get('oid')
                _, dead_tup_raw = connector.execute_query(get_dead_tuples_for_table_query(connector), params=(table_oid,), return_raw=True)
                n_dead_tup = dead_tup_raw[0]['n_dead_tup'] if dead_tup_raw else 0
                
                # Each dead tuple requires 6 bytes in autovacuum_work_mem
                estimated_mem_needed_bytes = n_dead_tup * 6
                
                if estimated_mem_needed_bytes > current_av_work_mem_bytes:
                    # Calculate a recommended size, adding a 25% buffer
                    recommended_mem_kb = int((estimated_mem_needed_bytes * 1.25) / 1024)
                    # Cap recommendation at 1GB for PG <= 16
                    if connector.version_info.get('major_version', 0) <= 16:
                        recommended_mem_kb = min(recommended_mem_kb, 1024 * 1024)

                    finding = (f"* `{table.get('table_name')}` has `{n_dead_tup:,}` dead tuples, requiring "
                               f"`{int(estimated_mem_needed_bytes / 1024**2)}MB` of memory for an efficient vacuum. This "
                               f"exceeds the current `autovacuum_work_mem` of `{int(current_av_work_mem_bytes / 1024**2)}MB`.")
                    per_table_findings.append(finding)
                    recommendations.append(f"ALTER TABLE {table.get('table_name')} SET (autovacuum_work_mem = '{recommended_mem_kb}kB');")

            if recommendations:
                adoc_content.append("[CAUTION]\n====\nThe following tables have more dead tuples than can be processed in a single vacuum pass with the current `autovacuum_work_mem` setting. This can significantly slow down cleanup.\n\n" + "\n".join(per_table_findings) + "\n====")
                adoc_content.append("\n==== Recommended Per-Table Settings")
                adoc_content.append("[TIP]\n====\nApply these settings to the problematic tables to allow autovacuum to process them more efficiently. Test these changes in a staging environment first.\n====\n")
                adoc_content.append(f"[,sql]\n----\n" + "\n".join(recommendations) + "\n----")
                
        except Exception as e:
            adoc_content.append(f"\n[ERROR]\n====\nCould not perform per-table vacuum analysis: {e}\n====\n")
            
    # --- Expanded TIP Block ---
    tip_content = ["\n[TIP]\n====\nIf a database is nearing the wraparound limit, you must run `VACUUM` on high-risk tables to freeze old transaction IDs. However, ensuring autovacuum can work efficiently is the best long-term solution."]
    
    # Add Aurora-specific advice
    if settings.get('is_aurora', False):
        aurora_tip = ("\n\n*For AWS Aurora Users:* Aurora's `rds.adaptive_autovacuum` feature helps prevent wraparound by making vacuums more aggressive when transaction age is high. "
                      "Verify this is enabled (`1`) in your DB parameter group. Even with this feature, an extreme workload can outpace autovacuum, making per-table `autovacuum_work_mem` tuning critical.")
        tip_content.append(aurora_tip)
    
    # Add general advice about work_mem
    pg_version = connector.version_info.get('major_version', 0)
    work_mem_advice = (f"\n\n*`autovacuum_work_mem`*: This setting is critical for performance. If too low, autovacuum must scan a table's indexes multiple times to clean all dead tuples. "
                       f"For PostgreSQL versions 16 and prior, its effective maximum is 1GB. This limit is removed in version 17.")
    tip_content.append(work_mem_advice)
    
    tip_content.append("\n====\n")
    adoc_content.append("".join(tip_content))

    return "\n".join(adoc_content), structured_data
