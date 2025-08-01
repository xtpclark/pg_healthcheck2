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
    # Transaction wraparound is a critical, service-impacting event.
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
        structured_data["database_wraparound_risk"] = {"status": "success", "data": raw_db_risk}

        high_risk_dbs = [db['datname'] for db in raw_db_risk if float(db.get('percent_towards_wraparound', 0)) > 75]
        if high_risk_dbs:
            db_list = ", ".join(f"`{db}`" for db in high_risk_dbs)
            critical_note = (
                f"\n[CRITICAL]\n====\n**Immediate Action Recommended:** The following database(s) are more than 75% towards the `autovacuum_freeze_max_age` limit: {db_list}. "
                "If this progresses, the database will shut down to prevent data loss. You must run `VACUUM` on the high-risk tables shown below.\n====\n"
            )
            adoc_content.append(critical_note)

        adoc_content.append("==== Wraparound Risk by Database")
        adoc_content.append(formatted)
        
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

    # --- Per-Table Autovacuum Memory Analysis ---
    if raw_table_risk:
        try:
            _, mem_settings_raw = connector.execute_query(get_autovacuum_memory_settings_query(connector), return_raw=True)
            mem_settings = {s['name']: s for s in mem_settings_raw}
            av_work_mem_val = mem_settings.get('autovacuum_work_mem', {}).get('setting', '-1')
            av_work_mem_unit = mem_settings.get('autovacuum_work_mem', {}).get('unit')

            if av_work_mem_val == '-1':
                av_work_mem_val = mem_settings.get('maintenance_work_mem', {}).get('setting', '65536')
                av_work_mem_unit = mem_settings.get('maintenance_work_mem', {}).get('unit', 'kB')

            current_av_work_mem_bytes = _to_bytes(av_work_mem_val, av_work_mem_unit)
            
            per_table_findings = []
            recommendations = []

            for table in raw_table_risk:
                table_oid = table.get('oid')
                _, dead_tup_raw = connector.execute_query(get_dead_tuples_for_table_query(connector), params=(table_oid,), return_raw=True)
                n_dead_tup = dead_tup_raw[0]['n_dead_tup'] if dead_tup_raw else 0
                estimated_mem_needed_bytes = n_dead_tup * 6
                
                if estimated_mem_needed_bytes > current_av_work_mem_bytes:
                    recommended_mem_kb = int((estimated_mem_needed_bytes * 1.25) / 1024)
                    if connector.version_info.get('major_version', 0) <= 16:
                        recommended_mem_kb = min(recommended_mem_kb, 1024 * 1024)

                    finding = (f"* `{table.get('table_name')}` has `{n_dead_tup:,}` dead tuples, requiring "
                               f"`{int(estimated_mem_needed_bytes / 1024**2)}MB` of memory for an efficient vacuum.")
                    per_table_findings.append(finding)
                    recommendations.append(f"ALTER TABLE {table.get('table_name')} SET (autovacuum_work_mem = '{recommended_mem_kb}kB');")

            if recommendations:
                adoc_content.append("\n==== Per-Table Vacuum Memory Analysis")
                adoc_content.append("[CAUTION]\n====\nThe following tables have more dead tuples than can be processed in a single vacuum pass with the current `autovacuum_work_mem` setting. This can significantly slow down cleanup.\n\n" + "\n".join(per_table_findings) + "\n====")
                adoc_content.append("\n==== Recommended Per-Table Settings")
                adoc_content.append("[TIP]\n====\nApply these settings to the problematic tables to allow autovacuum to process them more efficiently. Test these changes in a staging environment first.\n====\n")
                adoc_content.append(f"[,sql]\n----\n" + "\n".join(recommendations) + "\n----")
            else:
                adoc_content.append(
                    "\n==== Per-Table Vacuum Memory Analysis\n"
                    "[NOTE]\n====\nThe high-risk tables identified above have a critical transaction ID age but do not currently have a high number of dead tuples. "
                    "This means that while a `VACUUM FREEZE` is still urgently required to prevent wraparound, a specific `autovacuum_work_mem` tuning recommendation is not needed for these tables at this time.\n====\n"
                )
                
        except Exception as e:
            adoc_content.append(f"\n[ERROR]\n====\nCould not perform per-table vacuum analysis: {e}\n====\n")
            
    # --- Relevant Settings ---
    try:
        adoc_content.append("\n==== Relevant Configuration Settings")
        query = get_vacuum_age_settings_query(connector)
        formatted, raw = connector.execute_query(query, return_raw=True)
        adoc_content.append(formatted)
        structured_data["vacuum_age_settings"] = {"status": "success", "data": raw}
    except Exception as e:
        adoc_content.append(f"\n[ERROR]\n====\nCould not retrieve vacuum age settings: {e}\n====\n")

    tip_content = ["\n[TIP]\n====\n"]
    
    # MODIFIED: Formatted as a list and added the new advice
    tip_content.append("* **Immediate Action**: If a database is nearing the wraparound limit, you must run `VACUUM` on high-risk tables to freeze old transaction IDs.")
    tip_content.append("\n\n* **Long-Term Prevention**: Consider increasing `autovacuum_freeze_max_age` to a higher value, such as `2,000,000,000`. This provides a much larger safety buffer and gives autovacuum more time to work before the situation becomes critical.")

    if settings.get('is_aurora', False):
        aurora_tip = (
            "\n\n* **For AWS Aurora/RDS Users**: Aurora's `rds.adaptive_autovacuum` feature helps prevent wraparound by making vacuums more aggressive. "
            "You may observe in CloudWatch that the `MaximumUsedTransactionIDs` metric spikes and then drops sharply; this is the adaptive feature working. "
            "While this prevents a shutdown, it's a sign that your baseline autovacuum settings need tuning to avoid these performance-impacting events."
        )
        tip_content.append(aurora_tip)
    
    work_mem_advice = (f"\n\n* **Performance Tuning**: Ensure `autovacuum_work_mem` is sufficient for your largest tables. If too low, autovacuum must scan a table's indexes multiple times to clean all dead tuples. "
                       f"For PostgreSQL versions 16 and prior, its effective maximum is 1GB.")
    tip_content.append(work_mem_advice)
    
    tip_content.append("\n====\n")
    adoc_content.append("".join(tip_content))

    return "\n".join(adoc_content), structured_data
