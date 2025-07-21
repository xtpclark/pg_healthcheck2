from modules.postgresql_version_compatibility import get_postgresql_version, validate_postgresql_version
import decimal

def _safe_int(val):
    if val is None:
        return 0
    if isinstance(val, (int, float)):
        return int(val)
    if isinstance(val, decimal.Decimal):
        return int(val)
    try:
        return int(str(val).replace(',', ''))
    except Exception:
        return 0

def run_inactive_replication_slots(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Detects inactive replication slots that may be consuming disk space and provides recommendations for cleanup.
    Analyzes both physical and logical slots, reporting relevant details and highlighting those with high retained WAL.
    Version-aware: Handles pre-9.4 (not supported), and LSN diff function changes in PG10+.
    """
    adoc_content = ["=== Inactive Replication Slots", "Detects inactive replication slots that may be consuming disk space.\n"]
    structured_data = {}

    # Get PostgreSQL version compatibility information
    compatibility = get_postgresql_version(cursor, execute_query)
    is_supported, error_msg = validate_postgresql_version(compatibility)

    # Replication slots introduced in PG 9.4 (version_num >= 90400)
    if compatibility.get('version_num', 0) < 90400:
        adoc_content.append("[NOTE]\n====\nReplication slots are not supported in PostgreSQL versions prior to 9.4.\n====\n")
        structured_data["inactive_replication_slots"] = {"status": "not_supported", "reason": "Replication slots not available in this PostgreSQL version."}
        return "\n".join(adoc_content), structured_data

    # Use correct LSN diff function for version
    if compatibility.get('version_num', 0) >= 100000:
        lsn_diff_func = 'pg_wal_lsn_diff'
    else:
        lsn_diff_func = 'pg_xlog_location_diff'

    # Show queries if requested
    if settings.get('show_qry') == 'true':
        adoc_content.append("Replication slot analysis queries:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append(f"""
            SELECT slot_name, plugin, slot_type, database, active, restart_lsn, {lsn_diff_func}(pg_current_wal_lsn(), restart_lsn) AS retained_bytes
            FROM pg_replication_slots
            WHERE active = false;
        """)
        adoc_content.append("----")

    # Main query for inactive slots
    query = (
        f"SELECT slot_name, plugin, slot_type, database, active, restart_lsn, "
        f"{lsn_diff_func}(pg_current_wal_lsn(), restart_lsn) AS retained_bytes "
        "FROM pg_replication_slots WHERE active = false;"
    )

    try:
        formatted_result, raw_result = execute_query(query, return_raw=True)
    except Exception as e:
        adoc_content.append(f"[ERROR]\n====\nError querying replication slots: {e}\n====\n")
        structured_data["inactive_replication_slots"] = {"status": "error", "details": str(e)}
        return "\n".join(adoc_content), structured_data

    if "[ERROR]" in formatted_result:
        adoc_content.append(f"Inactive Replication Slots\n{formatted_result}")
        structured_data["inactive_replication_slots"] = {"status": "error", "details": raw_result}
    else:
        adoc_content.append("Inactive Replication Slots")
        adoc_content.append(formatted_result)
        structured_data["inactive_replication_slots"] = {"status": "success", "data": raw_result}

        # Highlight slots with high retained_bytes (e.g., > 1GB)
        high_retention = []
        for row in raw_result:
            retained = _safe_int(row.get('retained_bytes', 0))
            if retained > 1_000_000_000:  # 1GB
                high_retention.append(row)
        if high_retention:
            adoc_content.append("[WARNING]\n====\nThe following inactive slots are retaining more than 1GB of WAL. This can cause disk bloat and prevent WAL recycling. Consider dropping these slots if they are no longer needed.\n====\n")
            for row in high_retention:
                adoc_content.append(f"* Slot: `{row.get('slot_name')}` | Type: {row.get('slot_type')} | Retained: {row.get('retained_bytes')} bytes")

    adoc_content.append("[TIP]\n====\nInactive replication slots can prevent PostgreSQL from recycling old WAL files, leading to disk space issues. Regularly review and drop unused slots with:\n\n    SELECT pg_drop_replication_slot('slot_name');\n\nOnly drop slots you are certain are not needed by any replica or logical consumer.\n====\n")

    if settings.get('is_aurora') == 'true':
        adoc_content.append("[NOTE]\n====\nAurora manages replication slots differently. Manual slot management may not be required.\n====\n")

    return "\n".join(adoc_content), structured_data 