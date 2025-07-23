from plugins.postgres.utils.postgresql_version_compatibility import get_postgresql_version

def run_replication_health(connector, settings):
    """
    Performs a comprehensive check of physical and logical replication,
    and the health of replication slots, adapting to different PostgreSQL versions.
    """
    adoc_content = ["=== Replication Health Analysis", "Provides a consolidated view of physical and logical replication status, along with a critical check for inactive replication slots that can cause disk space issues.\n"]
    structured_data = {}

    # Get PostgreSQL version to select the correct queries
    try:
        version_info = get_postgresql_version(connector.cursor, connector.execute_query)
        pg_major_version = version_info.get('major_version_number', 0)
    except Exception as e:
        adoc_content.append(f"[ERROR]\n====\nCould not determine PostgreSQL version for replication checks: {e}\n====\n")
        return "\n".join(adoc_content), {"status": "error", "details": str(e)}

    # --- 1. Physical Replication Status ---
    try:
        adoc_content.append("==== Physical Replication (Streaming)")
        
        # Select the correct query based on PostgreSQL version
        if pg_major_version >= 10:
            physical_rep_query = """
                SELECT
                    usename,
                    application_name,
                    client_addr,
                    state,
                    sync_state,
                    pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn) AS sent_lag_bytes,
                    pg_wal_lsn_diff(sent_lsn, write_lsn) AS write_lag_bytes,
                    pg_wal_lsn_diff(write_lsn, flush_lsn) AS flush_lag_bytes,
                    pg_wal_lsn_diff(flush_lsn, replay_lsn) AS replay_lag_bytes,
                    write_lag, -- Interval type for PG10+
                    flush_lag, -- Interval type for PG10+
                    replay_lag -- Interval type for PG10+
                FROM pg_stat_replication;
            """
        else:
            # Older versions do not have the interval-based lag columns
            physical_rep_query = """
                SELECT
                    usename,
                    application_name,
                    client_addr,
                    state,
                    sync_state,
                    pg_xlog_location_diff(pg_current_xlog_location(), sent_location) AS sent_lag_bytes,
                    pg_xlog_location_diff(sent_location, write_location) AS write_lag_bytes,
                    pg_xlog_location_diff(write_location, flush_location) AS flush_lag_bytes,
                    pg_xlog_location_diff(flush_location, replay_location) AS replay_lag_bytes
                FROM pg_stat_replication;
            """

        formatted, raw = connector.execute_query(physical_rep_query, return_raw=True)
        if "[ERROR]" in formatted:
            adoc_content.append(formatted)
        elif not raw:
            adoc_content.append("[NOTE]\n====\nNo active physical replication standbys are connected to this instance.\n====\n")
        else:
            adoc_content.append("[IMPORTANT]\n====\nReview the status and lag for each standby. Significant lag can indicate network issues or that the standby is struggling to keep up, increasing the risk of data loss in a failover.\n====\n")
            adoc_content.append(formatted)
        structured_data["physical_replication"] = {"status": "success", "data": raw}
    except Exception as e:
        adoc_content.append(f"[ERROR]\n====\nCould not analyze physical replication: {e}\n====\n")

    # --- 2. Logical Replication (Pub/Sub) Status ---
    # This feature is available from PG10+
    if pg_major_version >= 10:
        try:
            adoc_content.append("\n==== Logical Replication (Publication/Subscription)")
            # This query is compatible with PG10 and newer
            pub_sub_query = "SELECT subname, pubname, enabled, attname FROM pg_subscription s JOIN pg_publication_tables pt ON s.subname = pt.subname;"
            formatted, raw = connector.execute_query(pub_sub_query, return_raw=True)
            if "[ERROR]" in formatted:
                adoc_content.append(formatted)
            elif not raw:
                adoc_content.append("[NOTE]\n====\nNo logical replication subscriptions found on this instance.\n====\n")
            else:
                adoc_content.append(formatted)
            structured_data["logical_replication"] = {"status": "success", "data": raw}
        except Exception as e:
            adoc_content.append(f"[ERROR]\n====\nCould not analyze logical replication: {e}\n====\n")
    
    # --- 3. Replication Slot Health ---
    try:
        adoc_content.append("\n==== Replication Slot Health")
        # Select the correct query based on PostgreSQL version
        if pg_major_version >= 10:
            inactive_slots_query = """
                SELECT
                    slot_name,
                    plugin,
                    slot_type,
                    database,
                    active,
                    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS replication_lag_bytes
                FROM pg_replication_slots
                WHERE active = 'f';
            """
        else:
             inactive_slots_query = """
                SELECT
                    slot_name,
                    plugin,
                    slot_type,
                    database,
                    active,
                    pg_size_pretty(pg_xlog_location_diff(pg_current_xlog_location(), restart_lsn)) AS replication_lag_bytes
                FROM pg_replication_slots
                WHERE active = 'f';
            """
        formatted, raw = connector.execute_query(inactive_slots_query, return_raw=True)
        if "[ERROR]" in formatted:
            adoc_content.append(formatted)
        elif not raw:
            adoc_content.append("[NOTE]\n====\nNo inactive replication slots found. This is a healthy state.\n====\n")
        else:
            adoc_content.append("[CRITICAL]\n====\n**Action Required!** Inactive replication slots were found. These slots prevent the primary from removing old WAL files, which WILL eventually fill up the disk and cause a database outage. You must drop any slot that is no longer in use.\n====\n")
            adoc_content.append(formatted)
        structured_data["inactive_replication_slots"] = {"status": "success", "data": raw}
    except Exception as e:
        adoc_content.append(f"[ERROR]\n====\nCould not analyze replication slots: {e}\n====\n")

    adoc_content.append("\n[TIP]\n====\n* **To drop an unused slot**: Run `SELECT pg_drop_replication_slot('slot_name');`\n* **Monitoring is key**: Regularly monitor replication lag and the status of replication slots to prevent data loss and outages.\n====\n")

    return "\n".join(adoc_content), structured_data
