def get_physical_replication_query(connector):
    """
    Returns a version-aware query to check physical replication status.
    This relies on the connector's pre-fetched version information.
    """
    # This check correctly uses the pre-fetched version info from the connector
    if connector.version_info.get('is_pg10_or_newer'):
        return """
            SELECT usename, application_name, client_addr, state, sync_state,
                   pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn) AS sent_lag_bytes,
                   write_lag, flush_lag, replay_lag
            FROM pg_stat_replication;
        """
    else:
        # Fallback for legacy versions older than 10
        return """
            SELECT usename, application_name, client_addr, state, sync_state,
                   pg_xlog_location_diff(pg_current_xlog_location(), sent_location) AS sent_lag_bytes
            FROM pg_stat_replication;
        """

def get_replication_slots_query(connector):
    """
    Returns a version-aware query to check all replication slots.
    """
    # This check also correctly uses the connector's version info
    if connector.version_info.get('is_pg10_or_newer'):
        lsn_diff_func = 'pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)'
    else:
        lsn_diff_func = 'pg_xlog_location_diff(pg_current_xlog_location(), restart_lsn)'

    return f"""
        SELECT
            slot_name, plugin, slot_type, database, active,
            pg_size_pretty({lsn_diff_func}) AS replication_lag_size,
            wal_status, safe_wal_size
        FROM pg_replication_slots;
    """

def get_subscription_stats_query(connector):
    """
    Returns a query for logical replication subscription stats.
    """
    if connector.version_info.get('is_pg10_or_newer'):
        return "SELECT subname, received_lsn, last_msg_send_time, last_msg_receipt_time FROM pg_stat_subscription;"
    else:
        return None
