from plugins.postgres.utils.postgresql_version_compatibility import get_postgresql_version, get_blocking_query

def run_current_lock_waits(connector, settings):
    """
    Identifies any sessions that are currently blocked, waiting to acquire a lock, which can indicate contention issues.
    """
    adoc_content = ["=== Current Session Lock Waits", "Shows active sessions that are currently waiting for a lock to be released by another session. Persistent lock waits are a sign of transaction contention.\n"]
    structured_data = {}

    try:
        # This query identifies sessions that are blocked by other sessions.
        blocking_query = get_blocking_query(connector.cursor, connector.execute_query)
        
        if settings.get('show_qry') == 'true':
            adoc_content.append("Current lock waits query:")
            adoc_content.append("[,sql]\n----")
            adoc_content.append(blocking_query)
            adoc_content.append("----")

        formatted_result, raw_result = connector.execute_query(blocking_query, return_raw=True)

        if "[ERROR]" in formatted_result:
            adoc_content.append(formatted_result)
            structured_data["lock_waits"] = {"status": "error", "details": raw_result}
        elif not raw_result:
            adoc_content.append("[NOTE]\n====\nNo sessions are currently waiting for locks. This is a healthy state.\n====\n")
            structured_data["lock_waits"] = {"status": "success", "data": []}
        else:
            adoc_content.append("[IMPORTANT]\n====\nLock contention detected! The following sessions are blocked. Investigate the 'blocking_query' to understand the source of the lock.\n====\n")
            adoc_content.append(formatted_result)
            structured_data["lock_waits"] = {"status": "success", "data": raw_result}
            
    except Exception as e:
        error_msg = f"Failed during lock wait analysis: {e}"
        adoc_content.append(f"[ERROR]\n====\n{error_msg}\n====\n")
        structured_data["lock_waits"] = {"status": "error", "details": error_msg}

    adoc_content.append("\n[TIP]\n====\nLock waits occur when one transaction tries to access a resource (like a table row) that another active transaction has locked. To resolve, you can either wait for the blocking transaction to complete or, if necessary, terminate the blocking session using `SELECT pg_terminate_backend(blocking_pid);`. Long-term solutions involve optimizing transaction logic, improving indexing, and ensuring short transaction durations.\n====\n")
    
    return "\n".join(adoc_content), structured_data
