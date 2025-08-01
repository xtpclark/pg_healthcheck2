from plugins.postgres.utils.qrylib.connection_metrics import (
    get_total_connections_query,
    get_connection_states_query,
    get_connections_by_user_db_query
)

def get_weight():
    """Returns the importance score for this module."""
    # Connection management is a core operational concern.
    return 7

def run_connection_metrics(connector, settings):
    """
    Analyzes PostgreSQL connection metrics to monitor database load and identify
    potential connection-related issues.
    """
    adoc_content = ["=== Connection Metrics Analysis", "Analyzes database connection statistics to monitor load and identify potential bottlenecks.\n"]
    structured_data = {}

    # --- Total Connections vs. Limit ---
    try:
        adoc_content.append("==== Total Connections vs. Max Limit")
        query = get_total_connections_query()
        formatted, raw = connector.execute_query(query, return_raw=True)
        adoc_content.append(formatted)
        structured_data["total_connections_and_limits"] = {"status": "success", "data": raw}
    except Exception as e:
        adoc_content.append(f"\n[ERROR]\n====\nCould not analyze total connections: {e}\n====\n")

    # --- Connection States ---
    try:
        adoc_content.append("\n==== Connection States")
        query = get_connection_states_query()
        formatted, raw = connector.execute_query(query, return_raw=True)
        adoc_content.append(formatted)
        structured_data["connection_states"] = {"status": "success", "data": raw}
    except Exception as e:
        adoc_content.append(f"\n[ERROR]\n====\nCould not analyze connection states: {e}\n====\n")

    # --- Connections by User and Database ---
    try:
        adoc_content.append("\n==== Top Connections by User and Database")
        query = get_connections_by_user_db_query()
        params = {'limit': settings.get('row_limit', 10)}
        formatted, raw = connector.execute_query(query, params=params, return_raw=True)
        adoc_content.append(formatted)
        structured_data["connections_by_user_database"] = {"status": "success", "data": raw}
    except Exception as e:
        adoc_content.append(f"\n[ERROR]\n====\nCould not analyze connections by user/database: {e}\n====\n")

    adoc_content.append("\n[TIP]\n====\nMonitor total connections to prevent exhaustion. A high number of `idle in transaction` connections often indicates application-level problems where transactions are not being committed or rolled back properly. Consider using a connection pooler like PgBouncer for applications with many short-lived connections.\n====\n")

    return "\n".join(adoc_content), structured_data
