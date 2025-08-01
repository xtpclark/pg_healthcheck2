from plugins.postgres.utils.qrylib.transaction_wraparound import (
    get_database_wraparound_query,
    get_table_wraparound_query,
    get_vacuum_age_settings_query
)

def get_weight():
    """Returns the importance score for this module."""
    # Transaction wraparound is a critical, service-impacting event.
    return 10

def run_transaction_wraparound(connector, settings):
    """
    Analyzes databases and tables to determine how close they are to a
    transaction ID wraparound failure.
    """
    adoc_content = ["=== Transaction ID Wraparound Analysis", "Monitors the age of the oldest transaction ID. If this age reaches the `autovacuum_freeze_max_age` limit, the database will shut down to prevent data corruption.\n"]
    structured_data = {}
    params = {'limit': settings.get('row_limit', 10)}

    # --- Database-Level Analysis ---
    try:
        adoc_content.append("==== Wraparound Risk by Database")
        query = get_database_wraparound_query(connector)
        formatted, raw = connector.execute_query(query, return_raw=True)
        adoc_content.append(formatted)
        structured_data["database_wraparound_risk"] = {"status": "success", "data": raw}
    except Exception as e:
        adoc_content.append(f"\n[ERROR]\n====\nCould not analyze database-level wraparound risk: {e}\n====\n")

    # --- Table-Level Analysis ---
    try:
        adoc_content.append("\n==== Top Tables Contributing to Wraparound Risk")
        query = get_table_wraparound_query(connector)
        formatted, raw = connector.execute_query(query, params=params, return_raw=True)
        adoc_content.append(formatted)
        structured_data["table_wraparound_risk"] = {"status": "success", "data": raw}
    except Exception as e:
        adoc_content.append(f"\n[ERROR]\n====\nCould not analyze table-level wraparound risk: {e}\n====\n")

    # --- Relevant Settings ---
    try:
        adoc_content.append("\n==== Relevant Configuration Settings")
        query = get_vacuum_age_settings_query(connector)
        formatted, raw = connector.execute_query(query, return_raw=True)
        adoc_content.append(formatted)
        structured_data["vacuum_age_settings"] = {"status": "success", "data": raw}
    except Exception as e:
        adoc_content.append(f"\n[ERROR]\n====\nCould not retrieve vacuum age settings: {e}\n====\n")

    adoc_content.append("\n[TIP]\n====\nIf a database is nearing the wraparound limit, you must run `VACUUM` on the specific tables identified as high-risk to freeze old transaction IDs. Ensure autovacuum is running and properly configured to prevent this from becoming an emergency.\n====\n")

    return "\n".join(adoc_content), structured_data
