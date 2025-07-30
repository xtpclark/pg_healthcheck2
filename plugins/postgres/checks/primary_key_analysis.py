from plugins.postgres.utils.postgresql_version_compatibility import (
    get_pk_exhaustion_summary_query,
    get_pk_exhaustion_details_query
)

def run_primary_key_analysis(connector, settings):
    """
    Analyzes integer-based primary keys for exhaustion risks and provides a summary.
    """
    adoc_content = ["=== Primary Key Exhaustion Risk Analysis", "Identifies `integer` and `smallint` primary keys that are approaching their maximum value, which can cause application outages.\n"]
    structured_data = {}

    # --- 1. Get the High-Level Summary for AI Analysis ---
    try:
        summary_query = get_pk_exhaustion_summary_query(connector)
        _, summary_raw = connector.execute_query(summary_query, return_raw=True)
        structured_data["primary_key_summary"] = {"status": "success", "data": summary_raw[0] if summary_raw else {}}
    except Exception as e:
        adoc_content.append(f"\n[ERROR]\n====\nCould not retrieve primary key summary: {e}\n====\n")
        structured_data["primary_key_summary"] = {"status": "error", "details": str(e)}

    # --- 2. Get Detailed List of High-Risk Primary Keys ---
    try:
        adoc_content.append("==== High-Risk Primary Keys (Nearing Exhaustion)")
        details_query = get_pk_exhaustion_details_query(connector)
        formatted, raw = connector.execute_query(details_query, return_raw=True)

        if "[ERROR]" in formatted:
            adoc_content.append(formatted)
            structured_data["high_risk_pks_details"] = {"status": "error", "data": raw}
        elif not raw:
            adoc_content.append("[NOTE]\n====\nNo integer-based primary keys found to be more than 80% exhausted. This is a healthy sign.\n====\n")
            structured_data["high_risk_pks_details"] = {"status": "success", "data": []}
        else:
            adoc_content.append("[CRITICAL]\n====\n**Action Required:** The following primary keys are nearing their maximum value. Once the limit is reached, `INSERT` operations will fail, causing an application outage. Plan to migrate these to `bigint` immediately.\n====\n")
            adoc_content.append(formatted)
            structured_data["high_risk_pks_details"] = {"status": "success", "data": raw}

    except Exception as e:
        adoc_content.append(f"\n[ERROR]\n====\nCould not analyze high-risk primary keys: {e}\n====\n")

    # --- 3. Add General Recommendations ---
    adoc_content.append("\n==== Recommendations")
    adoc_content.append("[TIP]\n====\n"
                        "* **Best Practice**: Always use `bigint` (or `bigserial`) for new primary keys on tables expected to have significant growth.\n"
                        "* **Migration**: Migrating a primary key from `integer` to `bigint` is a complex, online operation that requires careful planning to avoid downtime. Use a proven method like a multi-step migration with triggers or a logical replication-based approach.\n"
                        "* **Monitoring**: Proactively monitor the `percentage_used` for all integer-based primary keys to avoid future emergencies.\n"
                        "====\n")

    return "\n".join(adoc_content), structured_data
