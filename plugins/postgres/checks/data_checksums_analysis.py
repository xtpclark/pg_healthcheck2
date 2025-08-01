from plugins.postgres.utils.qrylib.data_checksums import get_data_checksums_query

def get_weight():
    """Returns the importance score for this module."""
    # Data integrity is critical.
    return 8

def run_data_checksums(connector, settings):
    """
    Analyzes the data_checksums configuration to ensure data integrity.
    """
    adoc_content = ["=== Data Checksums Analysis", "Analyzes the `data_checksums` configuration, which is critical for protecting against silent data corruption."]
    structured_data = {}
    data_key = "data_checksums_status"

    try:
        query = get_data_checksums_query()
        formatted, raw = connector.execute_query(query, return_raw=True)

        if "[ERROR]" in formatted:
            adoc_content.append(formatted)
            structured_data[data_key] = {"status": "error", "data": raw}
        elif not raw:
            adoc_content.append("[NOTE]\n====\nCould not determine data_checksums status.\n====\n")
            structured_data[data_key] = {"status": "error", "data": "Query returned no data"}
        else:
            checksums_enabled = raw[0].get('setting') == 'on'
            structured_data[data_key] = {"status": "success", "data": {"checksums_enabled": checksums_enabled}}

            if checksums_enabled:
                adoc_content.append("\n==== Data Checksums: ✅ ENABLED")
                adoc_content.append("Data checksums are correctly enabled, providing a crucial layer of protection against silent data corruption on disk.")
            else:
                adoc_content.append("\n==== Data Checksums: ❌ DISABLED")
                adoc_content.append("[CRITICAL]\n====\nData checksums are disabled. This creates a significant risk of undetected data corruption, which can lead to permanent data loss, incorrect query results, and failed recovery operations. It is strongly recommended to enable this feature.\n====\n")

            # Add the raw output table for reference
            adoc_content.append(formatted)

    except Exception as e:
        error_msg = f"Could not analyze data_checksums status: {e}"
        adoc_content.append(f"[ERROR]\n====\n{error_msg}\n====\n")
        structured_data[data_key] = {"status": "error", "error": str(e)}
    
    tip = (
        "**Best Practice**: Always enable `data_checksums` on production databases. "
        "This feature must be enabled when the cluster is initialized with `initdb`. "
        "For existing clusters, you can use the `pg_checksums` utility, but this requires a clean shutdown and downtime."
    )
    
    if settings.get('is_aurora', False):
        tip += (
            "\n\n**Note for AWS Aurora**: While Aurora manages storage-level integrity, "
            "enabling PostgreSQL-level checksums provides an additional, application-aware layer of validation."
        )
        
    adoc_content.append(f"\n[TIP]\n====\n{tip}\n====\n")

    return "\n".join(adoc_content), structured_data
