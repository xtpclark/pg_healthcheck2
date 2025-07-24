from plugins.postgres.utils.postgresql_version_compatibility import (
    get_unused_indexes_query,
    get_duplicate_indexes_query,
    get_invalid_indexes_query,
    get_specialized_indexes_summary_query,
    get_specialized_indexes_details_query
)

def run_index_health_analysis(connector, settings):
    """
    Performs a comprehensive analysis of index health, identifying unused,
    duplicate, invalid, and specialized indexes.
    """
    adoc_content = ["=== Index Health and Maintenance", "Provides a consolidated to-do list for index maintenance, identifying issues that can consume resources and slow down write operations.\n"]
    structured_data = {}
    params = {'limit': settings.get('row_limit', 10)}

    # --- Unused Indexes ---
    try:
        adoc_content.append("==== Unused Indexes")
        query = get_unused_indexes_query(connector)
        formatted, raw = connector.execute_query(query, params=params, return_raw=True)
        if "[ERROR]" in formatted:
            adoc_content.append(formatted)
        elif not raw:
            adoc_content.append("[NOTE]\n====\nNo significantly large and unused indexes found.\n====\n")
        else:
            adoc_content.append("[IMPORTANT]\n====\n**CRITICAL CONSIDERATION FOR READ REPLICAS:** Indexes that appear 'unused' on the primary may be heavily used on read replicas. **Verify usage on ALL replicas before dropping any index.**\n====\n")
            adoc_content.append(formatted)
        structured_data["unused_indexes"] = {"status": "success", "data": raw}
    except Exception as e:
        adoc_content.append(f"[ERROR]\n====\nCould not analyze unused indexes: {e}\n====\n")

    # --- Duplicate Indexes ---
    try:
        adoc_content.append("\n==== Duplicate Indexes")
        query = get_duplicate_indexes_query(connector)
        formatted, raw = connector.execute_query(query, params=params, return_raw=True)
        if "[ERROR]" in formatted:
            adoc_content.append(formatted)
        elif not raw:
            adoc_content.append("[NOTE]\n====\nNo duplicate indexes found.\n====\n")
        else:
            adoc_content.append("[IMPORTANT]\n====\nDuplicate indexes provide no benefit and double write overhead. Safely drop one index from each group listed.\n====\n")
            adoc_content.append(formatted)
        structured_data["duplicate_indexes"] = {"status": "success", "data": raw}
    except Exception as e:
        adoc_content.append(f"[ERROR]\n====\nCould not analyze duplicate indexes: {e}\n====\n")

    # --- Invalid Indexes ---
    try:
        adoc_content.append("\n==== Invalid Indexes")
        query = get_invalid_indexes_query(connector)
        formatted, raw = connector.execute_query(query, return_raw=True)
        if "[ERROR]" in formatted:
            adoc_content.append(formatted)
        elif not raw:
            adoc_content.append("[NOTE]\n====\nNo invalid indexes found.\n====\n")
        else:
            adoc_content.append("[IMPORTANT]\n====\nInvalid indexes are unusable and waste space. Rebuild them with `REINDEX INDEX CONCURRENTLY <index_name>;` to avoid locking.\n====\n")
            adoc_content.append(formatted)
        structured_data["invalid_indexes"] = {"status": "success", "data": raw}
    except Exception as e:
        adoc_content.append(f"[ERROR]\n====\nCould not analyze invalid indexes: {e}\n====\n")

    # --- Specialized Index Types Analysis ---
    try:
        adoc_content.append("\n==== Specialized Index Types")
        summary_query = get_specialized_indexes_summary_query(connector)
        _, summary_raw = connector.execute_query(summary_query, return_raw=True)
        summary_data = {item['index_type']: item['count'] for item in summary_raw} if isinstance(summary_raw, list) else {}
        structured_data["specialized_indexes_summary"] = {"status": "success", "data": summary_data}
        
        details_query = get_specialized_indexes_details_query(connector)
        formatted_result, raw_result = connector.execute_query(details_query, return_raw=True)

        if "[ERROR]" in formatted_result:
            adoc_content.append(formatted_result)
        elif not raw_result:
            adoc_content.append("[NOTE]\n====\nNo specialized (non-B-Tree) indexes found.\n====\n")
        else:
            adoc_content.append("[IMPORTANT]\n====\nSpecialized indexes are used in your database. Review the list to ensure they are appropriate for their workloads.\n====\n")
            adoc_content.append(formatted_result)
        structured_data["specialized_indexes_details"] = {"status": "success", "data": raw_result}
    except Exception as e:
        adoc_content.append(f"[ERROR]\n====\nCould not analyze specialized indexes: {e}\n====\n")

    return "\n".join(adoc_content), structured_data
