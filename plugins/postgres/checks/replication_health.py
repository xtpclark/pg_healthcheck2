from plugins.postgres.utils.postgresql_version_compatibility import (
    get_physical_replication_query,
    get_replication_slots_query,
    get_subscription_stats_query
)

def run_replication_health(connector, settings):
    """
    Performs a comprehensive check of physical and logical replication,
    and the health of replication slots, using the central connector for version detection.
    """
    adoc_content = ["=== Replication Health Analysis", "Provides a consolidated view of replication status and potential issues like inactive slots.\n"]
    structured_data = {}

    # --- 1. Physical Replication Status ---
    try:
        adoc_content.append("==== Physical Replication (Streaming)")
        # This now correctly gets the version-aware query from the compatibility module
        query = get_physical_replication_query(connector)
        formatted, raw = connector.execute_query(query, return_raw=True)

        if "[ERROR]" in formatted:
            adoc_content.append(formatted)
        elif not raw:
            adoc_content.append("[NOTE]\n====\nNo active physical replication standbys are connected.\n====\n")
        else:
            adoc_content.append("[IMPORTANT]\n====\nReview standby status and lag. Significant lag can indicate network or performance issues.\n====\n")
            adoc_content.append(formatted)
        structured_data["physical_replication_status"] = {"status": "success", "data": raw}
    except Exception as e:
        adoc_content.append(f"\n[ERROR]\n====\nCould not analyze physical replication: {e}\n====\n")

    # --- 2. Replication Slot Health ---
    try:
        adoc_content.append("\n==== Replication Slot Health")
        query = get_replication_slots_query(connector)
        formatted, raw_slots = connector.execute_query(query, return_raw=True)

        if not isinstance(raw_slots, list):
            if "[ERROR]" in formatted:
                adoc_content.append(formatted)
            raw_slots = []

        if not raw_slots and "[ERROR]" not in formatted:
            adoc_content.append("[NOTE]\n====\nNo replication slots found.\n====\n")
        elif "[ERROR]" not in formatted:
            adoc_content.append(formatted)

        inactive_slots = [s for s in raw_slots if isinstance(s, dict) and not s.get('active')]
        summary = {"total_slots": len(raw_slots), "inactive_slots_count": len(inactive_slots)}
        structured_data["replication_slots_summary"] = {"status": "success", "data": summary}
        structured_data["replication_slots_details"] = {"status": "success", "data": raw_slots}

        if inactive_slots:
            adoc_content.append("\n[CRITICAL]\n====\n**Action Required!** Inactive replication slots were found. These prevent the primary from removing old WAL files and will eventually fill the disk, causing an outage. Drop any unused slots immediately.\n====\n")

    except Exception as e:
        adoc_content.append(f"\n[ERROR]\n====\nCould not analyze replication slots: {e}\n====\n")

    # --- 3. Logical Replication Subscription Status ---
    try:
        query = get_subscription_stats_query(connector)
        if query:
            adoc_content.append("\n==== Logical Subscription Status")
            formatted, raw = connector.execute_query(query, return_raw=True)
            if "[ERROR]" in formatted:
                adoc_content.append(formatted)
            elif not raw:
                adoc_content.append("[NOTE]\n====\nNo logical replication subscriptions found.\n====\n")
            else:
                adoc_content.append(formatted)
            structured_data["logical_subscription_status"] = {"status": "success", "data": raw}
    except Exception as e:
        adoc_content.append(f"\n[ERROR]\n====\nCould not analyze logical subscriptions: {e}\n====\n")

    return "\n".join(adoc_content), structured_data
