from plugins.postgres.utils.qrylib.autovacuum_config import (
    get_global_autovacuum_settings_query,
    get_autovacuum_overrides_query
)

def get_weight():
    """Returns the importance score for this module."""
    return 10 # Core configuration, highest importance


def run_autovacuum_config(connector, settings):
    """
    Analyzes and presents global autovacuum settings and highlights any tables
    with custom, overriding configurations.
    """
    adoc_content = ["=== Autovacuum Configuration Analysis", "Analyzes autovacuum settings to ensure efficient bloat management, highlighting tables with custom configurations that override global defaults.\n"]
    structured_data = {}

    try:
        # --- Global Settings ---
        adoc_content.append("==== Global Autovacuum Settings")
        global_settings_query = get_global_autovacuum_settings_query()
        global_formatted, global_raw = connector.execute_query(global_settings_query, return_raw=True)

        if "[ERROR]" in global_formatted:
            raise Exception("Could not retrieve global autovacuum settings.")
        
        adoc_content.append(global_formatted.replace('\\n', '\n'))
        structured_data["global_autovacuum_settings"] = {"status": "success", "data": global_raw}

        # --- Tables with Custom Overrides ---
        adoc_content.append("\n==== Tables with Custom Autovacuum Settings")
        overrides_query = get_autovacuum_overrides_query()
        overrides_formatted, overrides_raw = connector.execute_query(overrides_query, return_raw=True)
        
        if "[ERROR]" in overrides_formatted:
             raise Exception("Could not retrieve per-table autovacuum overrides.")
        
        if not overrides_raw:
            adoc_content.append("[NOTE]\n====\nNo tables found with custom autovacuum settings. All tables are using the global defaults.\n====\n")
        else:
            adoc_content.append("[IMPORTANT]\n====\nThe following tables have custom settings that override the global defaults. Review these overrides to ensure they are intentional and appropriate for the table's workload.\n====\n")
            adoc_content.append(overrides_formatted.replace('\\n', '\n'))
        
        structured_data["custom_autovacuum_overrides"] = {"status": "success", "data": overrides_raw}

    except Exception as e:
        error_msg = f"Failed during autovacuum analysis: {e}"
        adoc_content.append(f"\n[ERROR]\n====\n{error_msg}\n====\n")
        structured_data["autovacuum_analysis"] = {"status": "error", "details": str(e)}
        return "\n".join(adoc_content), structured_data
    
    adoc_content.append("\n[TIP]\n====\nAutovacuum is critical for database health. For very large or high-traffic tables, tuning per-table settings can be beneficial, but misconfigurations can lead to severe bloat. Always verify that custom settings are intentional.\n====\n")

    return "\n".join(adoc_content), structured_data
