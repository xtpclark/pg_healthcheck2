def run_autovacuum_config(connector, settings):
    """
    Analyzes and presents effective autovacuum settings for each table,
    highlighting overrides from the global defaults.
    """
    adoc_content = ["=== Autovacuum Configuration Analysis", "Analyzes autovacuum settings to ensure efficient bloat management, highlighting tables with custom configurations that override global defaults.\n"]
    structured_data = {}

    # Query to get global autovacuum defaults
    global_settings_query = "SELECT name, setting FROM pg_settings WHERE name LIKE 'autovacuum_%';"
    
    # A more advanced query to get effective settings per table and compare them to the defaults
    effective_settings_query = """
    WITH global_settings AS (
        SELECT name, setting FROM pg_settings WHERE name LIKE 'autovacuum_%'
    )
    SELECT
        n.nspname AS schema_name,
        c.relname AS table_name,
        'autovacuum_enabled' AS parameter,
        (SELECT setting FROM global_settings WHERE name = 'autovacuum') AS global_default,
        coalesce(substring(t.reloptions::text from 'autovacuum_enabled=([^,]+)'), (SELECT setting FROM global_settings WHERE name = 'autovacuum')) AS effective_value
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_options_to_table(c.reloptions) t ON t.option_name = 'autovacuum_enabled'
    WHERE c.relkind IN ('r', 'm') AND n.nspname NOT IN ('pg_catalog', 'information_schema')
    AND coalesce(substring(t.reloptions::text from 'autovacuum_enabled=([^,]+)'), (SELECT setting FROM global_settings WHERE name = 'autovacuum')) != (SELECT setting FROM global_settings WHERE name = 'autovacuum')
    
    UNION ALL

    SELECT
        n.nspname AS schema_name,
        c.relname AS table_name,
        'autovacuum_vacuum_threshold' AS parameter,
        (SELECT setting FROM global_settings WHERE name = 'autovacuum_vacuum_threshold') AS global_default,
        coalesce(substring(t.reloptions::text from 'autovacuum_vacuum_threshold=([^,]+)'), (SELECT setting FROM global_settings WHERE name = 'autovacuum_vacuum_threshold')) AS effective_value
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_options_to_table(c.reloptions) t ON t.option_name = 'autovacuum_vacuum_threshold'
    WHERE c.relkind IN ('r', 'm') AND n.nspname NOT IN ('pg_catalog', 'information_schema')
    AND coalesce(substring(t.reloptions::text from 'autovacuum_vacuum_threshold=([^,]+)'), (SELECT setting FROM global_settings WHERE name = 'autovacuum_vacuum_threshold')) != (SELECT setting FROM global_settings WHERE name = 'autovacuum_vacuum_threshold')
    
    ORDER BY schema_name, table_name, parameter
    LIMIT %(limit)s;
    """

    try:
        # --- Global Settings ---
        adoc_content.append("==== Global Autovacuum Settings")
        global_formatted, global_raw = connector.execute_query(global_settings_query, return_raw=True)
        if "[ERROR]" in global_formatted:
            raise Exception("Could not retrieve global autovacuum settings.")
        adoc_content.append(global_formatted)
        structured_data["global_autovacuum_settings"] = {"status": "success", "data": global_raw}

        # --- Tables with Custom Overrides ---
        adoc_content.append("\n==== Tables with Custom Autovacuum Settings")
        params = {'limit': settings.get('row_limit', 10)}
        overrides_formatted, overrides_raw = connector.execute_query(effective_settings_query, params=params, return_raw=True)
        
        if "[ERROR]" in overrides_formatted:
             raise Exception("Could not retrieve per-table autovacuum overrides.")
        
        if not overrides_raw:
            adoc_content.append("[NOTE]\n====\nNo tables found with custom autovacuum settings that override the global defaults.\n====\n")
        else:
            adoc_content.append("[IMPORTANT]\n====\nThe following tables have custom settings that differ from the global defaults. Review these overrides to ensure they are intentional and appropriate for the table's workload.\n====\n")
            adoc_content.append(overrides_formatted)
        
        structured_data["custom_autovacuum_overrides"] = {"status": "success", "data": overrides_raw}

    except Exception as e:
        error_msg = f"Failed during autovacuum analysis: {e}"
        adoc_content.append(f"[ERROR]\n====\n{error_msg}\n====\n")
        # Ensure structured data reflects the error
        if "global_autovacuum_settings" not in structured_data:
            structured_data["global_autovacuum_settings"] = {"status": "error", "details": str(e)}
        if "custom_autovacuum_overrides" not in structured_data:
            structured_data["custom_autovacuum_overrides"] = {"status": "error", "details": str(e)}
        return "\n".join(adoc_content), structured_data
    
    adoc_content.append("\n[TIP]\n====\nAutovacuum is critical for database health. Ensure it is enabled globally. For very large or high-traffic tables, tuning per-table settings (like `autovacuum_vacuum_scale_factor` = 0.05) can be beneficial, but misconfigurations can lead to severe bloat. Always verify that custom settings are intentional.\n====\n")

    return "\n".join(adoc_content), structured_data
