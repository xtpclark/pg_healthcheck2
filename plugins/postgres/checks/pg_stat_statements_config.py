
def get_weight():
    """Returns the importance score for this module."""
    return 10 # Core configuration, highest importance

def run_pg_stat_statements_config(connector, settings):
    """
    Checks if pg_stat_statements is enabled and properly configured for query analysis.
    """
    adoc_content = ["=== pg_stat_statements Configuration Status", "Analyzes the configuration of the `pg_stat_statements` extension, which is critical for query performance monitoring.\n"]
    structured_data = {}

    # --- Data Collection ---
    try:
        # Get all relevant settings in one query
        settings_query = "SELECT name, setting FROM pg_settings WHERE name IN ('shared_preload_libraries', 'pg_stat_statements.track');"
        _, raw_settings = connector.execute_query(settings_query, return_raw=True)
        settings_map = {s['name']: s['setting'] for s in raw_settings}

        # Check if the extension is created in the database
        extension_query = "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements');"
        _, raw_ext_exists = connector.execute_query(extension_query, is_check=True, return_raw=True)
        
        is_preloaded = 'pg_stat_statements' in settings_map.get('shared_preload_libraries', '')
        is_extension_created = (str(raw_ext_exists).lower() == 't' or str(raw_ext_exists).lower() == 'true')
        tracking_level = settings_map.get('pg_stat_statements.track', 'none')
        is_tracking_enabled = tracking_level != 'none'

        structured_data["config_status"] = {
            "is_preloaded": is_preloaded,
            "is_extension_created": is_extension_created,
            "tracking_level": tracking_level,
            "is_tracking_enabled": is_tracking_enabled
        }

    except Exception as e:
        adoc_content.append(f"[ERROR]\n====\nFailed to check pg_stat_statements configuration: {e}\n====\n")
        structured_data["config_status"] = {"status": "error", "details": str(e)}
        return "\n".join(adoc_content), structured_data

    # --- Analysis and Reporting ---
    status_table = [
        "[cols=\"1,1,3\",options=\"header\"]",
        "|===",
        "| Check | Status | Details",
        f"| In `shared_preload_libraries` | {'✅ OK' if is_preloaded else '❌ FAILED'} | Required for the extension to load at startup.",
        f"| Extension Created in Database | {'✅ OK' if is_extension_created else '❌ FAILED'} | The `CREATE EXTENSION` command must be run.",
        f"| Query Tracking Enabled | {'✅ OK' if is_tracking_enabled else '❌ FAILED'} | `pg_stat_statements.track` is set to `{tracking_level}`. Should be `top` or `all`.",
        "|==="
    ]
    adoc_content.append("\n".join(status_table))

    # --- Tailored Recommendations ---
    if is_preloaded and is_extension_created and is_tracking_enabled:
        adoc_content.append("\n[NOTE]\n====\nThe `pg_stat_statements` extension is **fully enabled** and configured correctly for query monitoring.\n====\n")
    else:
        adoc_content.append("\n[IMPORTANT]\n====\n**Action Required:** `pg_stat_statements` is not fully functional. Please follow these steps:\n")
        if not is_preloaded:
            adoc_content.append("* **Step 1: Add to `shared_preload_libraries`**. In your `postgresql.conf` or RDS/Aurora parameter group, set `shared_preload_libraries = 'pg_stat_statements'`. **A database restart is required for this change.**")
        if not is_extension_created:
            adoc_content.append(f"* **Step 2: Create the Extension**. Connect to the `{settings.get('database')}` database and run the command: `CREATE EXTENSION pg_stat_statements;`")
        if not is_tracking_enabled:
            adoc_content.append(f"* **Step 3: Enable Tracking**. The tracking level is currently `{tracking_level}`. Run the command: `ALTER SYSTEM SET pg_stat_statements.track = 'top';` followed by `SELECT pg_reload_conf();` to start tracking queries.")
        adoc_content.append("====\n")
        
    adoc_content.append("[TIP]\n====\nProper configuration of `pg_stat_statements` is vital for capturing comprehensive query metrics. Regularly reset statistics (`SELECT pg_stat_statements_reset();`) before performance tests for focused analysis periods.\n====\n")

    return "\n".join(adoc_content), structured_data
