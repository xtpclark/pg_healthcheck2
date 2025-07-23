import re
# --- MODIFIED: Import the centralized compatibility module ---
from .postgresql_version_compatibility import get_postgresql_version

def run_suggested_config_values(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes key configuration values against best practices, using the centralized PostgreSQL compatibility module.
    """
    adoc_content = ["=== Configuration Analysis vs. Best Practices (Version-Aware)", "Analyzes current configuration settings against established PostgreSQL best practices, adjusted for your specific version, to identify areas for review and tuning.\n"]
    structured_data = {}

    # --- MODIFIED: Use the centralized function to get version info ---
    try:
        compatibility_info = get_postgresql_version(cursor, execute_query)
        pg_major_version = compatibility_info.get('major_version_number', 0)
        structured_data["postgres_version"] = pg_major_version
    except Exception as e:
        pg_major_version = 0 # Default if version detection fails
        structured_data["postgres_version"] = f"Could not detect version: {e}"


    # --- Best Practice Guidelines (Now with Version-Specific Notes) ---
    BEST_PRACTICE_GUIDELINES = {
        'shared_buffers': {
            'guideline': "Typically 25% of total system RAM, up to a max of 8GB for most workloads.",
            'considerations': "The goal is to fit the 'working set' of your data in memory. Sizing this too large can negatively impact system performance. Requires a restart."
        },
        'work_mem': {
            'guideline': "Start with 4MB-8MB. Increase for complex reporting queries.",
            'considerations': "This memory is allocated *per operation* within a query. High values can lead to memory exhaustion with many concurrent sessions."
        },
        'maintenance_work_mem': {
            'guideline': "Start with 256MB-512MB. Can be larger for big databases.",
            'considerations': "Used for VACUUM, CREATE INDEX, etc. A larger value can significantly speed up these maintenance tasks."
        },
        'effective_cache_size': {
            'guideline': "Typically 50-75% of total system RAM.",
            'considerations': "A hint to the query planner about how much memory is available for caching data, influencing its choice of query plans."
        },
        'max_connections': {
            'guideline': "Application-dependent. Use a connection pooler (like PgBouncer).",
            'considerations': "Each connection consumes memory. It's better to have fewer connections via a pooler than to set this value too high. Requires a restart."
        },
        'checkpoint_completion_target': {
            'guideline': "0.9 (90%)",
            'considerations': "Spreads checkpoint I/O over a longer period, reducing I/O spikes. A value of 0.9 is the standard recommendation for most systems."
        },
        'wal_buffers': {
            'guideline': "16MB (the maximum effective value). The default of -1 is often sufficient on modern versions (10+).",
            'considerations': "The default of -1 automatically sizes it to 1/32 of shared_buffers, but setting it to 16MB is a common best practice."
        },
        'max_wal_size': {
            'guideline': "Start with 1GB-2GB. Increase for write-heavy workloads.",
            'considerations': "This is the modern way to manage WAL size (since PG 9.5), replacing the older `checkpoint_segments`."
        },
        'random_page_cost': {
            'guideline': "1.1 for SSD/NVMe. The default was changed from 4.0 to 1.1 in PG 10.",
            'considerations': f"Your version (PG {pg_major_version}) defaults to {'1.1' if pg_major_version >= 10 else '4.0'}. A lower value encourages index scans."
        },
        'effective_io_concurrency': {
            'guideline': "200+ for modern SSDs, 2 for HDDs.",
            'considerations': "Relevant for bitmap heap scans. Represents the number of concurrent I/O operations the system can handle."
        }
    }

    # --- Data Collection ---
    settings_to_check = list(BEST_PRACTICE_GUIDELINES.keys())
    if pg_major_version < 9.5: # max_wal_size doesn't exist before 9.5
        settings_to_check.remove('max_wal_size')

    settings_placeholders = ', '.join([f"'{s}'" for s in settings_to_check])
    config_query = f"SELECT name, setting, unit, short_desc, context FROM pg_settings WHERE name IN ({settings_placeholders}) ORDER BY name;"

    if settings['show_qry'] == 'true':
        adoc_content.append(f"Configuration analysis query (for PostgreSQL {pg_major_version}):")
        adoc_content.append("[,sql]\n----")
        adoc_content.append(config_query)
        adoc_content.append("----")

    formatted_result, raw_settings = execute_query(config_query, return_raw=True)

    if "[ERROR]" in formatted_result:
        adoc_content.append(f"Could not retrieve configuration settings:\n{formatted_result}")
        structured_data["configuration_analysis"] = {"status": "error", "details": raw_settings}
        return "\n".join(adoc_content), structured_data

    structured_data["configuration_analysis"] = {"status": "success", "data": raw_settings}

    # --- Analysis and Reporting ---
    adoc_content.append(f"[NOTE]\n====\nThis section compares your database's current configuration (PostgreSQL {pg_major_version}) against general best practices. These are guidelines, not absolute rules. Optimal values depend heavily on your specific hardware, workload, and PostgreSQL version. Always test configuration changes in a staging environment.\n====\n")

    analysis_table = ["[cols=\"2,2,4,4\",options=\"header\"]", "|===", "| Setting | Current Value | Best Practice Guideline | Considerations"]
    
    current_settings_map = {s['name']: s for s in raw_settings}

    for name, guideline_info in BEST_PRACTICE_GUIDELINES.items():
        if name in current_settings_map:
            setting = current_settings_map[name]
            current_value = f"{setting['setting']}{setting['unit'] if setting['unit'] else ''}"
            
            analysis_table.append(f"| `{name}`")
            analysis_table.append(f"| `{current_value}`")
            analysis_table.append(f"| {guideline_info['guideline']}")
            analysis_table.append(f"| {guideline_info['considerations']}")

    analysis_table.append("|===")
    adoc_content.append("\n".join(analysis_table))

    if settings.get('is_aurora', False):
        adoc_content.append("\n[NOTE]\n====\n**AWS RDS Aurora Considerations:** Aurora manages many settings automatically. Focus on workload optimization and tuning within Aurora-specific parameter groups.\n====\n")

    return "\n".join(adoc_content), structured_data
