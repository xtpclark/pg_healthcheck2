import re

def get_weight():
    """Returns the importance score for this module."""
    return 1

def _to_megabytes(value_str, unit_str):
    """Helper function to convert PostgreSQL memory settings to megabytes."""
    if not unit_str or not value_str or not value_str.isdigit():
        return 0
    value = int(value_str)
    if unit_str == 'kB':
        return value / 1024
    if unit_str == 'MB':
        return value
    if unit_str == 'GB':
        return value * 1024
    if 'kB' in unit_str: # Handles '8kB', '16kB', etc.
        block_size_kb = int(re.findall(r'\d+', unit_str)[0])
        return (value * block_size_kb) / 1024
    return 0

def run_suggested_config_values(connector, settings):
    """
    Analyzes key configuration values against best practices and highlights potential issues.
    """
    adoc_content = ["=== Configuration Analysis vs. Best Practices (Version-Aware)", "Analyzes current configuration settings against established PostgreSQL best practices, adjusted for your specific version, to identify areas for review and tuning.\n"]
    structured_data = {}
    
    # Ensure settings is a dict to prevent errors on .get()
    settings = settings or {}
    version_info = connector.version_info
    pg_major_version = version_info.get('major_version', 0)
    
    # Handle Aurora-specific case first
    if settings.get('is_aurora', False):
        adoc_content.append("[NOTE]\n====\n*Aurora Environment Detected:*\n\nIn AWS Aurora, many key configuration parameters like `shared_buffers`, `max_wal_size`, and `max_connections` are managed automatically by AWS. Tuning should be done through the DB Parameter Group in the AWS console. This check is skipped for Aurora environments.\n====\n")
        return "\n".join(adoc_content), structured_data

    # --- Standard PostgreSQL Analysis ---
    settings_to_check = [
        'shared_buffers', 'work_mem', 'maintenance_work_mem', 'effective_cache_size',
        'max_connections', 'checkpoint_completion_target', 'max_wal_size', 'random_page_cost',
        'effective_io_concurrency', 'autovacuum_freeze_max_age'
    ]
    
    settings_placeholders = ', '.join([f"'{s}'" for s in settings_to_check])
    config_query = f"SELECT name, setting, unit FROM pg_settings WHERE name IN ({settings_placeholders});"
    
    formatted_result, raw_settings = connector.execute_query(config_query, return_raw=True)

    if "[ERROR]" in formatted_result:
        adoc_content.append(f"Could not retrieve configuration settings:\n{formatted_result}")
        return "\n".join(adoc_content), {"configuration_analysis": {"status": "error"}}

    structured_data["configuration_analysis"] = raw_settings
    
    adoc_content.append(f"[NOTE]\n====\nThis section analyzes your configuration for PostgreSQL {pg_major_version}. These are guidelines, not absolute rules. Optimal values depend heavily on your specific hardware and workload.\n====\n")

    findings = []
    good_settings = []

    for setting in raw_settings:
        name = setting['name']
        value_str = setting['setting']
        unit_str = setting.get('unit')
        
        if name == 'shared_buffers':
            val_mb = _to_megabytes(value_str, unit_str)
            if val_mb < 256:
                findings.append(f"[CAUTION]\n====\n*{name}* is set to `{val_mb:.0f}MB`, which is very low for a production system. **Guideline:** Set to 25% of system RAM (up to 8GB for most workloads) to improve cache performance.\n====")
            else:
                good_settings.append(f"`{name}`: `{val_mb:.0f}MB`")
        
        elif name == 'work_mem':
            val_mb = _to_megabytes(value_str, unit_str)
            if val_mb < 4:
                findings.append(f"[CAUTION]\n====\n*{name}* is set to `{val_mb:.0f}MB`. This may cause complex queries to spill to disk. **Guideline:** Start at 4-8MB. Be cautious, as this is allocated per operation.\n====")
            else:
                good_settings.append(f"`{name}`: `{val_mb:.0f}MB`")

        elif name == 'maintenance_work_mem':
            val_mb = _to_megabytes(value_str, unit_str)
            if val_mb < 128:
                findings.append(f"[CAUTION]\n====\n*{name}* is set to `{val_mb:.0f}MB`. A larger value can significantly speed up maintenance tasks like VACUUM and CREATE INDEX. **Guideline:** Start at 256MB-512MB for larger databases.\n====")
            else:
                good_settings.append(f"`{name}`: `{val_mb:.0f}MB`")
        
        elif name == 'max_connections':
            if int(value_str) > 200:
                findings.append(f"[CAUTION]\n====\n*{name}* is set to `{value_str}`. A high number of connections consumes significant memory. **Guideline:** Use a connection pooler like PgBouncer to manage connections efficiently rather than setting this value too high.\n====")
            else:
                good_settings.append(f"`{name}`: `{value_str}`")

        elif name == 'random_page_cost':
            if pg_major_version >= 10 and float(value_str) > 1.1:
                findings.append(f"[CAUTION]\n====\n*{name}* is set to `{value_str}`. For modern SSDs, this should be `1.1` to encourage the planner to use index scans.\n====")
            else:
                good_settings.append(f"`{name}`: `{value_str}`")

        elif name == 'autovacuum_freeze_max_age':
            if int(value_str) < 2000000000:
                findings.append(f"[CAUTION]\n====\n*{name}* is set to `{int(value_str):,}`. **Guideline:** As a general cushion, consider setting this to the maximum value of `2,000,000,000` to provide more time before aggressive anti-wraparound vacuums are required.\n====")
            else:
                good_settings.append(f"`{name}`: `{int(value_str):,}`")

    if findings:
        adoc_content.append("\n".join(findings))
    else:
        adoc_content.append("\n[NOTE]\n====\nNo immediate configuration issues were detected based on a high-level analysis.\n====\n")

    if good_settings:
        adoc_content.append("\n==== Settings Within General Guidelines")
        adoc_content.append("[NOTE]\n====\n" + ", ".join(good_settings) + "\n====\n")

    return "\n".join(adoc_content), structured_data
