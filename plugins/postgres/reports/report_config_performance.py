"""Defines the performance report structure for PostgreSQL health checks, detailing the sections and the specific check modules to be run in order.

Attributes:
    REPORT_SECTIONS (list): A list of dictionaries defining the report
        structure. Each dictionary represents a section with a title and
        a list of actions (modules to run).
"""

REPORT_SECTIONS = [
    # Report Header
    {
        "title": "Report Header",
        "actions": [
            {"type": "header", "file": "report_header.txt"}
        ]
    },
    {
        "title": "Background",
        "actions": [
            {"type": "comments", "display_title": "", "file": "background.txt"}
        ]
    },
    {
        "title": "PostgreSQL Overview",
        "actions": [
            {"type": "module", "module": "postgres_overview", "function": "run_postgres_overview"}
        ]
    },
    {
        "title": "Performance Settings",
        "actions": [
            {"type": "module", "module": "critical_performance_settings", "function": "run_critical_performance_settings"},
            {"type": "module", "module": "suggested_config_values", "function": "run_suggested_config_values"}
        ]
    },
    {
        "title": "Cache Analysis",
        "actions": [
            {"type": "module", "module": "section_cache_analysis", "function": "run_cache_analysis"},
            {"type": "module", "module": "temp_files_analysis", "function": "run_temp_files_analysis"}
        ]
    },
    {
        "title": "Query Performance",
        "actions": [
            {"type": "module", "module": "top_queries_by_execution_time", "function": "run_top_queries_by_execution_time"},
            {"type": "module", "module": "hot_queries", "function": "run_hot_queries"},
            {"type": "module", "module": "long_running_queries", "function": "run_long_running_queries"},
            {"type": "module", "module": "wait_event_analysis", "function": "run_wait_event_analysis"}
        ]
    },
    {
        "title": "Lock Analysis",
        "actions": [
            {"type": "module", "module": "lock_wait_config", "function": "run_lock_wait_config"},
            {"type": "module", "module": "current_lock_waits", "function": "run_current_lock_waits"},
            {"type": "module", "module": "pg_locks_analysis", "function": "run_pg_locks_analysis"}
        ]
    },
    {
        "title": "Index Analysis",
        "actions": [
            {"type": "module", "module": "unused_idx", "function": "run_unused_idx"},
            {"type": "module", "module": "missing_idx", "function": "run_missing_idx"},
            {"type": "module", "module": "large_idx", "function": "run_large_idx"},
            {"type": "module", "module": "dupe_idx", "function": "run_dupe_idx"}
        ]
    },
    {
        "title": "Table Performance",
        "actions": [
            {"type": "module", "module": "large_tbl", "function": "run_large_tbl"},
            {"type": "module", "module": "table_metrics", "function": "run_table_metrics"},
            {"type": "module", "module": "foreign_key_audit", "function": "run_foreign_key_audit"}
        ]
    },
    {
        "title": "Vacuum and Maintenance",
        "actions": [
            {"type": "module", "module": "section_vacuum_analysis", "function": "run_vacuum_analysis"},
            {"type": "module", "module": "autovacuum_config", "function": "run_autovacuum_config"},
            {"type": "module", "module": "vacstat2", "function": "run_vacstat2"}
        ]
    }
]

    # --- FINAL SECTION: AI-Generated Recommendations ---
    # This section is now implicitly handled by the core engine when `ai_analyze: true` is set in config.
    # No explicit module call is needed in the report configuration itse
