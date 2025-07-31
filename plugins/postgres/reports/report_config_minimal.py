"""Defines a minimal report structure for PostgreSQL health checks.

This module contains the primary configuration for the minimal report,
detailing the sections and the specific check modules to be run in order.

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
        "title": "Critical Performance Settings",
        "actions": [
            {"type": "module", "module": "critical_performance_settings", "function": "run_critical_performance_settings"},
            {"type": "module", "module": "suggested_config_values", "function": "run_suggested_config_values"}
        ]
    },
    {
        "title": "Security Audit",
        "actions": [
            {"type": "module", "module": "security_audit", "function": "run_security_audit"},
            {"type": "module", "module": "hba_rules_audit", "function": "run_hba_rules_audit"}
        ]
    },
    {
        "title": "Critical Issues",
        "actions": [
            {"type": "module", "module": "unused_idx", "function": "run_unused_idx"},
            {"type": "module", "module": "no_pk_uk_tables", "function": "run_no_pk_uk_tables"},
            {"type": "module", "module": "current_lock_waits", "function": "run_current_lock_waits"},
            {"type": "module", "module": "long_running_queries", "function": "run_long_running_queries"}
        ]
    }
]
