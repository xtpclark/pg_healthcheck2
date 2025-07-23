# Minimal Report Configuration
# This configuration includes only the most essential health check sections
# Use with: python3 pg_healthcheck.py --report-config report_config_minimal.py

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
    },
    {
        "title": "Recommendations",
        "actions": [
            {"type": "module", "module": "run_recommendation_enhanced", "function": "run_recommendation_enhanced", "condition": {"var": "ai_analyze", "value": True}},
            {"type": "module", "module": "run_recommendation", "function": "run_recommendation", "condition": {"var": "ai_analyze", "value": True, "fallback": True}},
            {"type": "comments", "file": "recommendations.txt", "display_title": "Recommendations (Other)"}
        ]
    }
] 