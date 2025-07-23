# Security-Focused Report Configuration
# This configuration focuses on security analysis and compliance
# Use with: python3 pg_healthcheck.py --report-config report_config_security.py

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
        "title": "Security Analysis",
        "actions": [
            {"type": "module", "module": "users", "function": "run_users"},
            {"type": "module", "module": "stat_ssl", "function": "run_stat_ssl"},
            {"type": "module", "module": "security_audit", "function": "run_security_audit"},
            {"type": "module", "module": "hba_rules_audit", "function": "run_hba_rules_audit"},
            {"type": "module", "module": "data_checksums_analysis", "function": "run_data_checksums_analysis"}
        ]
    },
    {
        "title": "Connection Security",
        "actions": [
            {"type": "module", "module": "connection_metrics", "function": "run_connection_metrics"},
            {"type": "module", "module": "connection_pooling", "function": "run_connection_pooling"}
        ]
    },
    {
        "title": "Security Best Practices",
        "actions": [
            {"type": "comments", "file": "security.txt", "display_title": "General Security Best Practices"},
            {"type": "comments", "file": "users.txt", "display_title": "User and Role Management Best Practices"},
            {"type": "comments", "file": "connections.txt", "display_title": "Connection Management Best Practices"}
        ]
    },
    {
        "title": "Platform-Specific Security",
        "actions": [
            {"type": "comments", "file": "rds_aurora_best_practices.txt", "display_title": "AWS RDS/Aurora Best Practices"}
        ]
    },
    {
        "title": "Security Recommendations",
        "actions": [
            {"type": "module", "module": "run_recommendation_enhanced", "function": "run_recommendation_enhanced", "condition": {"var": "ai_analyze", "value": True}},
            {"type": "module", "module": "run_recommendation", "function": "run_recommendation", "condition": {"var": "ai_analyze", "value": True, "fallback": True}},
            {"type": "comments", "file": "recommendations.txt", "display_title": "Recommendations (Other)"}
        ]
    }
] 
