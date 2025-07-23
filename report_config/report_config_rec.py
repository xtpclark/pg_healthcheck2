# report_config/report_config.py
#
# This file defines the structure and execution order of the health check report.
# Modules are executed sequentially based on their order in this list.
#
# Best Practice: Group related checks into logical sections and always place
# the AI analysis section at the very end to ensure it has all the data.

REPORT_SECTIONS = [
    # --- Section 1: Header and Overview ---
    # Start with the report header and a high-level overview of the database.
    {
        'title': '', # No title for the header section
        'actions': [
            {'type': 'header', 'file': 'report_header.txt'},
        ]
    },
    {
        'title': 'PostgreSQL Overview',
        'actions': [
            {'type': 'module', 'module': 'postgres_overview', 'function': 'run_postgres_overview'},
        ]
    },

    # --- Section 2: Configuration Analysis ---
    # Review key configuration settings against best practices.
    {
        'title': 'Configuration Analysis',
        'actions': [
            {'type': 'module', 'module': 'suggested_config_values', 'function': 'run_suggested_config_values'},
            {'type': 'module', 'module': 'pg_stat_statements_config', 'function': 'run_pg_stat_statements_config'},
            {'type': 'module', 'module': 'autovacuum_config', 'function': 'run_autovacuum_config'},
        ]
    },

    # --- Section 3: Performance and Query Analysis ---
    # Deep dive into query performance, locks, and other performance metrics.
    {
        'title': 'Performance and Query Analysis',
        'actions': [
            {'type': 'module', 'module': 'top_queries_by_execution_time', 'function': 'run_top_queries_by_execution_time'},
            {'type': 'module', 'module': 'top_write_queries', 'function': 'run_top_write_queries'},
            {'type': 'module', 'module': 'hot_queries', 'function': 'run_hot_queries'},
            {'type': 'module', 'module': 'long_running_queries', 'function': 'run_long_running_queries'},
            {'type': 'module', 'module': 'current_lock_waits', 'function': 'run_current_lock_waits'},
            {'type': 'module', 'module': 'temp_files_analysis', 'function': 'run_temp_files_analysis'},
        ]
    },

    # --- Section 4: Table and Index Health ---
    # Analyze table bloat, index usage, and other schema-related health metrics.
    {
        'title': 'Table and Index Health',
        'actions': [
            {'type': 'module', 'module': 'table_metrics', 'function': 'run_table_metrics'},
            {'type': 'module', 'module': 'unused_idx', 'function': 'run_unused_idx'},
            {'type': 'module', 'module': 'dupe_idx', 'function': 'run_dupe_idx'},
            {'type': 'module', 'module': 'invalid_idx', 'function': 'run_invalid_idx'},
            {'type': 'module', 'module': 'no_pk_uk_tables', 'function': 'run_no_pk_uk_tables'},
        ]
    },

    # --- Section 5: Security and Replication ---
    # Review security settings, user access, and replication status.
    {
        'title': 'Security and Replication',
        'actions': [
            {'type': 'module', 'module': 'security_audit', 'function': 'run_security_audit'},
            {'type': 'module', 'module': 'stat_ssl', 'function': 'run_stat_ssl'},
            {'type': 'module', 'module': 'physical_replication', 'function': 'run_physical_replication'},
            {'type': 'module', 'module': 'pub_sub', 'function': 'run_pub_sub'},
        ]
    },

    # --- FINAL SECTION: AI-Generated Recommendations ---
    # This section MUST be last. It takes all the structured data collected
    # by the previous modules and sends it to the AI for analysis.
    {
        'title': 'AI-Generated Recommendations',
        'condition': {'var': 'ai_analyze', 'value': True}, # Only run if ai_analyze is true in config.yaml
        'actions': [
            {'type': 'module', 'module': 'run_recommendation_enhanced', 'function': 'run_recommendation_enhanced'},
        ]
    },
]
