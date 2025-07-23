# report_config/report_config_rec.py
#
# This file defines the structure and execution order of the health check report.
# It is focused on generating actionable recommendations.

REPORT_SECTIONS = [
    # --- Section 1: Header and Overview ---
    {
        'title': '', # No title for the header section
        'actions': [
            {'type': 'header', 'file': 'report_header.txt'},
        ]
    },
    {
        'title': 'PostgreSQL Overview',
        'actions': [
            {'type': 'module', 'module': 'plugins.postgres.checks.postgres_overview', 'function': 'run_postgres_overview'},
        ]
    },

    # --- Section 2: Configuration Analysis ---
    {
        'title': 'Configuration Analysis',
        'actions': [
            {'type': 'module', 'module': 'plugins.postgres.checks.suggested_config_values', 'function': 'run_suggested_config_values'},
            {'type': 'module', 'module': 'plugins.postgres.checks.pg_stat_statements_config', 'function': 'run_pg_stat_statements_config'},
            {'type': 'module', 'module': 'plugins.postgres.checks.autovacuum_config', 'function': 'run_autovacuum_config'},
        ]
    },

    # --- Section 3: Performance and Query Analysis ---
    {
        'title': 'Performance and Query Analysis',
        'actions': [
            {'type': 'module', 'module': 'plugins.postgres.checks.top_queries_by_execution_time', 'function': 'run_top_queries_by_execution_time'},
            {'type': 'module', 'module': 'plugins.postgres.checks.top_write_queries', 'function': 'run_top_write_queries'},
            {'type': 'module', 'module': 'plugins.postgres.checks.hot_queries', 'function': 'run_hot_queries'},
            {'type': 'module', 'module': 'plugins.postgres.checks.long_running_queries', 'function': 'run_long_running_queries'},
            {'type': 'module', 'module': 'plugins.postgres.checks.current_lock_waits', 'function': 'run_current_lock_waits'},
            {'type': 'module', 'module': 'plugins.postgres.checks.temp_files_analysis', 'function': 'run_temp_files_analysis'},
        ]
    },

    # --- Section 4: Table and Index Health ---
    {
        'title': 'Table and Index Health',
        'actions': [
            {'type': 'module', 'module': 'plugins.postgres.checks.table_metrics', 'function': 'run_table_metrics'},
            {'type': 'module', 'module': 'plugins.postgres.checks.unused_idx', 'function': 'run_unused_idx'},
            {'type': 'module', 'module': 'plugins.postgres.checks.dupe_idx', 'function': 'run_dupe_idx'},
            {'type': 'module', 'module': 'plugins.postgres.checks.invalid_idx', 'function': 'run_invalid_idx'},
            {'type': 'module', 'module': 'plugins.postgres.checks.no_pk_uk_tables', 'function': 'run_no_pk_uk_tables'},
        ]
    },

    # --- Section 5: Security and Replication ---
    {
        'title': 'Security and Replication',
        'actions': [
            {'type': 'module', 'module': 'plugins.postgres.checks.security_audit', 'function': 'run_security_audit'},
            {'type': 'module', 'module': 'plugins.postgres.checks.stat_ssl', 'function': 'run_stat_ssl'},
            {'type': 'module', 'module': 'plugins.postgres.checks.physical_replication', 'function': 'run_physical_replication'},
            {'type': 'module', 'module': 'plugins.postgres.checks.pub_sub', 'function': 'run_pub_sub'},
        ]
    },

    # --- FINAL SECTION: AI-Generated Recommendations ---
    {
        'title': 'AI-Generated Recommendations',
        'condition': {'var': 'ai_analyze', 'value': True},
        'actions': [
            # This utility is now called from main.py and doesn't need to be a module here.
            # We keep a placeholder or it can be removed entirely depending on final main.py logic.
            # For now, let's assume it's handled by the core engine.
        ]
    },
]
