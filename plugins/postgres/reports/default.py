# This file defines the structure and execution order of the health check report.
# It is focused on generating actionable recommendations with consolidated checks.

REPORT_SECTIONS = [
    # --- Section 1: Header and Overview ---
    {
        'title': '', # No title for the header section
        'actions': [
            {'type': 'header', 'file': 'report_header.txt'},
        ]
    },

    # --- Section 2: PostgreSQL Overview ---
    {
        'title': 'PostgreSQL Overview',
        'actions': [
            {'type': 'module', 'module': 'plugins.postgres.checks.postgres_overview', 'function': 'run_postgres_overview'},
            {'type': 'module', 'module': 'plugins.postgres.checks.table_object_counts', 'function': 'run_table_object_counts'},
            {'type': 'module', 'module': 'plugins.postgres.checks.superuser_reserved', 'function': 'run_superuser_reserved'},
        ]
    },

    # --- Section 3: RDS/Aurora Overview ---
    {
        'title': 'RDS/Aurora Overview',
        'actions': [
            {'type': 'module', 'module': 'plugins.postgres.checks.aurora_stat_statements', 'function': 'run_aurora_stat_statements'},
            {'type': 'module', 'module': 'plugins.postgres.checks.aws_cloudwatch_metrics', 'function': 'run_aws_cloudwatch_metrics'}
        ]
    },

    # --- Section 4: Configuration Analysis ---
    {
        'title': 'Configuration Analysis',
        'actions': [
#            {'type': 'module', 'module': 'plugins.postgres.checks.suggested_config_values', 'function': 'run_suggested_config_values'},
            {'type': 'module', 'module': 'plugins.postgres.checks.pg_stat_statements_config', 'function': 'run_pg_stat_statements_config'},
            {'type': 'module', 'module': 'plugins.postgres.checks.autovacuum_config', 'function': 'run_autovacuum_config'},
            {'type': 'module', 'module': 'plugins.postgres.checks.extensions_update_check', 'function': 'run_extensions_update_check'}
        ]
    },


    # --- Section 5: Performance and Query Analysis ---
    {
        'title': 'Performance and Query Analysis',
        'actions': [
            {'type': 'module', 'module': 'plugins.postgres.checks.cache_analysis', 'function': 'run_cache_analysis'},
            {'type': 'module', 'module': 'plugins.postgres.checks.bgwriter', 'function': 'run_bgwriter_analysis'},
            {'type': 'module', 'module': 'plugins.postgres.checks.high_insert_tables', 'function': 'run_high_insert_tables'},
            {'type': 'module', 'module': 'plugins.postgres.checks.query_analysis', 'function': 'run_query_analysis'},
            {'type': 'module', 'module': 'plugins.postgres.checks.top_queries_by_execution_time', 'function': 'run_top_queries_by_execution_time'},
            {'type': 'module', 'module': 'plugins.postgres.checks.top_write_queries', 'function': 'run_top_write_queries'},
            {'type': 'module', 'module': 'plugins.postgres.checks.hot_queries', 'function': 'run_hot_queries'},
            {'type': 'module', 'module': 'plugins.postgres.checks.long_running_queries', 'function': 'run_long_running_queries'},
            {'type': 'module', 'module': 'plugins.postgres.checks.current_lock_waits', 'function': 'run_current_lock_waits'},
            {'type': 'module', 'module': 'plugins.postgres.checks.temp_files_analysis', 'function': 'run_temp_files_analysis'}
        ]
    },

    # --- Section 6: Table and Index Health (Consolidated) ---
    {
        'title': 'Table and Index Health',
        'actions': [
            {'type': 'module', 'module': 'plugins.postgres.checks.table_object_counts', 'function': 'run_table_object_counts'},
            {'type': 'module', 'module': 'plugins.postgres.checks.table_metrics', 'function': 'run_table_metrics'},
            {'type': 'module', 'module': 'plugins.postgres.checks.index_health_analysis', 'function': 'run_index_health_analysis'},
            {'type': 'module', 'module': 'plugins.postgres.checks.table_constraint_analysis', 'function': 'run_table_constraint_analysis'},
            {'type': 'module', 'module': 'plugins.postgres.checks.foreign_key_audit', 'function': 'run_foreign_key_audit'},
            {'type': 'module', 'module': 'plugins.postgres.checks.primary_key_analysis', 'function': 'run_primary_key_analysis'},
            {'type': 'module', 'module': 'plugins.postgres.checks.function_audit', 'function': 'run_function_audit'}
#            {'type': 'module', 'module': 'plugins.postgres.checks.database_object_inventory', 'function': 'run_object_inventory'},
        ]
    },

    # --- Section 5: Security and Replication (Consolidated) ---
    {
        'title': 'Security and Replication',
        'actions': [
            {'type': 'module', 'module': 'plugins.postgres.checks.security_and_encryption', 'function': 'run_security_and_encryption_analysis'},
            {'type': 'module', 'module': 'plugins.postgres.checks.replication_health', 'function': 'run_replication_health'}
        ]
    },
]
    # --- FINAL SECTION: AI-Generated Recommendations ---
    # This section is now implicitly handled by the core engine when `ai_analyze: true` is set in config.
    # No explicit module call is needed in the report configuration itse
