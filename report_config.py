REPORT_SECTIONS = [
    # Special section for report header
    {
        "title": "Report Header", # This title is internal, won't be rendered as a section header
        "actions": [
            {"type": "header", "file": "report_header.txt"} # New type 'header'
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
        "condition": {"var": "run_osinfo", "value": True},
        "title": "System Details",
        "actions": [
            {"type": "module", "module": "get_osinfo", "function": "run_osinfo"}
        ]
    },
    {
        "title": "PostgreSQL Settings",
        "actions": [
            {"type": "module", "module": "general_config_settings", "function": "run_general_config_settings"},
            {"type": "module", "module": "critical_performance_settings", "function": "run_critical_performance_settings"},
            {"type": "module", "module": "suggested_config_values", "function": "run_suggested_config_values"},
            {"type": "module", "module": "aurora_cpu_metrics", "function": "run_aurora_cpu_metrics"},
            {"type": "module", "module": "datadog_setup", "function": "run_datadog_setup"}
        ]
    },
    {
        "title": "Cache Analysis\nwork_mem directly affects buffer cache hit ratios because when its setting is too low, PostgreSQL is forced to spill intermediate data to slower temporary files on disk, leading to reduced in-memory processing and a higher number of cache misses.",
        "actions": [
            {"type": "module", "module": "temp_files_analysis", "function": "run_temp_files_analysis"},
            {"type": "module", "module": "section_cache_analysis", "function": "run_cache_analysis"}
        ]
    },
    {
        "title": "Vacuum, Bloat and TXID Wrap Analysis",
        "actions": [
            {"type": "module", "module": "section_vacuum_analysis", "function": "run_vacuum_analysis"},
            {"type": "module", "module": "autovacuum_config", "function": "run_autovacuum_config"},
            {"type": "module", "module": "vacstat2", "function": "run_vacstat2"}
         #   {"type": "module", "module": "table_metrics", "function": "run_table_metrics"}
        ]
    },
    {
        "title": "Monitoring",
        "actions": [
            {"type": "module", "module": "pg_stat_statements_config", "function": "run_pg_stat_statements_config"},
            {"type": "module", "module": "monitoring_metrics", "function": "run_monitoring_metrics"},
            {"type": "module", "module": "monitoring_recommendations", "function": "run_monitoring_recommendations"}
        ]
    },
    {
        "title": "Replication",
        "actions": [
            {"type": "module", "module": "pub_sub", "function": "run_pub_sub"},
            {"type": "module", "module": "physical_replication", "function": "run_physical_replication"},
            {"type": "module", "module": "high_availability", "function": "run_high_availability"}
        ]
    },
    {
        "title": "WAL and Checkpoints",
        "actions": [
            {"type": "module", "module": "checkpoint", "function": "run_checkpoint"},
            {"type": "module", "module": "wal_usage", "function": "run_wal_usage"},
            {"type": "module", "module": "bgwriter", "function": "run_bgwriter"}
        ]
    },
    {
        "title": "Index Analysis",
        "actions": [
            {"type": "module", "module": "idx_brin", "function": "run_brin_idx"},
            {"type": "module", "module": "idx_gin", "function": "run_gin_idx"},
            {"type": "module", "module": "idx_spgist", "function": "run_spgist_idx"},
            {"type": "module", "module": "idx_hash", "function": "run_hash_idx"},
            {"type": "module", "module": "idx_gist", "function": "run_gist_idx"},
            {"type": "module", "module": "dupe_idx", "function": "run_dupe_idx"},
            {"type": "module", "module": "unused_idx", "function": "run_unused_idx"},
            {"type": "module", "module": "invalid_idx", "function": "run_invalid_idx"},
            {"type": "module", "module": "missing_idx", "function": "run_missing_idx"},
            {"type": "module", "module": "large_idx", "function": "run_large_idx"}
        ]
    },
   # {
   #     "title": "Duplicate Index Analysis",
   #     "actions": [
   #         {"type": "module", "module": "dupe_idx", "function": "run_dupe_idx"}
   #     ]
   # },
    {
        "title": "Table Analysis",
        "actions": [
            {"type": "module", "module": "large_tbl", "function": "run_large_tbl"},
            {"type": "module", "module": "partitioned_tbl", "function": "run_list_part"},
            # {"type": "module", "module": "n_tuples_in", "function": "run_tuples_in"},
            {"type": "module", "module": "high_insert_tables", "function": "run_high_insert_tables"}, #n_tuples_in
            {"type": "module", "module": "top_write_queries", "function": "run_top_write_queries"}, #n_tuples_in
            {"type": "module", "module": "matviews", "function": "run_matview"},
            {"type": "module", "module": "table_metrics", "function": "run_table_metrics"},
            {"type": "module", "module": "foreign_key_audit", "function": "run_foreign_key_audit"},
            {"type": "module", "module": "primary_key_analysis", "function": "run_primary_key_analysis"},
            {"type": "module", "module": "no_pk_uk_tables", "function": "run_no_pk_uk_tables"},
            {"type": "module", "module": "table_object_counts", "function": "run_table_object_counts"},
        ]
    },
    {
        'title': 'Function and Stored Procedure Audit',
        'actions': [
            {'type': 'module', 'module': 'function_audit', 'function': 'run_function_audit'},
        ]
    },
    {
        "title": "Query Analysis",
        "actions": [
            {"type": "module", "module": "top_queries_by_execution_time", "function": "run_top_queries_by_execution_time"},
            {"type": "module", "module": "active_query_states", "function": "run_active_query_states"},
            {"type": "module", "module": "long_running_queries", "function": "run_long_running_queries"},
            {"type": "module", "module": "lock_wait_config", "function": "run_lock_wait_config"},
            {"type": "module", "module": "current_lock_waits", "function": "run_current_lock_waits"},
            {"type": "module", "module": "pg_locks_analysis", "function": "run_pg_locks_analysis"},
            {"type": "module", "module": "wait_event_analysis", "function": "run_wait_event_analysis"},
            {"type": "module", "module": "hot_queries", "function": "run_hot_queries"}
        ]
    },
    {
        "title": "Connections and Security",
        "actions": [
            {"type": "module", "module": "users", "function": "run_users"},
            {"type": "module", "module": "stat_ssl", "function": "run_stat_ssl"},
            {"type": "module", "module": "security_audit", "function": "run_security_audit"},
            {"type": "module", "module": "hba_rules_audit", "function": "run_hba_rules_audit"},
            {"type": "module", "module": "connection_metrics", "display_title":"Connection Metrics", "function": "run_connection_metrics"},
            {"type": "module", "module": "connection_pooling", "function": "run_connection_pooling"},
            {"type": "module", "module": "data_checksums_analysis", "function": "run_data_checksums_analysis"}
        ]
    },
 
#   {
#        "title": "Trend Analysis",
#        "actions": [
#            {"type": "module", "module": "trend_analysis_storage", "function": "run_trend_storage", "condition": {"var": "trend_storage_enabled", "value": True}},
#            {"type": "module", "module": "trend_analysis_viewer", "function": "run_trend_viewer", "condition": {"var": "trend_storage_enabled", "value": True}}
#        ]
#    },
    {
        "title": "Recommendations",
        "actions": [
            {"type": "module", "module": "run_recommendation_enhanced", "function": "run_recommendation_enhanced", "condition": {"var": "ai_analyze", "value": True}},
            {"type": "module", "module": "run_recommendation", "function": "run_recommendation", "condition": {"var": "ai_analyze", "value": True, "fallback": True}},
            {"type": "comments", "file": "recommendations.txt", "display_title": "Recommendations (Other)"},
            {"type": "comments", "file": "pgbadger_setup.txt", "display_title": "PgBadger Setup"}
        #    {"type": "image", "file": "example.png", "alt": "Example Image"}
        ]
    },
    # New section for General PostgreSQL Best Practices
    {
        "title": "General PostgreSQL Best Practices",
        "actions": [
            {"type": "comments", "file": "indexes.txt", "display_title": "Index Management Best Practices"},
            {"type": "module", "module": "index_replica_analysis", "function": "run_index_replica_analysis"},
            {"type": "comments", "file": "tables.txt", "display_title": "Table Management Best Practices"},
            {"type": "comments", "file": "users.txt", "display_title": "User and Role Management Best Practices"},
            {"type": "comments", "file": "security.txt", "display_title": "General Security Best Practices"},
            {"type": "comments", "file": "connections.txt", "display_title": "Connection Management Best Practices"},
            {"type": "comments", "file": "ha.txt", "display_title": "High Availability (HA) Best Practices"}
        ]
    },
    # New section for Platform-Specific Best Practices
    {
        "title": "Platform-Specific Best Practices",
        "actions": [
            {"type": "comments", "file": "rds_aurora_best_practices.txt", "display_title": "AWS RDS/Aurora Best Practices"},
            {"type": "comments", "file": "instaclustr_best_practices.txt", "display_title": "Instaclustr Managed PostgreSQL Best Practices"},
            {"type": "comments", "file": "netapp_anf_best_practices.txt", "display_title": "NetApp ANF Storage Best Practices for PostgreSQL"}
        ]
    },
    {
        "title": "Appendix",
        "actions": [
#            {"type": "module", "module": "pgset", "function": "run_pgset", "condition": {"var": "run_settings", "value": True}},
            {"type": "module", "module": "systemwide_extensions", "function": "run_systemwide_extensions", "condition": {"var": "show_avail_ext", "value": True}},
            {"type": "module", "module": "extensions_update_check", "function": "run_extensions_update_check"},
            {"type": "module", "module": "rds_upgrade", "function": "run_rds_upgrade"},
            {"type": "module", "module": "check_aws_region", "function": "run_check_aws_region"}
        ]
    }
]
