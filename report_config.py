REPORT_SECTIONS = [
    {
        "title": "Background",
        "actions": [
            {"type": "comments", "file": "background.txt"}
        ]
    },
    {
        "title": "PostgreSQL Overview",
        "actions": [
            {"type": "module", "module": "postgres_overview", "function": "run_postgres_overview"}
        ]
    },
    {
        "condition": {"var": "run_osinfo", "value": "true"},
        "title": "System Details",
        "actions": [
            {"type": "module", "module": "get_osinfo", "function": "run_osinfo"}
        ]
    },
    {
        "title": "PostgreSQL Settings",
        "actions": [
            {"type": "module", "module": "section_postgresql_settings", "function": "run_settings"},
            {"type": "module", "module": "aurora_cpu_metrics", "function": "run_aurora_cpu_metrics"},
            {"type": "module", "module": "datadog_setup", "function": "run_datadog_setup"}
        ]
    },
    {
        "title": "Cache Analysis",
        "actions": [
            {"type": "module", "module": "section_cache_analysis", "function": "run_cache_analysis"}
        ]
    },
    {
        "title": "Vacuum, Bloat and TXID Wrap Analysis",
        "actions": [
            {"type": "module", "module": "section_vacuum_analysis", "function": "run_vacuum_analysis"},
            {"type": "module", "module": "autovacuum_config", "function": "run_autovacuum_config"},
            {"type": "module", "module": "vacstat2", "function": "run_vacstat2"},
            {"type": "module", "module": "table_metrics", "function": "run_table_metrics"}
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
        "title": "Index Analysis for db: ${PGDB}",
        "actions": [
            {"type": "module", "module": "unused_idx", "function": "run_unused_idx"},
            {"type": "module", "module": "dupe_idx", "function": "run_dupe_idx"},
            {"type": "module", "module": "missing_idx", "function": "run_missing_idx"},
            {"type": "module", "module": "large_idx", "function": "run_large_idx"},
            {"type": "module", "module": "idx_brin", "function": "run_brin_idx"},
            {"type": "comments", "file": "indexes.txt"}
        ]
    },
    {
        "title": "Table Analysis for db: ${PGDB}",
        "actions": [
            {"type": "module", "module": "large_tbl", "function": "run_large_tbl"},
            {"type": "module", "module": "table_object_counts", "function": "run_table_object_counts"},
            {"type": "module", "module": "matviews", "function": "run_matview"},
            {"type": "module", "module": "partitioned_tbl", "function": "run_list_part"},
            {"type": "module", "module": "n_tuples_in", "function": "run_tuples_in"},
            {"type": "module", "module": "table_metrics", "function": "run_table_metrics"},
            {"type": "module", "module": "foreign_key_audit", "function": "run_foreign_key_audit"},
            {"type": "comments", "file": "tables.txt"}
        ]
    },
    {
        "title": "Query Analysis",
        "actions": [
            {"type": "module", "module": "top_queries_by_execution_time", "function": "run_top_queries_by_execution_time"},
            {"type": "module", "module": "active_query_states", "function": "run_active_query_states"},
            {"type": "module", "module": "long_running_queries", "function": "run_long_running_queries"},
            {"type": "module", "module": "lock_wait_config", "function": "run_lock_wait_config"},
            {"type": "module", "module": "current_lock_waits", "function": "run_current_lock_waits"}
        ]
    },
    {
        "title": "Connections and Security for db: ${PGDB}",
        "actions": [
            {"type": "module", "module": "users", "function": "run_users"},
            {"type": "module", "module": "stat_ssl", "function": "run_stat_ssl"},
            {"type": "module", "module": "security_audit", "function": "run_security_audit"},
            {"type": "module", "module": "connection_metrics", "function": "run_connection_metrics"},
            {"type": "module", "module": "connection_pooling", "function": "run_connection_pooling"},
            {"type": "comments", "file": "users.txt"},
            {"type": "comments", "file": "security.txt"},
            {"type": "comments", "file": "connections.txt"}
        ]
    },
    {
        "title": "Recommendations",
        "actions": [
            {"type": "module", "module": "run_recommendation", "function": "run_recommendation"},
            {"type": "comments", "file": "recommendations.txt"},
            {"type": "image", "file": "example.png", "alt": "Example Image"}
        ]
    },
    {
        "title": "Appendix",
        "actions": [
            {"type": "module", "module": "pgset", "function": "run_pgset", "condition": {"var": "run_settings", "value": "true"}},
            {"type": "module", "module": "systemwide_extensions", "function": "run_systemwide_extensions", "condition": {"var": "show_avail_ext", "value": "true"}},
            {"type": "module", "module": "rds_upgrade", "function": "run_rds_upgrade"},
            {"type": "module", "module": "check_aws_reg", "function": "run_check_aws_reg"},
            {"type": "comments", "file": "ha.txt"}
        ]
    }
]
