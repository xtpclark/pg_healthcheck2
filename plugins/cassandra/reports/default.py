REPORT_SECTIONS = [
    {
        "title": "Default Section",
        "actions": []
    },
    {
        "title": "Operational Health",
        "actions": [
            {"type": "module", "module": "plugins.cassandra.checks.compaction_pending_tasks", "function": "run_compaction_pending_tasks"},
            {"type": "module", "module": "plugins.cassandra.checks.schema_version_consistency_check", "function": "run_schema_version_consistency_check"},
            {"type": "module", "module": "plugins.cassandra.checks.disk_space_per_keyspace", "function": "run_disk_space_per_keyspace_check"},
            {"type": "module", "module": "plugins.cassandra.checks.data_directory_disk_space_check", "function": "run_data_directory_disk_space_check"},
            {"type": "module", "module": "plugins.cassandra.checks.memory_usage_check", "function": "run_memory_usage_check"},
#            {'type': 'module', 'module': 'plugins.cassandra.checks.cassandra_process_check', 'function': 'run_cassandra_process_check'},
#            {'type': 'module', 'module': 'plugins.cassandra.checks.system_log_errors_check', 'function': 'run_system_log_errors_check'},
            {'type': 'module', 'module': 'plugins.cassandra.checks.cpu_load_average_check', 'function': 'run_cpu_load_average_check'},
            {'type': 'module', 'module': 'plugins.cassandra.checks.network_connections_check', 'function': 'run_network_connections_check'},
            {'type': 'module', 'module': 'plugins.cassandra.checks.temporary_files_check', 'function': 'run_temporary_files_check'},
            {'type': 'module', 'module': 'plugins.cassandra.checks.commitlog_directory_size_check', 'function': 'run_commitlog_directory_size_check'},
#            {'type': 'module', 'module': 'plugins.cassandra.checks.java_heap_usage_check', 'function': 'run_java_heap_usage_check'},
            {'type': 'module', 'module': 'plugins.cassandra.checks.gcstats_check', 'function': 'run_gcstats_check'},

            {'type': 'module', 'module': 'plugins.cassandra.checks.cluster_connectivity_check', 'function': 'run_cluster_connectivity_check'},
            {'type': 'module', 'module': 'plugins.cassandra.checks.nodetool_gossipinfo_peers_check', 'function': 'run_nodetool_gossipinfo_peers_check'},
            {'type': 'module', 'module': 'plugins.cassandra.checks.check_node_states', 'function': 'run_check_node_states'},
            {'type': 'module', 'module': 'plugins.cassandra.checks.gc_grace_seconds_audit', 'function': 'run_gc_grace_seconds_audit'},
            {'type': 'module', 'module': 'plugins.cassandra.checks.table_compression_settings', 'function': 'run_table_compression_settings'},
            {'type': 'module', 'module': 'plugins.cassandra.checks.row_cache_check', 'function': 'run_row_cache_check'},
            {'type': 'module', 'module': 'plugins.cassandra.checks.durable_writes_check', 'function': 'run_durable_writes_check'},
            {'type': 'module', 'module': 'plugins.cassandra.checks.materialized_views_check', 'function': 'run_materialized_views_check'},

            {'type': 'module', 'module': 'plugins.cassandra.checks.udf_aggregates_check', 'function': 'run_udf_aggregates_check'},
            {'type': 'module', 'module': 'plugins.cassandra.checks.keyspace_replication_health', 'function': 'run_keyspace_replication_health'},
            {'type': 'module', 'module': 'plugins.cassandra.checks.superuser_roles', 'function': 'run_superuser_roles_check'},
#            {'type': 'module', 'module': 'plugins.cassandra.checks.tombstone_metrics', 'function': 'run_tombstone_metrics'},
        ]
    },
    {
        'title': 'Configuration',
        'actions': [
            {'type': 'module', 'module': 'plugins.cassandra.checks.keyspace_replication_strategy', 'function': 'run_keyspace_replication_strategy'},
        ]
    },
    {
        'title': 'Hardware',
        'actions': [
             {'type': 'module', 'module': 'plugins.cassandra.checks.check_disk_usage', 'function': 'run_check_disk_usage'},       
        ]
    },
    {
        'title': 'Performance Monitoring',
        'actions': [
#            {'type': 'module', 'module': 'plugins.cassandra.checks.tombstone_metrics_check', 'function': 'run_tombstone_metrics_check'},
        ]
    },
    {
        'title': 'Security',
        'actions': [
## Failed message
#            {'type': 'module', 'module': 'plugins.cassandra.checks.role_permission_audit', 'function': 'run_role_permission_audit'},
        ]
    },
]

def get_default_report_definition(connector, settings):
    """Returns the report structure."""
    return REPORT_SECTIONS
