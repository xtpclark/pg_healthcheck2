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
            {'type': 'module', 'module': 'plugins.cassandra.checks.cassandra_process_check', 'function': 'run_cassandra_process_check'},
            {'type': 'module', 'module': 'plugins.cassandra.checks.system_log_errors_check', 'function': 'run_system_log_errors_check'},
            {'type': 'module', 'module': 'plugins.cassandra.checks.cpu_load_average_check', 'function': 'run_cpu_load_average_check'},
            {'type': 'module', 'module': 'plugins.cassandra.checks.network_connections_check', 'function': 'run_network_connections_check'},
            {'type': 'module', 'module': 'plugins.cassandra.checks.temporary_files_check', 'function': 'run_temporary_files_check'},
            {'type': 'module', 'module': 'plugins.cassandra.checks.commitlog_directory_size_check', 'function': 'run_commitlog_directory_size_check'},
            {'type': 'module', 'module': 'plugins.cassandra.checks.java_heap_usage_check', 'function': 'run_java_heap_usage_check'},
            {'type': 'module', 'module': 'plugins.cassandra.checks.gcstats_check', 'function': 'run_gcstats_check'},
        ]
    }
]

def get_default_report_definition(connector, settings):
    """Returns the report structure."""
    return REPORT_SECTIONS
