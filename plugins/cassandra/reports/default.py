def get_report_definition():
    """
    Returns the default report definition for the Cassandra plugin.
    This defines the sections and modules to be executed.
    """
    return [
        {
            'title': 'Cassandra Cluster Overview',
            'actions': [
                {
                    'type': 'module',
                    'module': 'plugins.cassandra.checks.nodetool_status_check',
                    'function': 'run_nodetool_status_check'
                }
            ]
        }
        # --- Add more sections and modules here as they are developed ---
    ]
