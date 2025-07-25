def get_report_definition():
    """
    Returns the default report definition for the MySQL plugin.
    This defines the sections and modules to be executed.
    """
    return [
        {
            'title': 'MySQL Process List',
            'actions': [
                {
                    'type': 'module',
                    'module': 'plugins.mysql.checks.processlist_analysis',
                    'function': 'run_processlist_analysis'
                }
            ]
        },
        {
            'title': 'MySQL Connection Check',
            'actions': [
                {
                    'type': 'module',
                    'module': 'plugins.mysql.checks.connection_check',
                    'function': 'run_connection_check'
                }
            ]
        }
        # --- Add more sections and modules here as they are developed ---
    ]
