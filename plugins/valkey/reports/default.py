def get_report_definition():
    """
    Returns the default report definition for the Valkey plugin.
    """
    return [
        {
            'title': 'Valkey Server Status',
            'actions': [
                {
                    'type': 'module',
                    'module': 'plugins.valkey.checks.ping_check',
                    'function': 'run_ping_check'
                },
                {
                    'type': 'module',
                    'module': 'plugins.valkey.checks.info_check',
                    'function': 'run_info_check'
                }
            ]
        }
    ]
