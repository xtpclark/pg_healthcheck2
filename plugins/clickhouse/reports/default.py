def get_report_definition():
    """
    Returns the default report definition for the ClickHouse plugin.
    """
    return [
        {
            'title': 'ClickHouse System Metrics',
            'actions': [
                {
                    'type': 'module',
                    'module': 'plugins.clickhouse.checks.system_metrics_check',
                    'function': 'run_system_metrics_check'
                }
            ]
        }
    ]
