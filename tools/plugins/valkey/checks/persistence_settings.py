def get_weight():
    return 7

def run_persistence_check(connector, settings):
    adoc_content = []
    structured_data = {
        'persistence_settings': {'status': 'unknown', 'data': {}},
        'recommendations': {'status': 'unknown', 'details': ''}
    }

    adoc_content.append('=== Persistence Settings and Status')
    adoc_content.append('This check evaluates the persistence settings (RDB and AOF) to ensure data durability in Valkey.')

    # Check RDB and AOF configuration
    adoc_content.append('==== Configuration Analysis')
    try:
        rdb_info_cmd = 'CONFIG GET save'
        rdb_info_formatted, rdb_info_raw = connector.execute_query(rdb_info_cmd, return_raw=True)
        structured_data['persistence_settings']['data']['rdb_config'] = rdb_info_raw
        adoc_content.append(rdb_info_formatted if rdb_info_formatted else 'No RDB configuration data available.')

        aof_info_cmd = 'CONFIG GET appendonly'
        aof_info_formatted, aof_info_raw = connector.execute_query(aof_info_cmd, return_raw=True)
        structured_data['persistence_settings']['data']['aof_config'] = aof_info_raw
        adoc_content.append(aof_info_formatted if aof_info_formatted else 'No AOF configuration data available.')

        structured_data['persistence_settings']['status'] = 'success'
    except Exception as e:
        adoc_content.append(f'Error retrieving persistence configuration: {str(e)}')
        structured_data['persistence_settings']['status'] = 'error'
        structured_data['persistence_settings']['data']['error'] = str(e)

    # Check persistence status
    adoc_content.append('==== Persistence Status')
    try:
        persistence_status_cmd = 'INFO PERSISTENCE'
        status_formatted, status_raw = connector.execute_query(persistence_status_cmd, return_raw=True)
        structured_data['persistence_settings']['data']['status'] = status_raw
        adoc_content.append(status_formatted if status_formatted else 'No persistence status data available.')

        # Analyze if persistence mechanisms are active
        rdb_enabled = any('save' in key for key in rdb_info_raw) and rdb_info_raw.get('save', '') != ''
        aof_enabled = 'yes' in aof_info_raw.get('appendonly', '').lower()

        if not rdb_enabled and not aof_enabled:
            adoc_content.append('[CRITICAL]')
            adoc_content.append('====')
            adoc_content.append('Neither RDB nor AOF persistence is enabled. Data will be lost on restart.')
            structured_data['recommendations']['status'] = 'critical'
            structured_data['recommendations']['details'] = 'Enable at least one persistence mechanism (RDB or AOF).'
        elif not aof_enabled:
            adoc_content.append('[WARNING]')
            adoc_content.append('====')
            adoc_content.append('AOF is not enabled. Data changes between RDB snapshots may be lost on crash.')
            structured_data['recommendations']['status'] = 'warning'
            structured_data['recommendations']['details'] = 'Consider enabling AOF for better durability.'
        else:
            adoc_content.append('[NOTE]')
            adoc_content.append('====')
            adoc_content.append('At least one persistence mechanism is enabled. Data durability is likely ensured.')
            structured_data['recommendations']['status'] = 'success'
            structured_data['recommendations']['details'] = 'Persistence settings are adequate.'
    except Exception as e:
        adoc_content.append(f'Error retrieving persistence status: {str(e)}')
        structured_data['persistence_settings']['status'] = 'error'
        structured_data['persistence_settings']['data']['status_error'] = str(e)

    adoc_content.append('==== Recommendations')
    adoc_content.append('- Ensure at least one persistence mechanism (RDB or AOF) is enabled for data durability.')
    adoc_content.append('- For mission-critical applications, enable AOF with frequent fsync settings.')

    return "\n".join(adoc_content), structured_data