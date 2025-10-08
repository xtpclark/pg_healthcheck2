def get_weight():
    return 8

def run_replication_status(connector, settings):
    adoc_content = []
    structured_data = {
        'replication_status': {
            'status': 'unknown',
            'data': {}
        }
    }

    adoc_content.append('=== Replication Status and Lag Check')
    adoc_content.append('This check verifies the replication status and lag for any configured replicas in the Valkey instance.')

    try:
        # Fetch replication info using INFO REPLICATION command
        replication_info = connector.execute_command('INFO REPLICATION')
        adoc_content.append('==== Replication Information')
        if replication_info:
            structured_data['replication_status']['status'] = 'success'
            structured_data['replication_status']['data'] = replication_info

            # Format the replication info for the report
            adoc_content.append('|===')
            adoc_content.append('| Key | Value')
            for key, value in replication_info.items():
                adoc_content.append(f'| {key} | {value}')
            adoc_content.append('|===')

            # Analyze replication status and lag
            adoc_content.append('==== Analysis')
            if replication_info.get('role') == 'master':
                adoc_content.append('This instance is configured as a master.')
                connected_slaves = int(replication_info.get('connected_slaves', 0))
                if connected_slaves > 0:
                    adoc_content.append(f'There are {connected_slaves} connected replicas.')
                    for i in range(connected_slaves):
                        slave_key = f'slave{i}'
                        slave_info = replication_info.get(slave_key, '')
                        if slave_info:
                            adoc_content.append(f'- Replica {i+1}: {slave_info}')
                            lag = int(replication_info.get(f'{slave_key}_repl_offset', 0)) - int(replication_info.get('master_repl_offset', 0))
                            adoc_content.append(f'  - Replication Lag: {lag} operations')
                            if lag > 1000:
                                adoc_content.append('[WARNING]')
                                adoc_content.append('====')
                                adoc_content.append(f'The replication lag for Replica {i+1} is high ({lag} operations). Consider investigating network or performance issues.')
                                adoc_content.append('====')
                else:
                    adoc_content.append('No replicas are currently connected to this master.')
                    adoc_content.append('[NOTE]')
                    adoc_content.append('====')
                    adoc_content.append('If replicas are expected, ensure they are properly configured and connected.')
                    adoc_content.append('====')
            elif replication_info.get('role') == 'slave':
                adoc_content.append('This instance is configured as a replica.')
                master_link_status = replication_info.get('master_link_status', 'down')
                if master_link_status == 'up':
                    adoc_content.append('The link to the master is up.')
                else:
                    adoc_content.append('[CRITICAL]')
                    adoc_content.append('====')
                    adoc_content.append('The link to the master is down. Replication is not occurring. Investigate connectivity or configuration issues immediately.')
                    adoc_content.append('====')
                lag = int(replication_info.get('master_repl_offset', 0)) - int(replication_info.get('slave_repl_offset', 0))
                adoc_content.append(f'- Replication Lag: {lag} operations')
                if lag > 1000:
                    adoc_content.append('[WARNING]')
                    adoc_content.append('====')
                    adoc_content.append(f'The replication lag is high ({lag} operations). Consider investigating network or performance issues.')
                    adoc_content.append('====')
            else:
                adoc_content.append('This instance does not appear to be configured for replication (neither master nor replica).')
        else:
            adoc_content.append('[WARNING]')
            adoc_content.append('====')
            adoc_content.append('Unable to retrieve replication information. Ensure the INFO REPLICATION command is supported and accessible.')
            adoc_content.append('====')
            structured_data['replication_status']['status'] = 'error'
            structured_data['replication_status']['data'] = {'error': 'No replication data returned'}
    except Exception as e:
        adoc_content.append('[CRITICAL]')
        adoc_content.append('====')
        adoc_content.append(f'Failed to execute replication status check: {str(e)}')
        adoc_content.append('====')
        structured_data['replication_status']['status'] = 'error'
        structured_data['replication_status']['data'] = {'error': str(e)}

    adoc_content.append('==== Recommendations')
    adoc_content.append('- Ensure replication settings are correctly configured if replication is desired.')
    adoc_content.append('- Monitor replication lag regularly to prevent data inconsistency.')
    adoc_content.append('- Investigate network or resource bottlenecks if lag or connection issues are observed.')

    return '\n'.join(adoc_content), structured_data