def get_weight():
    return 5

def run_connected_clients(connector, settings):
    adoc_content = []
    structured_data = {
        'client_connections': {'status': 'unknown', 'data': {}}
    }

    adoc_content.append('=== Connected Clients Check')
    adoc_content.append('This check inspects the number of connected clients and identifies potential connection issues.')

    try:
        result = connector.execute_command('INFO CLIENTS')
        clients_data = {}
        for line in result.splitlines():
            if '=' in line:
                key, value = line.split('=', 1)
                clients_data[key.strip()] = value.strip()
        
        connected_clients = int(clients_data.get('connected_clients', 0))
        max_clients = int(clients_data.get('maxclients', 0))
        
        structured_data['client_connections']['status'] = 'success'
        structured_data['client_connections']['data'] = {
            'connected_clients': connected_clients,
            'max_clients': max_clients
        }

        adoc_content.append('==== Analysis')
        adoc_content.append(f'Connected Clients: {connected_clients}')
        adoc_content.append(f'Maximum Clients Allowed: {max_clients}')

        if connected_clients >= max_clients * 0.9:
            adoc_content.append('[WARNING]')
            adoc_content.append('====')
            adoc_content.append('The number of connected clients is approaching the maximum limit. Consider increasing the maxclients setting or reducing the number of connections.')
        else:
            adoc_content.append('[NOTE]')
            adoc_content.append('====')
            adoc_content.append('The number of connected clients is within a safe range.')

    except Exception as e:
        structured_data['client_connections']['status'] = 'error'
        structured_data['client_connections']['details'] = str(e)
        adoc_content.append('[CRITICAL]')
        adoc_content.append('====')
        adoc_content.append(f'Failed to retrieve client connection data: {str(e)}')

    adoc_content.append('==== Recommendations')
    adoc_content.append('- Monitor the number of connected clients regularly.')
    adoc_content.append('- Adjust the `maxclients` configuration if necessary.')

    return '\n'.join(adoc_content), structured_data