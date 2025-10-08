from typing import Dict, Tuple, Any


def get_weight() -> int:
    return 7


def run_system_events_check(connector, settings: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    adoc_content = []
    structured_data = {
        'system_events_analysis': {
            'status': 'pending',
            'data': {},
            'details': ''
        }
    }

    # Header for the report
    adoc_content.append('=== System Events Check')
    adoc_content.append('This check analyzes the system.events table for errors or warnings to detect potential issues or anomalies.')

    # Query system.events for errors or warnings
    adoc_content.append('==== Events Analysis')
    try:
        from plugins.clickhouse.utils.qrylib import system_events_query
        formatted, raw = connector.execute_query(system_events_query, return_raw=True)
        structured_data['system_events_analysis']['status'] = 'success'
        structured_data['system_events_analysis']['data'] = raw
        if raw and len(raw) > 0:
            adoc_content.append('The following errors or warnings were found in the system.events table:')
            adoc_content.append(formatted)
            adoc_content.append('[WARNING]')
            adoc_content.append('====')
            adoc_content.append('Potential issues detected in system events. Review the listed events for errors or warnings that may indicate underlying problems.') 
        else:
            adoc_content.append('No errors or warnings were found in the system.events table.')
    except Exception as e:
        structured_data['system_events_analysis']['status'] = 'error'
        structured_data['system_events_analysis']['details'] = str(e)
        adoc_content.append('An error occurred while querying the system.events table.')
        adoc_content.append('[WARNING]')
        adoc_content.append('====')
        adoc_content.append(f'Error details: {str(e)}')

    # Recommendations
    adoc_content.append('==== Recommendations')
    if structured_data['system_events_analysis']['status'] == 'success' and structured_data['system_events_analysis']['data']:
        adoc_content.append('- Investigate any errors or warnings listed in the system.events table.')
        adoc_content.append('- Consider enabling additional logging or monitoring if recurring issues are detected.')
    else:
        adoc_content.append('- No specific recommendations at this time.')

    return '\n'.join(adoc_content), structured_data
