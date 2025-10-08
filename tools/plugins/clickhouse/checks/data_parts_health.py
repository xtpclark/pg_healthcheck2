from typing import Dict, Tuple, Any


def get_weight() -> int:
    return 7


def run_data_parts_health(connector: Any, settings: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    adoc_content = []
    structured_data = {
        'data_parts_health': {
            'status': 'success',
            'data': {},
            'issues': []
        }
    }

    adoc_content.append('=== Data Parts Health Check')
    adoc_content.append('This check inspects the health of data parts in ClickHouse using the system.parts table to detect unmerged or outdated parts that may affect data consistency.')
    adoc_content.append('')

    # Check for unmerged or outdated parts
    adoc_content.append('==== Unmerged or Outdated Parts Analysis')
    try:
        from ..utils.qrylib.data_parts_health import UNMERGED_PARTS_QUERY
        formatted, raw = connector.execute_query(UNMERGED_PARTS_QUERY, return_raw=True)
        structured_data['data_parts_health']['data']['unmerged_parts'] = raw
        if raw and len(raw) > 0:
            adoc_content.append('The following data parts appear to be unmerged or outdated, which may indicate issues with data consistency or merge operations:')
            adoc_content.append(formatted)
            adoc_content.append('')
            adoc_content.append('[WARNING]')
            adoc_content.append('====')
            adoc_content.append('Unmerged or outdated data parts can lead to inefficiencies or data inconsistency. Consider investigating merge processes or forcing merges if necessary.')
            structured_data['data_parts_health']['issues'].append('Unmerged or outdated parts detected.')
        else:
            adoc_content.append('No unmerged or outdated data parts detected. All parts appear to be in a healthy state.')
    except Exception as e:
        adoc_content.append('Error retrieving data parts health information: ' + str(e))
        structured_data['data_parts_health']['status'] = 'error'
        structured_data['data_parts_health']['issues'].append(str(e))
        adoc_content.append('[CRITICAL]')
        adoc_content.append('====')
        adoc_content.append('Failed to analyze data parts health due to an error. This may prevent identifying critical consistency issues.')

    adoc_content.append('')
    adoc_content.append('==== Recommendations')
    adoc_content.append('- Regularly monitor merge operations to prevent accumulation of unmerged parts.')
    adoc_content.append('- If unmerged parts persist, consider manually triggering merges or checking for configuration issues.')
    adoc_content.append('[TIP]')
    adoc_content.append('====')
    adoc_content.append('Use the `OPTIMIZE TABLE` command cautiously to force merges if necessary, but be aware of potential performance impacts.')

    return '\n'.join(adoc_content), structured_data
