from typing import Dict, Tuple, Any


def get_weight() -> int:
    return 7


def run_active_queries_check(connector: Any, settings: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    adoc_content = []
    structured_data = {
        'active_queries': {
            'status': 'success',
            'data': {}
        }
    }

    adoc_content.append('=== Active Queries Check')
    adoc_content.append('This check identifies active queries in ClickHouse using system.queries to detect slow or stuck queries.\n')

    try:
        adoc_content.append('==== Query Analysis')
        query = """
SELECT
    query_id,
    user,
    query,
    elapsed,
    read_rows,
    read_bytes,
    total_rows_approx,
    memory_usage
FROM
    system.queries
WHERE
    is_initial_query = 1
ORDER BY
    elapsed DESC
LIMIT 10
"""
        formatted, raw = connector.execute_query(query, return_raw=True)
        structured_data['active_queries']['data'] = raw

        if raw and len(raw) > 0:
            adoc_content.append('The following active queries were found (top 10 by elapsed time):')
            adoc_content.append(formatted)
            slow_queries = [q for q in raw if q.get('elapsed', 0) > 10.0]
            if slow_queries:
                adoc_content.append('[WARNING]')
                adoc_content.append('====')
                adoc_content.append('Slow queries detected (elapsed time > 10 seconds). Investigate these queries for potential performance issues.')
        else:
            adoc_content.append('No active queries found at this time.')
    except Exception as e:
        adoc_content.append('[CRITICAL]')
        adoc_content.append('====')
        adoc_content.append(f'Failed to retrieve active queries: {str(e)}')
        structured_data['active_queries']['status'] = 'error'
        structured_data['active_queries']['details'] = str(e)

    try:
        adoc_content.append('==== Recommendations')
        adoc_content.append('- Monitor long-running queries for optimization opportunities.')
        adoc_content.append('- Consider setting query timeouts if stuck queries are a recurring issue.')
        adoc_content.append('[TIP]')
        adoc_content.append('====')
        adoc_content.append('Use ClickHouse\'s query log for deeper historical analysis of query performance.')
    except Exception as e:
        adoc_content.append(f'Error in recommendations section: {str(e)}')

    return "\n".join(adoc_content), structured_data
