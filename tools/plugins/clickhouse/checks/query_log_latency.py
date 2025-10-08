from typing import Tuple, Dict, Any


def get_weight() -> int:
    return 7


def run_query_log_latency_check(connector: Any, settings: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    adoc_content = []
    structured_data = {
        'query_log_analysis': {'status': 'pending', 'data': {}},
        'recommendations': {'status': 'pending', 'details': []}
    }

    # Header for the report
    adoc_content.append('=== Query Log Latency Analysis')
    adoc_content.append('This check analyzes the ClickHouse system.query_log to identify tables with high insert or select latency.')

    # Step 1: Fetch query log data for latency analysis
    adoc_content.append('==== Query Log Data')
    try:
        from plugins.clickhouse.utils.qrylib.query_log_latency import QUERY_LOG_LATENCY
        formatted, raw = connector.execute_query(QUERY_LOG_LATENCY, return_raw=True)
        if raw:
            adoc_content.append(formatted)
            structured_data['query_log_analysis']['status'] = 'success'
            structured_data['query_log_analysis']['data'] = raw
        else:
            adoc_content.append('No data found in system.query_log for the specified time range.')
            structured_data['query_log_analysis']['status'] = 'warning'
            structured_data['query_log_analysis']['data'] = {'message': 'No data available'}
    except Exception as e:
        error_msg = f'Error fetching query log data: {str(e)}'
        adoc_content.append(error_msg)
        structured_data['query_log_analysis']['status'] = 'error'
        structured_data['query_log_analysis']['data'] = {'error': error_msg}

    # Step 2: Recommendations based on analysis
    adoc_content.append('==== Recommendations')
    try:
        if structured_data['query_log_analysis']['status'] == 'success' and structured_data['query_log_analysis']['data']:
            high_latency_threshold = settings.get('high_latency_threshold_ms', 1000)
            high_latency_queries = [
                entry for entry in structured_data['query_log_analysis']['data']
                if float(entry.get('query_duration_ms', 0)) > high_latency_threshold
            ]
            if high_latency_queries:
                adoc_content.append('[WARNING]')
                adoc_content.append('====')
                adoc_content.append(f'Detected {len(high_latency_queries)} queries with latency above {high_latency_threshold}ms.')
                adoc_content.append('Consider optimizing the queries or indexing the involved tables.')
                structured_data['recommendations']['status'] = 'warning'
                structured_data['recommendations']['details'] = high_latency_queries
            else:
                adoc_content.append('[NOTE]')
                adoc_content.append('====')
                adoc_content.append('No queries with significant latency detected.')
                structured_data['recommendations']['status'] = 'success'
                structured_data['recommendations']['details'] = {'message': 'No high latency issues'}
        else:
            adoc_content.append('Unable to provide recommendations due to missing or erroneous query log data.')
            structured_data['recommendations']['status'] = 'error'
            structured_data['recommendations']['details'] = {'message': 'Analysis incomplete'}
    except Exception as e:
        error_msg = f'Error generating recommendations: {str(e)}'
        adoc_content.append(error_msg)
        structured_data['recommendations']['status'] = 'error'
        structured_data['recommendations']['details'] = {'error': error_msg}

    return "\n".join(adoc_content), structured_data
