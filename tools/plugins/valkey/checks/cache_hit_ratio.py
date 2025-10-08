# -*- coding: utf-8 -*-
# cache_hit_ratio.py: Valkey cache hit/miss ratio check for cache efficiency


def get_weight():
    return 7


def run_cache_hit_ratio(connector, settings):
    """
    Check the cache hit/miss ratio for Valkey when used as a cache.
    Returns an AsciiDoc formatted report and structured data dictionary.
    """
    adoc_content = []
    structured_data = {
        'cache_hit_ratio': {
            'status': 'unknown',
            'data': {}
        }
    }

    adoc_content.append('=== Cache Hit/Miss Ratio Check')
    adoc_content.append('This check evaluates the cache efficiency of Valkey by analyzing the hit/miss ratio.')

    try:
        # Fetch INFO STATS for hit/miss data
        info_stats_cmd = 'INFO STATS'
        formatted, raw = connector.execute_query(info_stats_cmd, return_raw=True)
        if raw and isinstance(raw, dict):
            keyspace_hits = int(raw.get('keyspace_hits', 0))
            keyspace_misses = int(raw.get('keyspace_misses', 0))
            total_requests = keyspace_hits + keyspace_misses

            if total_requests > 0:
                hit_ratio = (keyspace_hits / total_requests) * 100
                adoc_content.append('==== Cache Efficiency Analysis')
                adoc_content.append(f'- Total Requests: {total_requests}')
                adoc_content.append(f'- Cache Hits: {keyspace_hits}')
                adoc_content.append(f'- Cache Misses: {keyspace_misses}')
                adoc_content.append(f'- Hit Ratio: {hit_ratio:.2f}%')

                structured_data['cache_hit_ratio']['status'] = 'success'
                structured_data['cache_hit_ratio']['data'] = {
                    'total_requests': total_requests,
                    'hits': keyspace_hits,
                    'misses': keyspace_misses,
                    'hit_ratio_percent': hit_ratio
                }

                adoc_content.append('==== Recommendations')
                if hit_ratio < 70:
                    adoc_content.append('[WARNING]')
                    adoc_content.append('====')
                    adoc_content.append('The cache hit ratio is below 70%. Consider adjusting your caching strategy or increasing cache size.')
                else:
                    adoc_content.append('[TIP]')
                    adoc_content.append('====')
                    adoc_content.append('The cache hit ratio is acceptable. Monitor periodically for changes in workload.')
            else:
                adoc_content.append('==== Analysis')
                adoc_content.append('No cache requests detected. Unable to calculate hit/miss ratio.')
                structured_data['cache_hit_ratio']['status'] = 'error'
                structured_data['cache_hit_ratio']['details'] = 'No cache requests detected.'
        else:
            adoc_content.append('==== Error')
            adoc_content.append('Failed to retrieve cache statistics.')
            structured_data['cache_hit_ratio']['status'] = 'error'
            structured_data['cache_hit_ratio']['details'] = 'Failed to retrieve data.'
    except Exception as e:
        adoc_content.append('==== Error')
        adoc_content.append(f'Unexpected error while checking cache hit ratio: {str(e)}')
        structured_data['cache_hit_ratio']['status'] = 'error'
        structured_data['cache_hit_ratio']['details'] = str(e)

    return '\n'.join(adoc_content), structured_data
