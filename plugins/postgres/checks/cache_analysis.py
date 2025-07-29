from plugins.postgres.utils.qrylib.cache_analysis_queries import get_cache_analysis_queries

def get_weight():
    """Returns the importance score for this module."""
    return 7 # High importance, as cache hit ratio is a key performance indicator.


def run_cache_analysis(connector, settings):
    """
    Analyzes PostgreSQL buffer cache usage and hit ratios to identify performance bottlenecks.
    """
    adoc_content = ["=== Cache Hit and Usage Analysis", "Analyzes PostgreSQL buffer cache usage and hit ratios to identify performance bottlenecks."]
    structured_data = {}

    try:
        # Get a dictionary of all necessary, version-aware queries at once
        cache_queries = get_cache_analysis_queries(connector)

        # --- Database Cache Hit Ratio ---
        adoc_content.append("\n==== Database Cache Hit Ratio")
        hit_ratio_query = cache_queries.get("database_cache_hit_ratio")
        params = {'database': settings.get('database')}
        formatted, raw = connector.execute_query(hit_ratio_query, params=params, return_raw=True)

        if "[ERROR]" in formatted:
            adoc_content.append(formatted)
        else:
            adoc_content.append(formatted)
        structured_data["database_cache_hit_ratio"] = {"status": "success", "data": raw}

        # --- Buffer and Checkpoint Statistics ---
        adoc_content.append("\n==== Buffer and Checkpoint Statistics")
        # Loop through the remaining buffer-related queries returned by the helper
        for data_key, query in cache_queries.items():
            if data_key == "database_cache_hit_ratio":
                continue # Already processed

            title = data_key.replace('_', ' ').title()
            formatted, raw = connector.execute_query(query, return_raw=True)
            
            adoc_content.append(f"===== {title}")
            if "[ERROR]" in formatted:
                adoc_content.append(formatted)
            else:
                adoc_content.append(formatted)
            structured_data[data_key] = {"status": "success", "data": raw}

        # --- Add Enhanced Analysis and Recommendations ---
        adoc_content.append("\n==== Cache Performance Best Practices")
        adoc_content.append("[IMPORTANT]\n====\n**Target Cache Hit Ratio: >99% for primary OLTP workloads.**\n\nA high cache hit ratio is critical for performance, as reading from memory is orders of magnitude faster than reading from disk.\n====\n")
        
        adoc_content.append("**Recommendations for Low Cache Hit Ratio:**\n")
        adoc_content.append("* **Increase `shared_buffers`**: This is the most direct way to improve caching. A common starting point is 25% of system RAM.\n")
        adoc_content.append("* **Optimize Queries**: Ensure queries are using indexes effectively to reduce the amount of data that needs to be read from disk.\n")
        adoc_content.append("* **Monitor `bgwriter` and `checkpointer`**: High activity in `buffers_checkpoint` or `buffers_backend` can indicate that `shared_buffers` is too small or that checkpoints are too frequent, forcing clean buffers out of the cache prematurely.\n")

    except Exception as e:
        error_msg = f"Failed during cache analysis: {e}"
        adoc_content.append(f"\n[ERROR]\n====\n{error_msg}\n====\n")
        structured_data["cache_analysis_error"] = {"status": "error", "details": str(e)}

    return "\n".join(adoc_content), structured_data
