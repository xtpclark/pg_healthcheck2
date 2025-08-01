from plugins.postgres.utils.qrylib.index_health import (
    get_unused_indexes_query,
    get_duplicate_indexes_query,
    get_invalid_indexes_query,
    get_specialized_indexes_details_query
)

def get_weight():
    """Returns the importance score for this module."""
    # Index health is crucial for both read and write performance.
    return 8

def run_index_health_analysis(connector, settings):
    """
    Performs a comprehensive analysis of index health, identifying unused,
    duplicate, invalid, and specialized indexes.
    """
    adoc_content = ["=== Index Health and Maintenance", "Provides a consolidated to-do list for index maintenance, identifying issues that can consume resources and slow down write operations.\n"]
    structured_data = {}
    params = {'limit': settings.get('row_limit', 10)}

    # --- Unused Indexes ---
    try:
        adoc_content.append("==== Unused Indexes")
        query = get_unused_indexes_query(connector)
        formatted, raw = connector.execute_query(query, params=params, return_raw=True)
        if "[ERROR]" in formatted:
            adoc_content.append(formatted)
        elif not raw:
            adoc_content.append("[NOTE]\n====\nNo significantly large and unused indexes found.\n====\n")
        else:
            adoc_content.append("[IMPORTANT]\n====\n**CRITICAL CONSIDERATION FOR READ REPLICAS:** Indexes that appear 'unused' on the primary may be heavily used on read replicas. **Verify usage on ALL replicas before dropping any index.**\n====\n")
            adoc_content.append(formatted)
        structured_data["unused_indexes"] = {"status": "success", "data": raw}
    except Exception as e:
        adoc_content.append(f"[ERROR]\n====\nCould not analyze unused indexes: {e}\n====\n")

    # --- Duplicate Indexes ---
    try:
        adoc_content.append("\n==== Duplicate Indexes")
        query = get_duplicate_indexes_query(connector)
        formatted, raw = connector.execute_query(query, params=params, return_raw=True)
        if "[ERROR]" in formatted:
            adoc_content.append(formatted)
        elif not raw:
            adoc_content.append("[NOTE]\n====\nNo duplicate indexes found.\n====\n")
        else:
            adoc_content.append("[IMPORTANT]\n====\nDuplicate indexes provide no benefit and double write overhead. Safely drop one index from each group listed.\n====\n")
            adoc_content.append(formatted)
        structured_data["duplicate_indexes"] = {"status": "success", "data": raw}
    except Exception as e:
        adoc_content.append(f"[ERROR]\n====\nCould not analyze duplicate indexes: {e}\n====\n")

    # --- Invalid Indexes ---
    try:
        adoc_content.append("\n==== Invalid Indexes")
        query = get_invalid_indexes_query(connector)
        formatted, raw = connector.execute_query(query, return_raw=True)
        if "[ERROR]" in formatted:
            adoc_content.append(formatted)
        elif not raw:
            adoc_content.append("[NOTE]\n====\nNo invalid indexes found.\n====\n")
        else:
            adoc_content.append("[IMPORTANT]\n====\nInvalid indexes are unusable and waste space. Rebuild them with `REINDEX INDEX CONCURRENTLY <index_name>;` to avoid locking.\n====\n")
            adoc_content.append(formatted)
        structured_data["invalid_indexes"] = {"status": "success", "data": raw}
    except Exception as e:
        adoc_content.append(f"[ERROR]\n====\nCould not analyze invalid indexes: {e}\n====\n")

    # --- Specialized Index Types Analysis (MERGED) ---
    INDEX_TYPE_DESCRIPTIONS = {
        'gin': 'Generalized Inverted Index (GIN) is ideal for indexing composite values where elements can appear multiple times, such as text in `tsvector`, data in `jsonb`, or elements in an array.',
        'gist': 'Generalized Search Tree (GiST) is a versatile index for various kinds of data, commonly used for geometric data types and full-text search.',
        'brin': 'Block Range Index (BRIN) is designed for very large tables where data has a natural correlation with its physical storage order, like timestamp columns. They are very small and efficient.',
        'hash': 'Hash indexes are only useful for simple equality comparisons (`=`). They are not WAL-logged and do not replicate, so their use is generally discouraged in favor of B-Tree indexes.',
        'spgist': 'Space-Partitioned GiST is designed for certain types of non-uniformly distributed data, such as phone numbers or other partitioned data structures.'
    }
    try:
        adoc_content.append("\n==== Specialized Index Types Analysis")
        details_query = get_specialized_indexes_details_query(connector)
        formatted_result, raw_result = connector.execute_query(details_query, return_raw=True)

        if "[ERROR]" in formatted_result:
            adoc_content.append(formatted_result)
        elif not raw_result:
            adoc_content.append("[NOTE]\n====\nNo specialized (non-B-Tree) indexes were found in user schemas.\n====\n")
        else:
            adoc_content.append("[IMPORTANT]\n====\nSpecialized indexes are used in your database. Review the summaries below to ensure they are being used appropriately for their intended workloads.\n====\n")
            
            indexes_by_type = {}
            for row in raw_result:
                idx_type = row['index_type']
                if idx_type not in indexes_by_type:
                    indexes_by_type[idx_type] = []
                indexes_by_type[idx_type].append(row)

            for idx_type, indexes in indexes_by_type.items():
                adoc_content.append(f"\n===== {idx_type.upper()} Indexes")
                adoc_content.append(f"_{INDEX_TYPE_DESCRIPTIONS.get(idx_type, 'No description available.')}_")
                
                table = ['[cols="2,2,2,1",options="header"]', '|===', '| Schema | Table | Index Name | Size']
                for index in indexes:
                    table.append(f"| {index['schema_name']} | {index['table_name']} | `{index['index_name']}` | {index['index_size']}")
                table.append('|===')
                adoc_content.append('\n'.join(table))
            
        structured_data["specialized_indexes_details"] = {"status": "success", "data": raw_result}

    except Exception as e:
        adoc_content.append(f"[ERROR]\n====\nCould not analyze specialized indexes: {e}\n====\n")

    return "\n".join(adoc_content), structured_data
