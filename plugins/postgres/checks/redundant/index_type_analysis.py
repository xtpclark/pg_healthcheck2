# consolidated idx_ check modules.
def get_weight():
    """Returns the importance score for this module."""
    return 1 # Core configuration, highest importance

def run_index_type_analysis(connector, settings):
    """
    Analyzes the usage of specialized index types (GIN, GIST, BRIN, etc.)
    and provides context on their typical use cases.
    """
    adoc_content = ["=== Specialized Index Type Analysis", "Provides a summary of non-B-Tree indexes and their use cases. Using the right index type for the right workload is key to performance.\n"]
    structured_data = {}

    # A single query to find all non-standard index types
    index_type_query = """
    SELECT
        n.nspname AS schema_name,
        c.relname AS table_name,
        i.relname AS index_name,
        am.amname AS index_type,
        pg_size_pretty(pg_relation_size(i.oid)) as index_size
    FROM pg_class c
    JOIN pg_index ix ON ix.indrelid = c.oid
    JOIN pg_class i ON i.oid = ix.indexrelid
    JOIN pg_am am ON am.oid = i.relam
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE am.amname NOT IN ('btree')
      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
    ORDER BY am.amname, n.nspname, c.relname, i.relname;
    """

    # Descriptions for each specialized index type
    INDEX_TYPE_DESCRIPTIONS = {
        'gin': 'Generalized Inverted Index (GIN) is ideal for indexing composite values where elements can appear multiple times, such as text in `tsvector`, data in `jsonb`, or elements in an array.',
        'gist': 'Generalized Search Tree (GiST) is a versatile index for various kinds of data, commonly used for geometric data types and full-text search.',
        'brin': 'Block Range Index (BRIN) is designed for very large tables where data has a natural correlation with its physical storage order, like timestamp columns. They are very small and efficient.',
        'hash': 'Hash indexes are only useful for simple equality comparisons (`=`). They are not WAL-logged and do not replicate, so their use is generally discouraged in favor of B-Tree indexes.',
        'spgist': 'Space-Partitioned GiST is designed for certain types of non-uniformly distributed data, such as phone numbers or other partitioned data structures.'
    }

    try:
        if settings.get('show_qry') == 'true':
            adoc_content.append("Specialized index analysis query:")
            adoc_content.append("[,sql]\n----")
            adoc_content.append(index_type_query)
            adoc_content.append("----")

        formatted_result, raw_result = connector.execute_query(index_type_query, return_raw=True)

        if "[ERROR]" in formatted_result:
            adoc_content.append(formatted_result)
            structured_data["specialized_indexes"] = {"status": "error", "details": raw_result}
        elif not raw_result:
            adoc_content.append("[NOTE]\n====\nNo specialized (non-B-Tree) indexes were found in user schemas. All indexes are standard B-Trees.\n====\n")
            structured_data["specialized_indexes"] = {"status": "success", "data": []}
        else:
            adoc_content.append("[IMPORTANT]\n====\nSpecialized indexes are used in your database. Review the summaries below to ensure they are being used appropriately for their intended workloads.\n====\n")
            
            indexes_by_type = {}
            for row in raw_result:
                idx_type = row['index_type']
                if idx_type not in indexes_by_type:
                    indexes_by_type[idx_type] = []
                indexes_by_type[idx_type].append(row)

            for idx_type, indexes in indexes_by_type.items():
                adoc_content.append(f"\n==== {idx_type.upper()} Indexes")
                adoc_content.append(f"_{INDEX_TYPE_DESCRIPTIONS.get(idx_type, 'No description available.')}_")
                
                table = ['[cols="2,2,2,1",options="header"]', '|===', '| Schema | Table | Index Name | Size']
                for index in indexes:
                    table.append(f"| {index['schema_name']} | {index['table_name']} | `{index['index_name']}` | {index['index_size']}")
                table.append('|===')
                adoc_content.append('\n'.join(table))
            
            structured_data["specialized_indexes"] = {"status": "success", "data": indexes_by_type}

    except Exception as e:
        error_msg = f"Failed during specialized index analysis: {e}"
        adoc_content.append(f"[ERROR]\n====\n{error_msg}\n====\n")
        structured_data["specialized_indexes"] = {"status": "error", "details": error_msg}

    return "\n".join(adoc_content), structured_data
