# plugins/postgres/utils/qrylib/top_io_queries.py

def get_top_io_queries_query(connector):
    """
    Returns a query to find top queries by I/O wait time.
    Dynamically adjusts column names based on PostgreSQL version (PG17+ vs. legacy)
    to ensure compatibility.
    """
    # Base query structure
    base_query = """
    SELECT
        substring(regexp_replace(query, '\\s+', ' ', 'g') for 120) AS short_query,
    --    substring(query for 120) AS short_query,
        calls,
        total_exec_time,
        {io_time_columns}  -- This will be replaced
    FROM
        pg_stat_statements
    ORDER BY
        total_io_time DESC
    LIMIT %s;
    """

    # Default to 0 for safety, though the check module should prevent this query
    # from running if I/O timings aren't available at all.
    io_time_calculation = "0 AS total_io_time"

    # Check for PG17+ style I/O columns first
    if connector.has_pgstat_new_io_time:
        io_time_calculation = "(shared_blk_read_time + shared_blk_write_time + local_blk_read_time + local_blk_write_time + temp_blk_read_time + temp_blk_write_time) AS total_io_time"
    # Fallback to legacy style columns
    elif connector.has_pgstat_legacy_io_time:
        io_time_calculation = "(blk_read_time + blk_write_time) AS total_io_time"
    
    # Inject the correct column calculation into the base query
    return base_query.format(io_time_columns=io_time_calculation)
