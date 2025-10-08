# -*- coding: utf-8 -*-
# SQL Queries for High Memory Usage in ClickHouse

GET_HIGH_MEMORY_QUERIES = """
SELECT 
    query_id,
    query,
    memory_usage,
    peak_memory_usage
FROM system.processes
WHERE memory_usage > 1000000000 -- 1GB threshold
ORDER BY memory_usage DESC
LIMIT 10;
"""
