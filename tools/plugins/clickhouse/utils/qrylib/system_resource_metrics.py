# -*- coding: utf-8 -*-
# Query for system resource metrics in ClickHouse

QUERY = """
SELECT
    metric,
    value,
    description
FROM system.metrics
WHERE metric IN ('CPUUsage', 'MemoryUsage', 'DiskIO')
ORDER BY metric
"""
