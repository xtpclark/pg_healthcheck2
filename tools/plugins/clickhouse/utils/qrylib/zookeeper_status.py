# -*- coding: utf-8 -*-
# Copyright (c) 2023-2024, HealthCheck2 Team
# License: See LICENSE file

"""
ZooKeeper Status Queries for ClickHouse

This module contains SQL queries used to check ZooKeeper connection status
and health in ClickHouse.
"""

QUERY_ZOOKEEPER_STATUS = """
SELECT
    host,
    port,
    connected,
    last_error_code,
    last_error_message
FROM system.zookeeper
WHERE connected = 0 OR last_error_code != 0
ORDER BY host, port
"""
