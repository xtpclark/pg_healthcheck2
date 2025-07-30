"""
MySQL Version Compatibility Module

This module provides version-aware functionality to construct the correct
SQL queries based on the version of the connected MySQL/MariaDB database.
It relies on the version information fetched by the MySQLConnector.
"""

def get_processlist_query(connector):
    """
    Returns the most efficient query to get the current process list.
    MySQL 8.0 introduced performance_schema.processlist which is preferred.
    """
    # Use the reliable, pre-fetched version info from the connector
    if connector.version_info.get('is_mysql8_or_newer'):
        # The modern, more efficient query for MySQL 8+
        return """
            SELECT
                ID, USER, HOST, DB, COMMAND, TIME, STATE, INFO
            FROM performance_schema.processlist
            ORDER BY TIME DESC;
        """
    else:
        # The legacy query for older MySQL versions and MariaDB
        return """
            SELECT
                ID, USER, HOST, DB, COMMAND, TIME, STATE, INFO
            FROM information_schema.processlist
            ORDER BY TIME DESC;
        """

# --- Add other version-aware query functions here as you build new checks ---
# Example: A function for checking InnoDB buffer pool status
#
# def get_buffer_pool_stats_query(connector):
#     if connector.version_info.get('is_mysql8_or_newer'):
#         return "SELECT * FROM performance_schema.innodb_buffer_pool_stats;"
#     else:
#         return "SHOW ENGINE INNODB STATUS;"
