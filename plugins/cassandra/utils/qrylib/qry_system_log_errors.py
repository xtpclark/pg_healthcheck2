"""System log error queries for Cassandra (shell commands)."""

__all__ = [
    'get_system_log_errors_query'
]

import json

def get_system_log_errors_query(connector, log_lines=1000):
    """
    Returns JSON request for scanning Cassandra system.log for errors.
    
    Args:
        connector: Cassandra connector instance
        log_lines: Number of lines to tail (default: 1000)
    
    Returns:
        str: JSON string with operation and command
    """
    command = f"tail -n {log_lines} /var/log/cassandra/system.log 2>/dev/null | grep -i 'error\|exception\|warn' || true"
    return json.dumps({
        "operation": "shell",
        "command": command
    })