"""
Common helper functions for health checks.

Provides utilities that are useful across all database health checks,
regardless of the underlying database technology.
"""

import logging
from typing import Tuple, Dict, Any, Optional, List

logger = logging.getLogger(__name__)


def require_ssh(connector, operation_name: Optional[str] = None) -> Tuple[bool, str, Dict]:
    """
    Check if SSH is available and return appropriate response if not.
    
    Args:
        connector: Database connector with SSHSupportMixin
        operation_name: Optional description of what SSH is needed for
    
    Returns:
        tuple: (ssh_available: bool, skip_message: str, skip_data: dict)
        
    Example:
        ssh_ok, skip_msg, skip_data = require_ssh(connector, "nodetool commands")
        if not ssh_ok:
            return skip_msg, skip_data
        # Continue with check...
    """
    if hasattr(connector, 'has_ssh_support') and connector.has_ssh_support():
        return True, "", {}
    
    # SSH not available
    if hasattr(connector, 'get_ssh_skip_message'):
        skip_msg, skip_data = connector.get_ssh_skip_message(operation_name)
    else:
        # Fallback if connector doesn't have the mixin
        skip_msg = (
            "[IMPORTANT]\n"
            "====\n"
            f"This check requires SSH access{' for ' + operation_name if operation_name else ''}.\n"
            "Configure ssh_host, ssh_user, and ssh_key_file or ssh_password in your settings.\n"
            "====\n"
        )
        skip_data = {
            "status": "skipped",
            "reason": "SSH not configured"
        }
    
    return False, skip_msg, skip_data


def format_check_header(check_name: str, description: str, requires_ssh: bool = False) -> List[str]:
    """
    Generate a standard check header with title and description.
    
    Args:
        check_name: Name of the check
        description: Description of what the check does
        requires_ssh: If True, adds SSH requirement note
    
    Returns:
        list: Lines of AsciiDoc content
    """
    header = [
        f"=== {check_name}",
        "",
        description
    ]
    
    if requires_ssh:
        header.extend([
            "",
            "[NOTE]",
            "====",
            "This check requires SSH access to the database server.",
            "===="
        ])
    
    return header


def format_recommendations(recommendations: List[str]) -> List[str]:
    """
    Format a list of recommendations in standard AsciiDoc format.
    
    Args:
        recommendations: List of recommendation strings
    
    Returns:
        list: Formatted AsciiDoc lines
    """
    if not recommendations:
        return []
    
    output = [
        "",
        "==== Recommendations",
        "[TIP]",
        "===="
    ]
    
    for rec in recommendations:
        # Ensure each recommendation is a bullet point
        if not rec.startswith('*'):
            rec = f"* {rec}"
        output.append(rec)
    
    output.append("====")
    output.append("")
    
    return output


def safe_execute_query(connector, query: str, error_context: str = "Query") -> Tuple[bool, str, Any]:
    """
    Safely execute a query with standardized error handling.
    
    Args:
        connector: Database connector
        query: Query to execute
        error_context: Context for error messages (e.g., "Nodetool command", "CQL query")
    
    Returns:
        tuple: (success: bool, formatted: str, raw_data: Any)
    """
    try:
        formatted, raw = connector.execute_query(query, return_raw=True)
        
        # Check for errors in formatted output
        if "[ERROR]" in formatted:
            return False, formatted, raw
        
        return True, formatted, raw
        
    except Exception as e:
        logger.error(f"{error_context} failed: {e}", exc_info=True)
        error_msg = f"[ERROR]\n====\n{error_context} failed: {str(e)}\n====\n"
        error_data = {"error": str(e)}
        return False, error_msg, error_data
