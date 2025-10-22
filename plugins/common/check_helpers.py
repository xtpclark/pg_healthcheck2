"""
Common helper functions for health checks.

Provides utilities that are useful across all database health checks,
regardless of the underlying database technology.
"""

import logging
from typing import Tuple, Dict, Any, Optional, List, Union

logger = logging.getLogger(__name__)



def require_ssh(connector, operation_name):
    """
    Check if SSH is available for the connector.
    
    Works with both old (single ssh_manager) and new (multi-host mixin) patterns.
    
    Args:
        connector: The database connector instance
        operation_name: Human-readable name of the operation requiring SSH
    
    Returns:
        tuple: (available: bool, skip_message: str, skip_data: dict)
            - If SSH is available: (True, None, None)
            - If SSH is not available: (False, adoc_skip_message, structured_data)
    
    Example:
        available, skip_msg, skip_data = require_ssh(connector, "disk usage check")
        if not available:
            return skip_msg, skip_data
        
        # Proceed with SSH operations...
    """
    # NEW PATTERN: Check if connector uses SSHSupportMixin
    if hasattr(connector, 'has_ssh_support') and callable(connector.has_ssh_support):
        if not connector.has_ssh_support():
            # Use mixin's skip message method if available
            if hasattr(connector, 'get_ssh_skip_message'):
                return (False,) + connector.get_ssh_skip_message(operation_name)
            else:
                # Fallback message
                skip_msg = (
                    f"[IMPORTANT]\n"
                    f"====\n"
                    f"{operation_name} requires SSH access, which is not configured.\n"
                    f"====\n"
                )
                skip_data = {"status": "skipped", "reason": "SSH not configured"}
                return False, skip_msg, skip_data
        
        # SSH is available
        return True, None, None
    
    # OLD PATTERN: Check for single ssh_manager (backward compatibility)
    if not hasattr(connector, 'ssh_manager') or connector.ssh_manager is None:
        skip_msg = (
            f"[IMPORTANT]\n"
            f"====\n"
            f"{operation_name} requires SSH access, which is not configured.\n\n"
            f"Configure the following in your settings:\n\n"
            f"* `ssh_host`: Hostname or IP address\n"
            f"* `ssh_user`: SSH username\n"
            f"* `ssh_key_file` OR `ssh_password`: Authentication method\n"
            f"====\n"
        )
        skip_data = {"status": "skipped", "reason": "SSH not configured"}
        return False, skip_msg, skip_data
    
    # Old pattern SSH is available
    return True, None, None



def require_aws(connector: Any, operation_name: Optional[str] = None) -> Tuple[bool, str, Dict]:
    """
    Check if AWS support is available and return appropriate response if not.
    
    Args:
        connector: Database connector with AWSSupportMixin
        operation_name: Optional description of what AWS is needed for
    
    Returns:
        tuple: (aws_available: bool, skip_message: str, skip_data: dict)
        
    Example:
        aws_ok, skip_msg, skip_data = require_aws(connector, "CloudWatch metrics")
        if not aws_ok:
            return skip_msg, skip_data
        # Continue with check...
    """
    if hasattr(connector, 'has_aws_support') and connector.has_aws_support():
        return True, "", {}
    
    # AWS not available
    if hasattr(connector, 'get_aws_skip_message'):
        skip_msg, skip_data = connector.get_aws_skip_message(operation_name)
    else:
        # Fallback if connector doesn't have the mixin
        op_text = f" for {operation_name}" if operation_name else ""
        skip_msg = (
            "[IMPORTANT]\n"
            "====\n"
            f"This check requires AWS access{op_text}.\n\n"
            "Configure the following in your settings:\n\n"
            "* `aws_region`: AWS region (e.g., 'us-east-1')\n"
            "* `aws_access_key_id`: AWS access key\n"
            "* `aws_secret_access_key`: AWS secret key\n"
            "====\n"
        )
        skip_data = {
            "status": "skipped",
            "reason": "AWS not configured",
            "required_settings": ["aws_region", "aws_access_key_id", "aws_secret_access_key"]
        }
    
    return False, skip_msg, skip_data


def require_azure(connector: Any, operation_name: Optional[str] = None) -> Tuple[bool, str, Dict]:
    """
    Check if Azure support is available and return appropriate response if not.
    
    Args:
        connector: Database connector with AzureSupportMixin
        operation_name: Optional description of what Azure is needed for
    
    Returns:
        tuple: (azure_available: bool, skip_message: str, skip_data: dict)
        
    Example:
        azure_ok, skip_msg, skip_data = require_azure(connector, "Azure Monitor metrics")
        if not azure_ok:
            return skip_msg, skip_data
        # Continue with check...
    """
    if hasattr(connector, 'has_azure_support') and connector.has_azure_support():
        return True, "", {}
    
    # Azure not available
    if hasattr(connector, 'get_azure_skip_message'):
        skip_msg, skip_data = connector.get_azure_skip_message(operation_name)
    else:
        # Fallback if connector doesn't have the mixin
        op_text = f" for {operation_name}" if operation_name else ""
        skip_msg = (
            "[IMPORTANT]\n"
            "====\n"
            f"This check requires Azure access{op_text}.\n\n"
            "Configure credentials via:\n"
            "* Azure CLI (`az login`)\n"
            "* Environment variables: `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_TENANT_ID`\n"
            "* `config/azure_credentials.yaml` with `subscription_id`, `client_id`, `client_secret`, `tenant_id`\n"
            "====\n"
        )
        skip_data = {
            "status": "skipped",
            "reason": "Azure not configured",
            "required_settings": ["subscription_id", "resource_group", "server_name"]
        }
    
    return False, skip_msg, skip_data


def require_instaclustr(connector: Any, operation_name: Optional[str] = None) -> Tuple[bool, str, Dict]:
    """
    Check if Instaclustr support is available and return appropriate response if not.
    
    Args:
        connector: Database connector with InstaclustrSupportMixin
        operation_name: Optional description of what Instaclustr is needed for
    
    Returns:
        tuple: (instaclustr_available: bool, skip_message: str, skip_data: dict)
        
    Example:
        ic_ok, skip_msg, skip_data = require_instaclustr(connector, "cluster metrics")
        if not ic_ok:
            return skip_msg, skip_data
        # Continue with check...
    """
    if hasattr(connector, 'has_instaclustr_support') and connector.has_instaclustr_support():
        return True, "", {}
    
    # Instaclustr not available
    if hasattr(connector, 'get_instaclustr_skip_message'):
        skip_msg, skip_data = connector.get_instaclustr_skip_message(operation_name)
    else:
        # Fallback if connector doesn't have the mixin
        op_text = f" for {operation_name}" if operation_name else ""
        skip_msg = (
            "[IMPORTANT]\n"
            "====\n"
            f"This check requires Instaclustr access{op_text}.\n\n"
            "Configure in settings or config/instaclustr_credentials.yaml:\n"
            "* `instaclustr_api_key`: API key\n"
            "* `instaclustr_cluster_id`: Cluster ID\n"
            "====\n"
        )
        skip_data = {
            "status": "skipped",
            "reason": "Instaclustr not configured",
            "required_settings": ["instaclustr_api_key", "instaclustr_cluster_id"]
        }
    
    return False, skip_msg, skip_data


def format_check_header(check_name: str, description: str, 
                        requires_ssh: bool = False,
                        requires_aws: bool = False,
                        requires_azure: bool = False,
                        requires_instaclustr: bool = False) -> List[str]:
    """
    Generate a standard check header with title and description.
    
    Args:
        check_name: Name of the check
        description: Description of what the check does
        requires_ssh: If True, adds SSH requirement note
        requires_aws: If True, adds AWS requirement note
        requires_azure: If True, adds Azure requirement note
        requires_instaclustr: If True, adds Instaclustr requirement note
    
    Returns:
        list: Lines of AsciiDoc content
    """
    header = [
        f"=== {check_name}",
        "",
        description
    ]
    
    # Add requirement notes if needed
    requirements = []
    if requires_ssh:
        requirements.append("SSH access to the database server")
    if requires_aws:
        requirements.append("AWS credentials with CloudWatch read permissions")
    if requires_azure:
        requirements.append("Azure credentials with Monitor read permissions")
    if requires_instaclustr:
        requirements.append("Instaclustr API access")
    
    if requirements:
        header.extend([
            "",
            "[NOTE]",
            "====",
            "**Requirements:**",
            ""
        ])
        for req in requirements:
            header.append(f"* {req}")
        header.append("====")
    
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


def safe_execute_query(
    connector: Any,
    query: str,
    error_context: str = "Query",
    params: Optional[Dict[str, Any]] = None
) -> Tuple[bool, str, Union[List[Dict], Dict, Any]]:
    """
    Safely execute a query with standardized error handling.
    
    Args:
        connector: Database connector with execute_query method
        query: Query to execute (SQL, CQL, JSON command, etc.)
        error_context: Context for error messages (e.g., "Nodetool command", "CQL query")
        params: Optional query parameters for parameterized queries
    
    Returns:
        tuple: (success: bool, formatted: str, raw_data: Any)
            - success: True if query executed without errors
            - formatted: AsciiDoc formatted output string
            - raw_data: Structured data (list of dicts, dict, or error dict)
    
    Example:
        success, formatted, raw = safe_execute_query(
            connector, 
            "SELECT * FROM system.peers",
            "System peers query"
        )
        if not success:
            adoc_content.append(formatted)
            structured_data["result"] = {"status": "error", "data": raw}
            return "\n".join(adoc_content), structured_data
    """
    try:
        # Execute query with or without parameters
        if params:
            formatted, raw = connector.execute_query(query, params, return_raw=True)
        else:
            formatted, raw = connector.execute_query(query, return_raw=True)
        
        # Check for errors in formatted output
        if "[ERROR]" in formatted or "[CAUTION]" in formatted:
            logger.warning(f"{error_context} returned error: {formatted[:100]}...")
            return False, formatted, raw
        
        return True, formatted, raw
        
    except AttributeError as e:
        # Connector doesn't have execute_query method
        logger.error(f"Connector missing execute_query method: {e}", exc_info=True)
        error_msg = (
            "[CAUTION]\n"
            "====\n"
            f"Internal error: Connector does not support query execution.\n"
            f"Details: {str(e)}\n"
            "====\n"
        )
        error_data = {
            "error": "AttributeError",
            "message": str(e),
            "error_context": error_context,
            "query": query[:200] if query else None
        }
        return False, error_msg, error_data
    
    except Exception as e:
        logger.error(f"{error_context} failed: {e}", exc_info=True)
        error_msg = (
            "[CAUTION]\n"
            "====\n"
            f"{error_context} failed: {str(e)}\n"
            "====\n"
        )
        error_data = {
            "error": type(e).__name__,
            "message": str(e),
            "error_context": error_context,
            "query": query[:200] if query else None
        }
        return False, error_msg, error_data


def merge_structured_data(base_data: Dict[str, Any], 
                          new_data: Dict[str, Any],
                          section_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Safely merge new data into base structured data dictionary.
    
    Useful for combining results from multiple queries in a single check.
    
    Args:
        base_data: Base dictionary to merge into
        new_data: New data to add
        section_name: Optional section name to nest new_data under
    
    Returns:
        Updated base_data dictionary (also modifies in place)
    
    Example:
        structured_data = {}
        
        # First query
        success, formatted, raw = safe_execute_query(...)
        structured_data = merge_structured_data(
            structured_data,
            {"node_status": raw},
            "cluster_health"
        )
        
        # Second query
        success, formatted, raw = safe_execute_query(...)
        structured_data = merge_structured_data(
            structured_data,
            {"compaction_stats": raw},
            "cluster_health"
        )
        
        # Result: structured_data = {
        #     "cluster_health": {
        #         "node_status": [...],
        #         "compaction_stats": {...}
        #     }
        # }
    """
    if section_name:
        # Create section if it doesn't exist
        if section_name not in base_data:
            base_data[section_name] = {}
        
        # Merge into section
        base_data[section_name].update(new_data)
    else:
        # Merge directly into base
        base_data.update(new_data)
    
    return base_data


def calculate_percentage(value: Union[int, float], 
                        total: Union[int, float],
                        decimal_places: int = 1) -> Optional[float]:
    """
    Safely calculate percentage, handling zero division.
    
    Args:
        value: Numerator value
        total: Denominator value
        decimal_places: Number of decimal places to round to
    
    Returns:
        Percentage as float, or None if total is 0
    
    Example:
        pct = calculate_percentage(75, 100)  # Returns 75.0
        pct = calculate_percentage(1, 3, 2)  # Returns 33.33
        pct = calculate_percentage(5, 0)     # Returns None
    """
    if total == 0 or total is None:
        return None
    
    try:
        percentage = (float(value) / float(total)) * 100
        return round(percentage, decimal_places)
    except (ValueError, TypeError, ZeroDivisionError):
        logger.warning(f"Could not calculate percentage: {value}/{total}")
        return None


def format_bytes(bytes_value: int, decimal_places: int = 2) -> str:
    """
    Format bytes into human-readable string (KB, MB, GB, TB).
    
    Args:
        bytes_value: Number of bytes
        decimal_places: Number of decimal places to round to
    
    Returns:
        Formatted string (e.g., "1.50 GB")
    
    Example:
        format_bytes(1024)           # "1.00 KB"
        format_bytes(1536)           # "1.50 KB"
        format_bytes(1073741824)     # "1.00 GB"
        format_bytes(0)              # "0 B"
    """
    if bytes_value == 0:
        return "0 B"
    
    if bytes_value < 0:
        return f"{bytes_value} B"
    
    units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    unit_index = 0
    value = float(bytes_value)
    
    while value >= 1024.0 and unit_index < len(units) - 1:
        value /= 1024.0
        unit_index += 1
    
    return f"{value:.{decimal_places}f} {units[unit_index]}"
