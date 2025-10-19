from plugins.cassandra.utils.qrylib.qry_commitlog_directory_size import get_commitlog_directory_size_query
from plugins.common.check_helpers import (
    require_ssh,
    format_check_header,
    format_recommendations,
    safe_execute_query
)

def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 8  # High - disk space issues are critical

def run_commitlog_directory_size_check(connector, settings):
    """
    Analyzes the size of the Cassandra commitlog directory using shell commands.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    # Initialize with formatted header
    adoc_content = format_check_header(
        "Commitlog Directory Size Analysis (Shell)",
        "Checking the size of the Cassandra commitlog directory using `du -sh`.",
        requires_ssh=True
    )
    structured_data = {}
    
    # Check SSH availability
    ssh_ok, skip_msg, skip_data = require_ssh(connector, "shell commands")
    if not ssh_ok:
        adoc_content.append(skip_msg)
        structured_data["commitlog_size"] = skip_data
        return "\n".join(adoc_content), structured_data
    
    # Execute shell command
    query = get_commitlog_directory_size_query(connector)
    success, formatted, raw = safe_execute_query(connector, query, "du -sh commitlog")
    
    if not success:
        adoc_content.append(formatted)
        structured_data["commitlog_size"] = {"status": "error", "data": raw}
        return "\n".join(adoc_content), structured_data
    
    # Shell commands return raw text output
    # The connector's shell executor may parse it into a dict with 'output' key
    output = raw.get('output', '') if isinstance(raw, dict) else str(raw)
    
    if not output:
        adoc_content.append("[NOTE]\n====\nNo commitlog size data returned.\n====\n")
        structured_data["commitlog_size"] = {"status": "success", "data": {}}
        return "\n".join(adoc_content), structured_data
    
    # Parse du -sh output (e.g., "1.2G\t/var/lib/cassandra/commitlog")
    lines = output.strip().split('\n')
    if lines:
        line = lines[0]
        if '\t' in line:
            size_str, path = line.split('\t')
            path = path.strip()
            # Parse size: e.g., "1.2G" -> num=1.2, unit='G'
            if size_str:
                unit = size_str[-1].upper()
                size_num_str = size_str[:-1]
                try:
                    size_num = float(size_num_str)
                except ValueError:
                    size_num = 0.0
                
                # Convert to GB
                if unit == 'G':
                    size_gb = size_num
                elif unit == 'M':
                    size_gb = size_num / 1024.0
                elif unit == 'K':
                    size_gb = size_num / (1024.0 * 1024.0)
                elif unit == 'T':
                    size_gb = size_num * 1024.0
                else:
                    size_gb = 0.0
            else:
                size_gb = 0.0
                path = ''
        else:
            size_gb = 0.0
            path = ''
    else:
        size_gb = 0.0
        path = ''
    
    # Add formatted output
    adoc_content.append(formatted)
    
    # Analyze and report
    if size_gb > 50:
        adoc_content.append(
            f"[WARNING]\n====\n"
            f"Commitlog directory size is {size_gb:.2f} GB, which exceeds the recommended threshold of 50 GB.\n"
            "====\n"
        )
        
        recommendations = [
            "Analyze write patterns in your application - consider batching or reducing unnecessary writes",
            "Verify commitlog configuration in cassandra.yaml: ensure commitlog_segment_size_in_mb is appropriate (default 32MB)",
            "Check for stuck transactions or uncommitted writes using application logs",
            "If commitlog is on a separate disk, ensure it has sufficient space; consider moving to SSD for better performance",
            "Run 'nodetool compactionstats' to check if compaction issues are contributing to write amplification"
        ]
        adoc_content.extend(format_recommendations(recommendations))
        
        status = "warning"
    else:
        adoc_content.append(
            f"[NOTE]\n====\n"
            f"Commitlog directory size is {size_gb:.2f} GB (within normal limits).\n"
            "====\n"
        )
        status = "success"
    
    structured_data["commitlog_size"] = {
        "status": status,
        "data": {"size_gb": size_gb, "path": path},
        "size_gb": size_gb,
        "path": path
    }
    
    return "\n".join(adoc_content), structured_data