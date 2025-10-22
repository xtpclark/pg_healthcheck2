"""
Disk usage check for Kafka brokers.

Checks disk space usage on all Kafka broker nodes via SSH and reports
on partitions used by Kafka data directories.
"""

from plugins.common.check_helpers import require_ssh
from plugins.kafka.utils.qrylib.disk_usage_queries import get_disk_usage_query


def get_weight():
    """Module priority weight (1-10)."""
    return 7


def run_check_disk_usage(connector, settings):
    """
    Checks disk usage on all Kafka brokers via SSH.
    
    Collects disk usage for Kafka data directories across all configured hosts
    and analyzes against warning and critical thresholds.
    
    Args:
        connector: Kafka connector with multi-host SSH support
        settings: Configuration settings with thresholds
    
    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    adoc_content = ["=== Disk Usage (All Brokers)", ""]
    structured_data = {}
    
    # === STEP 1: CHECK SSH AVAILABILITY ===
    available, skip_msg, skip_data = require_ssh(connector, "Disk usage check")
    if not available:
        return skip_msg, skip_data
    
    try:
        # === STEP 2: GET THRESHOLDS ===
        warning_percent = settings.get('kafka_disk_warning_percent', 75)
        critical_percent = settings.get('kafka_disk_critical_percent', 90)
        
        # === STEP 3: EXECUTE ON ALL HOSTS ===
        # The command filters for common Kafka mount points
        command = "df -h | grep -E '(/data|/var/lib/kafka|/kafka|/opt/kafka)'"
        
        results = connector.execute_ssh_on_all_hosts(
            command,
            "disk usage check"
        )
        
        # === STEP 4: PARSE RESULTS ===
        all_disk_data = []
        issues_found = False
        critical_brokers = []
        warning_brokers = []
        errors = []
        
        for result in results:
            host = result['host']
            broker_id = result['node_id']  # Note: mixin uses 'node_id' not 'broker_id'
            
            if not result['success']:
                errors.append({
                    'host': host,
                    'broker_id': broker_id,
                    'error': result.get('error', 'Unknown error')
                })
                adoc_content.append(
                    f"[WARNING]\n====\n"
                    f"Could not check disk on {host} (Broker {broker_id}): {result.get('error')}\n"
                    f"====\n\n"
                )
                continue
            
            # Parse df output
            output = result['output'].strip()
            if not output:
                # No Kafka directories found - might use different paths
                adoc_content.append(
                    f"[NOTE]\n====\n"
                    f"No Kafka data directories found on {host} (Broker {broker_id})\n"
                    f"====\n\n"
                )
                continue
            
            lines = output.split('\n')
            broker_has_issues = False
            
            for line in lines:
                parts = line.split()
                if len(parts) >= 6:
                    filesystem = parts[0]
                    size = parts[1]
                    used = parts[2]
                    available = parts[3]
                    use_percent_str = parts[4].rstrip('%')
                    mount_point = ' '.join(parts[5:])  # Handle mount points with spaces
                    
                    try:
                        use_percent = int(use_percent_str)
                    except ValueError:
                        logger.warning(f"Could not parse usage percent: {use_percent_str}")
                        continue
                    
                    disk_info = {
                        'host': host,
                        'broker_id': broker_id,
                        'filesystem': filesystem,
                        'mount_point': mount_point,
                        'size': size,
                        'used': used,
                        'available': available,
                        'use_percent': use_percent,
                        'exceeds_critical': use_percent >= critical_percent,
                        'exceeds_warning': use_percent >= warning_percent
                    }
                    all_disk_data.append(disk_info)
                    
                    # === INTERPRET: Check thresholds ===
                    if use_percent >= critical_percent:
                        issues_found = True
                        broker_has_issues = True
                        if broker_id not in critical_brokers:
                            critical_brokers.append(broker_id)
                        
                        adoc_content.append(
                            f"[IMPORTANT]\n====\n"
                            f"**Critical Disk Usage**\n\n"
                            f"* **Broker:** {broker_id} ({host})\n"
                            f"* **Mount:** {mount_point}\n"
                            f"* **Usage:** {use_percent}% (threshold: {critical_percent}%)\n"
                            f"* **Available:** {available}\n"
                            f"* **Filesystem:** {filesystem}\n\n"
                            f"**Immediate action required to prevent data loss!**\n"
                            f"====\n\n"
                        )
                    
                    elif use_percent >= warning_percent:
                        issues_found = True
                        broker_has_issues = True
                        if broker_id not in warning_brokers:
                            warning_brokers.append(broker_id)
                        
                        adoc_content.append(
                            f"[WARNING]\n====\n"
                            f"**High Disk Usage**\n\n"
                            f"* **Broker:** {broker_id} ({host})\n"
                            f"* **Mount:** {mount_point}\n"
                            f"* **Usage:** {use_percent}% (threshold: {warning_percent}%)\n"
                            f"* **Available:** {available}\n"
                            f"* **Filesystem:** {filesystem}\n"
                            f"====\n\n"
                        )
            
            if not broker_has_issues:
                logger.info(f"Disk usage healthy on broker {broker_id} ({host})")
        
        # === STEP 5: SUMMARY TABLE ===
        if all_disk_data:
            adoc_content.append("==== Disk Usage Summary\n\n")
            adoc_content.append("|===\n")
            adoc_content.append("|Broker ID|Host|Mount Point|Size|Used|Available|Usage %\n")
            
            for disk in sorted(all_disk_data, key=lambda x: (x['broker_id'], x['use_percent']), reverse=True):
                # Highlight critical/warning rows
                usage_indicator = ""
                if disk['exceeds_critical']:
                    usage_indicator = "⚠️ "
                elif disk['exceeds_warning']:
                    usage_indicator = "⚡ "
                
                adoc_content.append(
                    f"|{disk['broker_id']}|{disk['host']}|{disk['mount_point']}|"
                    f"{disk['size']}|{disk['used']}|{disk['available']}|{usage_indicator}{disk['use_percent']}%\n"
                )
            adoc_content.append("|===\n\n")
        
        # === STEP 6: RECOMMENDATIONS ===
        if issues_found:
            adoc_content.append("==== Recommendations\n\n")
            adoc_content.append("[TIP]\n====\n")
            
            if critical_brokers:
                adoc_content.append("**Critical Priority (Immediate Action Required):**\n\n")
                adoc_content.append("* Delete old log segments: Review retention policies and manually clean up if needed\n")
                adoc_content.append("* Enable log compaction for compacted topics to reclaim space\n")
                adoc_content.append("* Add storage capacity or migrate to larger volumes immediately\n")
                adoc_content.append("* Check for partition imbalance and redistribute if possible\n\n")
            
            if warning_brokers:
                adoc_content.append("**High Priority (Plan Remediation):**\n\n")
                adoc_content.append("* Review and adjust topic retention policies (retention.ms, retention.bytes)\n")
                adoc_content.append("* Identify large topics and evaluate if data can be archived\n")
                adoc_content.append("* Plan for storage expansion in the near term\n")
                adoc_content.append("* Monitor growth rate to predict when critical threshold will be reached\n\n")
            
            adoc_content.append("**General Best Practices:**\n\n")
            adoc_content.append("* Set up disk space alerts at 70% threshold for early warning\n")
            adoc_content.append("* Implement automated retention policy enforcement\n")
            adoc_content.append("* Regular capacity planning reviews (monthly/quarterly)\n")
            adoc_content.append("* Consider tiered storage for older data if using Kafka 2.8+\n")
            adoc_content.append("* Document runbook for emergency disk cleanup procedures\n")
            adoc_content.append("====\n")
        else:
            adoc_content.append("[NOTE]\n====\n")
            adoc_content.append("✅ Disk usage is within healthy limits across all brokers.\n\n")
            adoc_content.append(f"All monitored filesystems are below {warning_percent}% usage.\n")
            adoc_content.append("====\n")
        
        # === STEP 7: STRUCTURED DATA ===
        structured_data["disk_usage"] = {
            "status": "success",
            "brokers_checked": len([r for r in results if r['success']]),
            "brokers_with_errors": len(errors),
            "critical_brokers": critical_brokers,
            "warning_brokers": warning_brokers,
            "thresholds": {
                "warning_percent": warning_percent,
                "critical_percent": critical_percent
            },
            "errors": errors,
            "data": all_disk_data
        }
        
    except Exception as e:
        error_msg = f"[ERROR]\n====\nDisk usage check failed: {e}\n====\n"
        adoc_content.append(error_msg)
        structured_data["disk_usage"] = {
            "status": "error",
            "details": str(e)
        }
    
    return "\n".join(adoc_content), structured_data
