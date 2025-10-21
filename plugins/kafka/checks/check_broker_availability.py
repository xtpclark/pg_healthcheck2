# Import query functions from qrylib
from plugins.kafka.utils.qrylib.cluster_metadata_queries import get_cluster_metadata_query
from plugins.kafka.utils.qrylib.describe_topics_queries import get_describe_topics_query

def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 10  # Critical: service availability threats

def run_check_broker_availability(connector, settings):
    """
    Performs the broker availability health check.
    
    Checks broker availability by examining under-replicated partitions.
    If under-replicated partitions exist, at least one broker is down.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = ["=== Broker Availability", ""]
    structured_data = {}
    
    try:
        # Get list of all brokers from cluster metadata
        metadata_query = get_cluster_metadata_query(connector)
        formatted, raw = connector.execute_query(metadata_query, return_raw=True)
        
        if "[ERROR]" in formatted:
            adoc_content.append(formatted)
            structured_data["broker_availability"] = {"status": "error", "data": []}
            return "\n".join(adoc_content), structured_data
        
        brokers = raw.get('brokers', [])
        controller_id = raw.get('controller_id', -1)
        all_broker_ids = {b['id'] for b in brokers}
        
        # Get topic descriptions which includes under-replicated partition info
        topics_query = get_describe_topics_query(connector)
        topics_formatted, topics_raw = connector.execute_query(topics_query, return_raw=True)
        
        # Check for under-replicated partitions
        total_under_replicated = 0
        if isinstance(topics_raw, list):
            for topic_info in topics_raw:
                total_under_replicated += topic_info.get('under_replicated_partitions', 0)
        
        # Use cluster internal API to find which brokers are actually down
        cluster = connector.admin_client._client.cluster
        cluster.request_update()  # Force metadata refresh
        
        # Get all broker nodes known to the cluster
        cluster_broker_nodes = cluster.brokers()
        #available_broker_ids = {node.id for node in cluster_broker_nodes}
        available_broker_ids = {node.nodeId for node in cluster_broker_nodes}
        
        # Compare configured brokers vs available brokers
        brokers_data = []
        unavailable = []
        
        for b in brokers:
            broker_id = b['id']
            host = b['host']
            port = b['port']
            
            # Check if broker is in the live broker list
            if broker_id in available_broker_ids:
                status = "available"
            else:
                status = "unavailable"
                unavailable.append(broker_id)
            
            brokers_data.append({
                "broker_id": broker_id,
                "host": host,
                "port": port,
                "status": status
            })
        
        # Check controller availability
        controller_broker = next((d for d in brokers_data if d['broker_id'] == controller_id), None)
        controller_available = controller_broker and controller_broker['status'] == 'available' if controller_id != -1 else False
        
        adoc_content.append("==== Analysis Results")
        adoc_content.append("")
        
        if unavailable:
            adoc_content.append(
                "[CRITICAL]\n====\n"
                f"**Action Required:** {len(unavailable)} broker(s) unavailable: {', '.join(map(str, unavailable))}\n"
                f"These brokers are configured but not responding.\n"
            )
            if total_under_replicated > 0:
                adoc_content.append(f"This has resulted in {total_under_replicated} under-replicated partition(s).\n")
            adoc_content.append("====\n")
        else:
            adoc_content.append("[NOTE]\n====\nNo issues detected. All configured brokers are available.\n====\n")
        
        if controller_id != -1 and not controller_available:
            adoc_content.append(
                "[CRITICAL]\n====\n"
                f"**Action Required:** Controller broker (ID: {controller_id}) is unavailable.\n"
                "Cluster operations like topic creation and partition reassignment may fail.\n"
                "====\n"
            )
        
        # Add broker status table
        adoc_content.append("\n|===")
        adoc_content.append("|Broker ID|Host|Port|Status")
        for d in sorted(brokers_data, key=lambda x: x['broker_id']):
            status_icon = "✅" if d['status'] == 'available' else "❌"
            adoc_content.append(f"|{d['broker_id']}|{d['host']}|{d['port']}|{status_icon} {d['status']}")
        adoc_content.append("|===")
        
        # Debug info
        if total_under_replicated > 0:
            adoc_content.append(f"\n**Note:** {total_under_replicated} partition(s) are under-replicated due to broker unavailability.\n")
        
        # Recommendations
        if unavailable or not controller_available:
            adoc_content.append("\n==== Recommendations")
            adoc_content.append(
                "[TIP]\n====\n"
                "* **Immediate Action:** Check logs on unavailable brokers for errors.\n"
                "* **Common Causes:** Process crashed, out of memory, disk full, network issues.\n"
                "* **Verify:** SSH to the broker and check `systemctl status kafka` or process status.\n"
                "* **Logs:** Check Kafka server logs (usually in /var/log/kafka or kafka/logs/).\n"
                "* **Restart:** If necessary, restart the broker service.\n"
                "* **Monitoring:** Set up alerts for broker status changes to catch issues early.\n"
                "====\n"
            )
        
        structured_data["broker_availability"] = {
            "status": "success",
            "data": brokers_data,
            "count": len(brokers),
            "available_count": len(brokers) - len(unavailable),
            "unavailable_count": len(unavailable),
            "controller_id": controller_id,
            "controller_available": controller_available,
            "under_replicated_partitions": total_under_replicated
        }
    
    except Exception as e:
        error_msg = f"[ERROR]\n====\nCheck failed: {e}\n====\n"
        adoc_content.append(error_msg)
        structured_data["broker_availability"] = {"status": "error", "details": str(e)}
    
    return "\n".join(adoc_content), structured_data
