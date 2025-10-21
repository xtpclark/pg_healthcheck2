from plugins.kafka.utils.qrylib.check_storage_health_queries import get_describe_log_dirs_query

def get_weight():
    return 8

def run_check_storage_health(connector, settings):
    """
    Analyzes storage health across Kafka brokers.
    
    Collects log directory information and analyzes:
    - Total storage per broker
    - Large partitions (data skew)
    - Storage threshold violations
    """
    adoc_content = ["=== Storage Health Analysis", ""]
    structured_data = {}
    
    # Separate thresholds for partitions vs brokers
    warning_partition_gb = settings.get('storage_warning_partition_gb', 10)
    critical_partition_gb = settings.get('storage_critical_partition_gb', 50)
    warning_broker_gb = settings.get('storage_warning_broker_gb', 100)
    critical_broker_gb = settings.get('storage_critical_broker_gb', 500)
    
    # Convert to bytes
    warning_partition_bytes = warning_partition_gb * 1024 * 1024 * 1024
    critical_partition_bytes = critical_partition_gb * 1024 * 1024 * 1024
    warning_broker_bytes = warning_broker_gb * 1024 * 1024 * 1024
    critical_broker_bytes = critical_broker_gb * 1024 * 1024 * 1024
    
    try:
        # Get log directory information (includes broker IDs)
        query = get_describe_log_dirs_query(connector)
        formatted, raw = connector.execute_query(query, return_raw=True)
        
        # Check for errors
        if "[ERROR]" in formatted or (isinstance(raw, dict) and 'error' in raw):
            adoc_content.append(formatted)
            structured_data["storage_health"] = {"status": "error", "data": raw}
            return "\n".join(adoc_content), structured_data
        
        # Validate data structure
        if not raw or not isinstance(raw, list):
            adoc_content.append("[NOTE]\n====\nNo storage usage data available.\n====\n")
            structured_data["storage_health"] = {
                "status": "success",
                "data": [],
                "message": "No log directory data returned"
            }
            return "\n".join(adoc_content), structured_data
        
        if len(raw) == 0:
            adoc_content.append("[NOTE]\n====\nNo partitions found in cluster.\n====\n")
            structured_data["storage_health"] = {"status": "success", "data": []}
            return "\n".join(adoc_content), structured_data
        
        # === COLLECT FACTS: Aggregate per broker and track large partitions ===
        broker_stats = {}
        large_partitions = []
        all_partition_data = []
        
        for entry in raw:
            broker_id = entry.get('broker_id')
            topic = entry.get('topic')
            partition = entry.get('partition')
            size_bytes = entry.get('size_bytes', 0)
            size_mb = size_bytes / (1024 * 1024)
            size_gb = size_bytes / (1024 * 1024 * 1024)
            
            # Track all partition data for structured output
            all_partition_data.append({
                'broker_id': broker_id,
                'topic': topic,
                'partition': partition,
                'size_bytes': size_bytes,
                'size_mb': round(size_mb, 2),
                'size_gb': round(size_gb, 2)
            })
            
            # Initialize broker stats if needed
            if broker_id not in broker_stats:
                broker_stats[broker_id] = {
                    'broker_id': broker_id,
                    'total_size_bytes': 0,
                    'total_size_gb': 0,
                    'partition_count': 0,
                    'largest_partition_mb': 0,
                    'largest_partition_topic': None
                }
            
            # Update broker totals
            broker_stats[broker_id]['total_size_bytes'] += size_bytes
            broker_stats[broker_id]['partition_count'] += 1
            
            if size_mb > broker_stats[broker_id]['largest_partition_mb']:
                broker_stats[broker_id]['largest_partition_mb'] = size_mb
                broker_stats[broker_id]['largest_partition_topic'] = f"{topic}-{partition}"
            
            # Track oversized partitions
            if size_bytes > critical_partition_bytes:
                large_partitions.append({
                    'broker_id': broker_id,
                    'topic': topic,
                    'partition': partition,
                    'size_bytes': size_bytes,
                    'size_gb': round(size_gb, 2),
                    'exceeds_critical_threshold': True
                })
            elif size_bytes > warning_partition_bytes:
                large_partitions.append({
                    'broker_id': broker_id,
                    'topic': topic,
                    'partition': partition,
                    'size_bytes': size_bytes,
                    'size_gb': round(size_gb, 2),
                    'exceeds_warning_threshold': True
                })
        
        # Convert broker totals to GB
        for broker_id in broker_stats:
            broker_stats[broker_id]['total_size_gb'] = round(
                broker_stats[broker_id]['total_size_bytes'] / (1024 * 1024 * 1024), 2
            )
        
        # === INTERPRET FACTS: Analyze and report issues ===
        issues_found = False
        broker_critical = []
        broker_warning = []
        
        for broker_id, stats in broker_stats.items():
            total_gb = stats['total_size_gb']
            
            if stats['total_size_bytes'] > critical_broker_bytes:
                issues_found = True
                broker_critical.append(broker_id)
                adoc_content.append(
                    f"[IMPORTANT]\n====\n"
                    f"**Critical Storage Usage:** Broker {broker_id} has {total_gb}GB total storage "
                    f"(threshold: {critical_broker_gb}GB)\n\n"
                    f"Partitions: {stats['partition_count']}, "
                    f"Largest: {round(stats['largest_partition_mb'] / 1024, 2)}GB ({stats['largest_partition_topic']})\n"
                    f"====\n\n"
                )
            
            elif stats['total_size_bytes'] > warning_broker_bytes:
                issues_found = True
                broker_warning.append(broker_id)
                adoc_content.append(
                    f"[WARNING]\n====\n"
                    f"**High Storage Usage:** Broker {broker_id} has {total_gb}GB total storage "
                    f"(threshold: {warning_broker_gb}GB)\n\n"
                    f"Partitions: {stats['partition_count']}, "
                    f"Largest: {round(stats['largest_partition_mb'] / 1024, 2)}GB ({stats['largest_partition_topic']})\n"
                    f"====\n\n"
                )
        
        if large_partitions:
            issues_found = True
            critical_count = sum(1 for p in large_partitions if p.get('exceeds_critical_threshold'))
            warning_count = sum(1 for p in large_partitions if p.get('exceeds_warning_threshold'))
            
            adoc_content.append("[WARNING]\n====\n")
            adoc_content.append(f"**Large Partitions Detected:**\n\n")
            if critical_count > 0:
                adoc_content.append(f"* {critical_count} partition(s) exceed {critical_partition_gb}GB (critical)\n")
            if warning_count > 0:
                adoc_content.append(f"* {warning_count} partition(s) exceed {warning_partition_gb}GB (warning)\n")
            adoc_content.append("\nThis may indicate data skew or retention issues.\n====\n\n")
            
            # Show top 10 largest
            adoc_content.append("**Largest Partitions:**\n\n")
            for lp in sorted(large_partitions, key=lambda x: x['size_bytes'], reverse=True)[:10]:
                severity = "CRITICAL" if lp.get('exceeds_critical_threshold') else "WARNING"
                adoc_content.append(
                    f"* [{severity}] Broker {lp['broker_id']}: {lp['topic']}-{lp['partition']} = {lp['size_gb']}GB\n"
                )
            
            if len(large_partitions) > 10:
                adoc_content.append(f"\n... and {len(large_partitions) - 10} more\n")
            adoc_content.append("\n")
        
        # Show detailed storage table
        adoc_content.append("==== Broker Storage Summary\n\n")
        adoc_content.append(formatted)
        adoc_content.append("\n")
        
        # Recommendations
        if issues_found:
            adoc_content.append("==== Recommendations\n")
            adoc_content.append("[TIP]\n====\n")
            
            if broker_critical or broker_warning:
                adoc_content.append("**Broker Storage:**\n")
                adoc_content.append("* Review and adjust topic retention policies to prevent excessive growth\n")
                adoc_content.append("* Enable log compaction for key-based topics to reduce storage\n")
                adoc_content.append("* Consider adding storage capacity or rebalancing partitions\n")
                adoc_content.append("* Set up disk space alerts at 70% capacity\n\n")
            
            if large_partitions:
                adoc_content.append("**Large Partitions:**\n")
                adoc_content.append("* Increase partition count for topics with data skew\n")
                adoc_content.append("* Review partitioning key strategy for even distribution\n")
                adoc_content.append("* Adjust retention settings for high-volume topics\n")
                adoc_content.append("* Consider topic archival strategies for historical data\n\n")
            
            adoc_content.append("**Monitoring:**\n")
            adoc_content.append("* Track storage growth trends over time\n")
            adoc_content.append("* Monitor partition size distribution\n")
            adoc_content.append("* Alert on rapid storage increases\n")
            adoc_content.append("====\n")
        else:
            adoc_content.append("[NOTE]\n====\n")
            adoc_content.append("✅ Storage usage is within healthy limits across all brokers.\n")
            adoc_content.append("====\n")
        
        # === STRUCTURED DATA: Pure facts for machines ===
        broker_list = list(broker_stats.values())
        
        structured_data["broker_storage"] = {
            "status": "success",
            "total_brokers": len(broker_list),
            "brokers_critical": broker_critical,
            "brokers_warning": broker_warning,
            "thresholds": {
                "warning_broker_gb": warning_broker_gb,
                "critical_broker_gb": critical_broker_gb,
                "warning_partition_gb": warning_partition_gb,
                "critical_partition_gb": critical_partition_gb
            },
            "data": broker_list
        }
        
        structured_data["partition_storage"] = {
            "status": "success",
            "total_large_partitions": len(large_partitions),
            "critical_partitions": sum(1 for p in large_partitions if p.get('exceeds_critical_threshold')),
            "warning_partitions": sum(1 for p in large_partitions if p.get('exceeds_warning_threshold')),
            "data": large_partitions
        }
    
    except Exception as e:
        error_msg = f"[ERROR]\n====\nStorage health check failed: {e}\n====\n"
        adoc_content.append(error_msg)
        structured_data["storage_health"] = {"status": "error", "details": str(e)}
    
    return "\n".join(adoc_content), structured_data
