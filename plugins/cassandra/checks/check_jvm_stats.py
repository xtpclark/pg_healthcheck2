"""
JVM statistics check for Cassandra brokers.

Monitors JVM heap usage, garbage collection activity, and memory pool
statistics across all broker nodes. This module serves as a template
pattern for JVM monitoring that can be adapted for other databases
like Cassandra, MongoDB, etc.

Key Metrics Monitored:
- Heap memory usage (Eden, Survivor, Old Generation)
- Non-heap memory (Metaspace, Code Cache)
- GC activity (count, time, frequency)
- Memory pool utilization

Template Pattern Features:
- Reusable parsing functions
- Configurable thresholds
- Multi-host execution
- Structured data for rules engine
"""

from plugins.common.check_helpers import require_ssh
import logging
import re

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 8


def parse_jstat_gc(output):
    """
    Parse jstat -gc output into structured data.
    
    Template function that can be reused across database types.
    
    Args:
        output: Raw jstat -gc output
        
    Returns:
        dict: Parsed GC statistics or None if parsing fails
    """
    try:
        lines = output.strip().split('\n')
        if len(lines) < 2:
            return None
        
        # Header line has column names
        headers = lines[0].split()
        # Data line has values
        values = lines[1].split()
        
        if len(headers) != len(values):
            logger.warning(f"jstat header/value mismatch: {len(headers)} vs {len(values)}")
            return None
        
        # Create dict from headers and values
        gc_data = {}
        for i, header in enumerate(headers):
            try:
                gc_data[header] = float(values[i])
            except ValueError:
                gc_data[header] = values[i]
        
        return gc_data
        
    except Exception as e:
        logger.error(f"Error parsing jstat -gc output: {e}")
        return None


def parse_jstat_gcutil(output):
    """
    Parse jstat -gcutil output into structured data.
    
    Template function for percentage-based GC stats.
    
    Args:
        output: Raw jstat -gcutil output
        
    Returns:
        dict: Parsed GC utilization statistics or None
    """
    try:
        lines = output.strip().split('\n')
        if len(lines) < 2:
            return None
        
        headers = lines[0].split()
        values = lines[1].split()
        
        if len(headers) != len(values):
            return None
        
        gcutil_data = {}
        for i, header in enumerate(headers):
            try:
                gcutil_data[header] = float(values[i])
            except ValueError:
                gcutil_data[header] = values[i]
        
        return gcutil_data
        
    except Exception as e:
        logger.error(f"Error parsing jstat -gcutil output: {e}")
        return None


def calculate_heap_usage(gc_data):
    """
    Calculate heap memory usage from jstat data.
    
    Template function for heap calculations.
    
    Args:
        gc_data: Parsed jstat -gc data
        
    Returns:
        dict: Heap usage statistics
    """
    try:
        # Young generation (Eden + Survivor spaces)
        eden_used = gc_data.get('EU', 0)  # Eden Used
        survivor0_used = gc_data.get('S0U', 0)  # Survivor 0 Used
        survivor1_used = gc_data.get('S1U', 0)  # Survivor 1 Used
        young_used = eden_used + survivor0_used + survivor1_used
        
        eden_capacity = gc_data.get('EC', 0)  # Eden Capacity
        survivor0_capacity = gc_data.get('S0C', 0)
        survivor1_capacity = gc_data.get('S1C', 0)
        young_capacity = eden_capacity + survivor0_capacity + survivor1_capacity
        
        # Old generation
        old_used = gc_data.get('OU', 0)  # Old Used
        old_capacity = gc_data.get('OC', 0)  # Old Capacity
        
        # Total heap
        heap_used = young_used + old_used
        heap_capacity = young_capacity + old_capacity
        
        # Metaspace (non-heap)
        metaspace_used = gc_data.get('MU', 0)  # Metaspace Used
        metaspace_capacity = gc_data.get('MC', 0)  # Metaspace Capacity
        
        return {
            'young_used_kb': young_used,
            'young_capacity_kb': young_capacity,
            'young_util_percent': (young_used / young_capacity * 100) if young_capacity > 0 else 0,
            'old_used_kb': old_used,
            'old_capacity_kb': old_capacity,
            'old_util_percent': (old_used / old_capacity * 100) if old_capacity > 0 else 0,
            'heap_used_kb': heap_used,
            'heap_capacity_kb': heap_capacity,
            'heap_util_percent': (heap_used / heap_capacity * 100) if heap_capacity > 0 else 0,
            'metaspace_used_kb': metaspace_used,
            'metaspace_capacity_kb': metaspace_capacity,
            'metaspace_util_percent': (metaspace_used / metaspace_capacity * 100) if metaspace_capacity > 0 else 0
        }
        
    except Exception as e:
        logger.error(f"Error calculating heap usage: {e}")
        return None


def calculate_gc_metrics(gc_data, gcutil_data):
    """
    Calculate GC performance metrics.
    
    Template function for GC analysis.
    
    Args:
        gc_data: Parsed jstat -gc data
        gcutil_data: Parsed jstat -gcutil data
        
    Returns:
        dict: GC performance metrics
    """
    try:
        # Young GC (Minor GC)
        ygc_count = gc_data.get('YGC', 0)  # Young GC count
        ygc_time = gc_data.get('YGCT', 0)  # Young GC time (seconds)
        
        # Full GC (Major GC)
        fgc_count = gc_data.get('FGC', 0)  # Full GC count
        fgc_time = gc_data.get('FGCT', 0)  # Full GC time (seconds)
        
        # Total
        total_gc_time = ygc_time + fgc_time
        total_gc_count = ygc_count + fgc_count
        
        # Calculate averages
        avg_ygc_time = (ygc_time / ygc_count * 1000) if ygc_count > 0 else 0  # Convert to ms
        avg_fgc_time = (fgc_time / fgc_count * 1000) if fgc_count > 0 else 0
        
        return {
            'young_gc_count': int(ygc_count),
            'young_gc_time_sec': ygc_time,
            'avg_young_gc_time_ms': avg_ygc_time,
            'full_gc_count': int(fgc_count),
            'full_gc_time_sec': fgc_time,
            'avg_full_gc_time_ms': avg_fgc_time,
            'total_gc_time_sec': total_gc_time,
            'total_gc_count': int(total_gc_count),
            'gc_time_percent': gcutil_data.get('GCT', 0) if gcutil_data else 0
        }
        
    except Exception as e:
        logger.error(f"Error calculating GC metrics: {e}")
        return None


def run_check_jvm_stats(connector, settings):
    """
    Checks JVM memory and GC statistics on all Cassandra brokers via SSH.
    
    This is a TEMPLATE CHECK that demonstrates the pattern for JVM monitoring
    across distributed systems. It can be adapted for:
    - Cassandra (change process grep pattern)
    - MongoDB (if using Java driver)
    - Elasticsearch
    - Any JVM-based database
    
    Pattern Features:
    1. Multi-host execution
    2. Process discovery (find Java PID)
    3. Multiple jstat commands
    4. Reusable parsing functions
    5. Threshold-based analysis
    6. Structured data for rules engine
    
    Args:
        connector: Database connector with multi-host SSH support
        settings: Configuration settings with thresholds
    
    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    adoc_content = ["=== JVM Memory & GC Statistics (All Brokers)", ""]
    structured_data = {}
    
    # Check SSH availability
    available, skip_msg, skip_data = require_ssh(connector, "JVM statistics check")
    if not available:
        return skip_msg, skip_data
    
    try:
        # === STEP 1: GET THRESHOLDS ===
        heap_warning_percent = settings.get('cassandra_jvm_heap_warning_percent', 75)
        heap_critical_percent = settings.get('cassandra_jvm_heap_critical_percent', 90)
        old_gen_warning_percent = settings.get('cassandra_jvm_old_gen_warning_percent', 80)
        old_gen_critical_percent = settings.get('cassandra_jvm_old_gen_critical_percent', 90)
        metaspace_warning_percent = settings.get('cassandra_jvm_metaspace_warning_percent', 85)
        metaspace_critical_percent = settings.get('cassandra_jvm_metaspace_critical_percent', 95)
        fgc_warning_count = settings.get('cassandra_jvm_fgc_warning_count', 10)
        fgc_critical_count = settings.get('cassandra_jvm_fgc_critical_count', 50)
        
        # === STEP 2: EXECUTE ON ALL HOSTS ===
        command = """
# Find Cassandra Java process
CASSANDRA_PID=$(ps aux | grep -i cassandra | grep java | grep -v grep | awk '{print $2}' | head -1)
CASSANDRA_USER=$(ps aux | grep -i cassandra | grep java | grep -v grep | awk '{print $1}' | head -1)

if [ -z "$CASSANDRA_PID" ]; then
    echo "ERROR: Cassandra process not found"
    exit 1
fi

if [ -z "$CASSANDRA_USER" ]; then
    CASSANDRA_USER=cassandra
fi

echo "CASSANDRA_PID=$CASSANDRA_PID"
echo "CASSANDRA_USER=$CASSANDRA_USER"

# Get GC stats (run as Cassandra user if not already that user)
echo "=== GC_STATS ==="
if [ "$(whoami)" = "$CASSANDRA_USER" ]; then
    jstat -gc $CASSANDRA_PID 2>&1
else
    sudo -u $CASSANDRA_USER jstat -gc $CASSANDRA_PID 2>&1
fi

# Get GC summary
echo "=== GC_UTIL ==="
if [ "$(whoami)" = "$CASSANDRA_USER" ]; then
    jstat -gcutil $CASSANDRA_PID 2>&1
else
    sudo -u $CASSANDRA_USER jstat -gcutil $CASSANDRA_PID 2>&1
fi

"""
        
        results = connector.execute_ssh_on_all_hosts(
            command,
            "JVM statistics check"
        )
        
        # === STEP 3: PARSE RESULTS ===
        all_jvm_data = []
        issues_found = False
        critical_brokers = []
        warning_brokers = []
        errors = []
        
        for result in results:
            host = result['host']
            broker_id = result['node_id']
            
            if not result['success']:
                errors.append({
                    'host': host,
                    'broker_id': broker_id,
                    'error': result.get('error', 'Unknown error')
                })
                adoc_content.append(
                    f"[WARNING]\n====\n"
                    f"Could not collect JVM stats on {host} (Broker {broker_id}): {result.get('error')}\n"
                    f"====\n\n"
                )
                continue
            
            # Parse output
            output = result['output'].strip()
            if not output or "ERROR" in output:
                error_msg = "Cassandra process not found" if "not found" in output else "Unknown error"
                errors.append({
                    'host': host,
                    'broker_id': broker_id,
                    'error': error_msg
                })
                adoc_content.append(
                    f"[WARNING]\n====\n"
                    f"Could not collect JVM stats on {host} (Broker {broker_id}): {error_msg}\n"
                    f"====\n\n"
                )
                continue
            
            # Extract PID
            pid_match = re.search(r'CASSANDRA_PID=(\d+)', output)
            cassandra_pid = pid_match.group(1) if pid_match else 'unknown'
            
            # Extract sections
            gc_stats_section = re.search(r'=== GC_STATS ===\n(.*?)\n===', output, re.DOTALL)
            gc_util_section = re.search(r'=== GC_UTIL ===\n(.*?)(?:\n===|$)', output, re.DOTALL)
            
            gc_data = None
            gcutil_data = None
            
            if gc_stats_section:
                gc_data = parse_jstat_gc(gc_stats_section.group(1))
            
            if gc_util_section:
                gcutil_data = parse_jstat_gcutil(gc_util_section.group(1))
            
            if not gc_data or not gcutil_data:
                errors.append({
                    'host': host,
                    'broker_id': broker_id,
                    'error': 'Could not parse jstat output'
                })
                continue
            
            # Calculate metrics
            heap_usage = calculate_heap_usage(gc_data)
            gc_metrics = calculate_gc_metrics(gc_data, gcutil_data)
            
            if not heap_usage or not gc_metrics:
                errors.append({
                    'host': host,
                    'broker_id': broker_id,
                    'error': 'Could not calculate JVM metrics'
                })
                continue
            
            # Combine all data
            jvm_info = {
                'host': host,
                'broker_id': broker_id,
                'cassandra_pid': cassandra_pid,
                **heap_usage,
                **gc_metrics
            }
            all_jvm_data.append(jvm_info)
            
            # === STEP 4: INTERPRET - Check Thresholds ===
            broker_has_issues = False
            
            # Check heap usage
            if jvm_info['heap_util_percent'] >= heap_critical_percent:
                issues_found = True
                broker_has_issues = True
                if broker_id not in critical_brokers:
                    critical_brokers.append(broker_id)
                
                adoc_content.append(
                    f"[IMPORTANT]\n====\n"
                    f"**Critical Heap Memory Usage**\n\n"
                    f"* **Broker:** {broker_id} ({host})\n"
                    f"* **Heap Usage:** {jvm_info['heap_util_percent']:.1f}% (threshold: {heap_critical_percent}%)\n"
                    f"* **Heap Used:** {jvm_info['heap_used_kb']/1024:.1f} MB / {jvm_info['heap_capacity_kb']/1024:.1f} MB\n"
                    f"* **Old Gen:** {jvm_info['old_util_percent']:.1f}%\n"
                    f"* **Full GC Count:** {jvm_info['full_gc_count']}\n\n"
                    f"**⚠️ Risk of OutOfMemoryError! Immediate action required.**\n"
                    f"====\n\n"
                )
            
            elif jvm_info['heap_util_percent'] >= heap_warning_percent:
                issues_found = True
                broker_has_issues = True
                if broker_id not in warning_brokers:
                    warning_brokers.append(broker_id)
                
                adoc_content.append(
                    f"[WARNING]\n====\n"
                    f"**High Heap Memory Usage**\n\n"
                    f"* **Broker:** {broker_id} ({host})\n"
                    f"* **Heap Usage:** {jvm_info['heap_util_percent']:.1f}% (threshold: {heap_warning_percent}%)\n"
                    f"* **Heap Used:** {jvm_info['heap_used_kb']/1024:.1f} MB / {jvm_info['heap_capacity_kb']/1024:.1f} MB\n"
                    f"====\n\n"
                )
            
            # Check Old Generation
            if jvm_info['old_util_percent'] >= old_gen_critical_percent:
                issues_found = True
                broker_has_issues = True
                if broker_id not in critical_brokers:
                    critical_brokers.append(broker_id)
                
                adoc_content.append(
                    f"[IMPORTANT]\n====\n"
                    f"**Critical Old Generation Usage**\n\n"
                    f"* **Broker:** {broker_id} ({host})\n"
                    f"* **Old Gen:** {jvm_info['old_util_percent']:.1f}% (threshold: {old_gen_critical_percent}%)\n"
                    f"* **Full GC Count:** {jvm_info['full_gc_count']}\n"
                    f"* **Avg Full GC Time:** {jvm_info['avg_full_gc_time_ms']:.1f} ms\n\n"
                    f"**Old generation is nearly full - risk of Full GC storms!**\n"
                    f"====\n\n"
                )
            
            # Check Full GC activity
            if jvm_info['full_gc_count'] >= fgc_critical_count:
                issues_found = True
                broker_has_issues = True
                if broker_id not in critical_brokers:
                    critical_brokers.append(broker_id)
                
                adoc_content.append(
                    f"[IMPORTANT]\n====\n"
                    f"**Excessive Full GC Activity**\n\n"
                    f"* **Broker:** {broker_id} ({host})\n"
                    f"* **Full GC Count:** {jvm_info['full_gc_count']} (threshold: {fgc_critical_count})\n"
                    f"* **Full GC Time:** {jvm_info['full_gc_time_sec']:.2f} seconds total\n"
                    f"* **Avg Full GC:** {jvm_info['avg_full_gc_time_ms']:.1f} ms\n\n"
                    f"**Frequent Full GCs indicate memory pressure!**\n"
                    f"====\n\n"
                )
            
            # Check Metaspace
            if jvm_info['metaspace_util_percent'] >= metaspace_critical_percent:
                issues_found = True
                broker_has_issues = True
                if broker_id not in critical_brokers:
                    critical_brokers.append(broker_id)
                
                adoc_content.append(
                    f"[WARNING]\n====\n"
                    f"**High Metaspace Usage**\n\n"
                    f"* **Broker:** {broker_id} ({host})\n"
                    f"* **Metaspace:** {jvm_info['metaspace_util_percent']:.1f}% (threshold: {metaspace_critical_percent}%)\n"
                    f"* **Used:** {jvm_info['metaspace_used_kb']/1024:.1f} MB / {jvm_info['metaspace_capacity_kb']/1024:.1f} MB\n"
                    f"====\n\n"
                )
        
        # === STEP 5: SUMMARY TABLES ===
        if all_jvm_data:
            adoc_content.append("==== Heap Memory Summary\n\n")
            adoc_content.append("|===\n")
            adoc_content.append("|Broker|Host|Heap Used (MB)|Heap Max (MB)|Heap %|Old Gen %|Metaspace %\n")
            
            for jvm in sorted(all_jvm_data, key=lambda x: x['heap_util_percent'], reverse=True):
                heap_indicator = ""
                if jvm['heap_util_percent'] >= heap_critical_percent:
                    heap_indicator = "🔴 "
                elif jvm['heap_util_percent'] >= heap_warning_percent:
                    heap_indicator = "⚠️ "
                
                old_indicator = ""
                if jvm['old_util_percent'] >= old_gen_critical_percent:
                    old_indicator = "🔴 "
                elif jvm['old_util_percent'] >= old_gen_warning_percent:
                    old_indicator = "⚠️ "
                
                adoc_content.append(
                    f"|{jvm['broker_id']}|{jvm['host']}|"
                    f"{jvm['heap_used_kb']/1024:.1f}|{jvm['heap_capacity_kb']/1024:.1f}|"
                    f"{heap_indicator}{jvm['heap_util_percent']:.1f}|"
                    f"{old_indicator}{jvm['old_util_percent']:.1f}|"
                    f"{jvm['metaspace_util_percent']:.1f}\n"
                )
            adoc_content.append("|===\n\n")
            
            adoc_content.append("==== Garbage Collection Summary\n\n")
            adoc_content.append("|===\n")
            adoc_content.append("|Broker|Host|Young GC Count|Avg Young GC (ms)|Full GC Count|Avg Full GC (ms)|Total GC Time (s)\n")
            
            for jvm in sorted(all_jvm_data, key=lambda x: x['full_gc_count'], reverse=True):
                fgc_indicator = ""
                if jvm['full_gc_count'] >= fgc_critical_count:
                    fgc_indicator = "🔴 "
                elif jvm['full_gc_count'] >= fgc_warning_count:
                    fgc_indicator = "⚠️ "
                
                adoc_content.append(
                    f"|{jvm['broker_id']}|{jvm['host']}|"
                    f"{jvm['young_gc_count']}|{jvm['avg_young_gc_time_ms']:.1f}|"
                    f"{fgc_indicator}{jvm['full_gc_count']}|{jvm['avg_full_gc_time_ms']:.1f}|"
                    f"{jvm['total_gc_time_sec']:.2f}\n"
                )
            adoc_content.append("|===\n\n")
        
        # === STEP 6: ERROR SUMMARY ===
        if errors:
            adoc_content.append("==== Collection Errors\n\n")
            adoc_content.append("[WARNING]\n====\n")
            adoc_content.append(f"Could not collect JVM stats from {len(errors)} broker(s):\n\n")
            for error in errors:
                adoc_content.append(f"* Broker {error['broker_id']} ({error['host']}): {error['error']}\n")
            adoc_content.append("====\n\n")
        
        # === STEP 7: RECOMMENDATIONS ===
        if issues_found:
            adoc_content.append("==== Recommendations\n\n")
            adoc_content.append("[TIP]\n====\n")
            
            if critical_brokers:
                adoc_content.append("**🔴 Critical Priority (Immediate Action):**\n\n")
                adoc_content.append("* **Increase heap size:** Add -Xmx and -Xms JVM options (recommend 6-8GB for Cassandra)\n")
                adoc_content.append("* **Tune GC settings:** Consider G1GC with appropriate pause time goals\n")
                adoc_content.append("* **Review memory leaks:** Check for growing old generation over time\n")
                adoc_content.append("* **Monitor Full GCs:** Frequent Full GCs indicate insufficient heap\n")
                adoc_content.append("* **Check broker workload:** High message rates may require more memory\n\n")
            
            if warning_brokers:
                adoc_content.append("**⚠️ High Priority (Plan Optimization):**\n\n")
                adoc_content.append("* **Monitor trends:** Track heap usage over time for growth patterns\n")
                adoc_content.append("* **Review broker configuration:** Optimize buffer sizes and batch configs\n")
                adoc_content.append("* **Plan heap increase:** Add memory before reaching critical levels\n")
                adoc_content.append("* **GC tuning:** Consider switching to G1GC if using CMS/Parallel\n\n")
            
            adoc_content.append("**📋 General Best Practices:**\n\n")
            adoc_content.append("* Set heap size: 6GB minimum, 8-12GB for production (but < 32GB to use compressed oops)\n")
            adoc_content.append("* Use G1GC: `-XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35`\n")
            adoc_content.append("* Set Xms = Xmx: Prevent heap resizing overhead\n")
            adoc_content.append("* Monitor GC logs: Enable GC logging for troubleshooting\n")
            adoc_content.append("* Consider off-heap caching: Reduce heap pressure for page cache\n")
            adoc_content.append("====\n")
        else:
            adoc_content.append("[NOTE]\n====\n")
            adoc_content.append("✅ JVM memory and GC performance is healthy across all brokers.\n\n")
            adoc_content.append(f"All brokers show heap usage below {heap_warning_percent}% with acceptable GC activity.\n")
            adoc_content.append("====\n")
        
        # === STEP 8: STRUCTURED DATA ===
        structured_data["jvm_stats"] = {
            "status": "success",
            "brokers_checked": len([r for r in results if r['success']]),
            "brokers_with_errors": len(errors),
            "critical_brokers": critical_brokers,
            "warning_brokers": warning_brokers,
            "thresholds": {
                "heap_warning_percent": heap_warning_percent,
                "heap_critical_percent": heap_critical_percent,
                "old_gen_warning_percent": old_gen_warning_percent,
                "old_gen_critical_percent": old_gen_critical_percent,
                "fgc_warning_count": fgc_warning_count,
                "fgc_critical_count": fgc_critical_count
            },
            "errors": errors,
            "data": all_jvm_data
        }
        
        # Summary for aggregate rules
        structured_data["jvm_summary"] = {
            "status": "success",
            "data": [{
                "total_brokers_checked": len([r for r in results if r['success']]),
                "critical_broker_count": len(critical_brokers),
                "warning_broker_count": len(warning_brokers),
                "max_heap_util": max([jvm['heap_util_percent'] for jvm in all_jvm_data], default=0),
                "max_old_gen_util": max([jvm['old_util_percent'] for jvm in all_jvm_data], default=0),
                "max_fgc_count": max([jvm['full_gc_count'] for jvm in all_jvm_data], default=0),
                "total_fgc_count": sum([jvm['full_gc_count'] for jvm in all_jvm_data])
            }]
        }
        
    except Exception as e:
        import traceback
        logger.error(f"JVM stats check failed: {e}\n{traceback.format_exc()}")
        error_msg = f"[ERROR]\n====\nJVM statistics check failed: {e}\n====\n"
        adoc_content.append(error_msg)
        structured_data["jvm_stats"] = {
            "status": "error",
            "details": str(e)
        }
    
    return "\n".join(adoc_content), structured_data
