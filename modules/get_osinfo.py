import subprocess

def run_osinfo(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Gathers basic operating system information (e.g., hostname, OS, uptime, CPU, memory)
    for the machine where the health check script is executed.
    """
    adoc_content = ["Provides an overview of the operating system and hardware resources of the machine running this health check script.\n"]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    if settings['show_qry'] == 'true':
        adoc_content.append("System information commands (executed via subprocess):")
        adoc_content.append("[,bash]\n----")
        adoc_content.append("hostname")
        adoc_content.append("uname -a")
        adoc_content.append("uptime")
        adoc_content.append("nproc --all") # For CPU core count
        adoc_content.append("free -h") # For memory info
        adoc_content.append("----")

    # Define commands to execute for OS information
    os_commands = [
        ("Hostname", "hostname", "hostname"),
        ("OS Information", "uname -a", "os_info"),
        ("System Uptime", "uptime", "system_uptime"),
        ("CPU Cores", "nproc --all", "cpu_cores"),
        ("Memory Usage", "free -h", "memory_usage")
    ]

    # Add a general note about where the OS info is collected from
    adoc_content.append("[NOTE]\n====\n"
                       "The system details in this section reflect the operating system and hardware of the **machine where this `pg_healthcheck.py` script is being executed**, "
                       "not necessarily the PostgreSQL database server itself, especially if the database is running remotely (e.g., on a cloud-managed service or a different host).\n"
                       "====\n")

    for title, command, data_key in os_commands:
        try:
            # Execute command using subprocess
            result = subprocess.run(command, shell=True, capture_output=True, text=True, check=True)
            output = result.stdout.strip()
            
            adoc_content.append(f"{title}\n")
            adoc_content.append(f"```text\n{output}\n```\n") # Use text block for raw output
            structured_data[data_key] = {"status": "success", "data": output}
        except subprocess.CalledProcessError as e:
            error_msg = f"Command failed: {command}\nError: {e.stderr.strip()}"
            adoc_content.append(f"{title}\n[ERROR]\n====\n{error_msg}\n====\n")
            structured_data[data_key] = {"status": "error", "details": error_msg}
        except FileNotFoundError:
            error_msg = f"Command not found: {command.split(' ')[0]}"
            adoc_content.append(f"{title}\n[ERROR]\n====\n{error_msg}\n====\n")
            structured_data[data_key] = {"status": "error", "details": error_msg}
        except Exception as e:
            error_msg = f"Unexpected error running command '{command}': {e}"
            adoc_content.append(f"{title}\n[ERROR]\n====\n{error_msg}\n====\n")
            structured_data[data_key] = {"status": "error", "details": error_msg}
    
    adoc_content.append("[TIP]\n====\n"
                   "Understanding the underlying operating system and hardware resources is critical for self-hosted PostgreSQL instances. "
                   "Monitor CPU load, memory utilization, disk space, and network throughput to identify resource bottlenecks. "
                   "Ensure sufficient resources are allocated to the database server.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\n"
                       "For AWS RDS Aurora, direct OS-level access is restricted. "
                       "Monitor instance metrics (CPUUtilization, FreeableMemory, DiskQueueDepth, NetworkThroughput) via Amazon CloudWatch. "
                       "Enhanced Monitoring provides more granular OS metrics for RDS instances.\n"
                       "====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data

