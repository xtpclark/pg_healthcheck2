"""
Patroni Health Status Check

Monitors cluster health, liveness, and readiness across all nodes.
Uses Patroni REST API with SSH fallback following the Instaclustr pattern.

Health Endpoints:
- GET /health - PostgreSQL operational state (200=up, 503=down)
- GET /liveness - Patroni heartbeat (200=alive)
- GET /readiness - Ready for traffic (200=ready, 503=not ready)

Output:
- Table showing health status for all nodes
- Issues identified with recommendations
- Health score calculation
"""

import logging
from datetime import datetime
from typing import Dict, List, Tuple
from plugins.common.check_helpers import CheckContentBuilder
from plugins.postgres.utils.patroni_client import PatroniClient
from plugins.postgres.utils.patroni_helpers import (
    skip_if_not_patroni,
    build_error_result
)

logger = logging.getLogger(__name__)


def check_patroni_health_status(connector, settings: Dict) -> Tuple[str, Dict]:
    """
    Check Patroni cluster health status across all nodes.

    This check queries the /health, /liveness, and /readiness endpoints
    for each node in the cluster to provide a comprehensive view of
    cluster health.

    Args:
        connector: PostgreSQL connector with environment detection
        settings: Configuration dictionary

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder()
    builder.h3("Patroni Health Status")

    # Skip if not Patroni
    skip_result = skip_if_not_patroni(connector)
    if skip_result:
        return skip_result

    timestamp = datetime.utcnow().isoformat() + 'Z'

    try:
        # Discover cluster nodes
        nodes = _discover_cluster_nodes(settings, connector)

        if not nodes:
            return build_error_result(
                'patroni_health_status',
                'Could not discover any cluster nodes',
                builder
            )

        # Query health status for all nodes
        health_data = _fetch_health_status_all_nodes(nodes, settings)

        # Build output
        _build_health_output(builder, health_data)

        # Analyze and build findings
        findings = _analyze_health_data(health_data, timestamp)

        return builder.build(), findings

    except Exception as e:
        logger.error(f"Failed to check Patroni health: {e}", exc_info=True)
        return build_error_result(
            'patroni_health_status',
            str(e),
            builder
        )


def _discover_cluster_nodes(settings: Dict, connector) -> List[Dict]:
    """
    Discover all nodes in the Patroni cluster.

    First tries to discover via Patroni API /cluster endpoint,
    then falls back to configured hosts.

    Args:
        settings: Configuration dictionary
        connector: PostgreSQL connector

    Returns:
        List of node dictionaries with 'name', 'host', 'port' keys
    """
    nodes = []

    # Try discovery via Patroni API
    try:
        patroni_host = settings.get('host')
        patroni_port = settings.get('patroni_port', 8008)

        if patroni_host:
            client = PatroniClient(
                f"http://{patroni_host}:{patroni_port}",
                timeout=settings.get('patroni_timeout', 5)
            )

            success, result = client.get_cluster_topology()
            client.close()

            if success and result.get('data'):
                members = result['data'].get('members', [])
                for member in members:
                    nodes.append({
                        'name': member.get('name', 'unknown'),
                        'host': member.get('host', patroni_host),
                        'port': patroni_port,
                        'role': member.get('role', 'unknown')
                    })

                logger.info(f"Discovered {len(nodes)} nodes via Patroni API")
                return nodes

    except Exception as e:
        logger.debug(f"Could not discover nodes via API: {e}")

    # Fallback: Use configured host
    if settings.get('host'):
        nodes.append({
            'name': settings.get('host'),
            'host': settings['host'],
            'port': settings.get('patroni_port', 8008),
            'role': 'unknown'
        })
        logger.info("Using configured host as single node")

    return nodes


def _fetch_health_status_all_nodes(nodes: List[Dict], settings: Dict) -> List[Dict]:
    """
    Fetch health, liveness, and readiness status for all nodes.

    Args:
        nodes: List of node dictionaries
        settings: Configuration dictionary

    Returns:
        List of node health dictionaries with status information
    """
    health_data = []
    timeout = settings.get('patroni_timeout', 5)

    for node in nodes:
        node_health = {
            'name': node['name'],
            'host': node['host'],
            'port': node['port'],
            'role': node.get('role', 'unknown')
        }

        # Create client for this node
        client = PatroniClient(
            f"http://{node['host']}:{node['port']}",
            timeout=timeout
        )

        try:
            # Query /health endpoint
            success, health_result = client.get_health_status()
            if success:
                node_health['health'] = {
                    'healthy': health_result.get('healthy', False),
                    'http_status': health_result.get('http_status')
                }
            else:
                node_health['health'] = {
                    'healthy': False,
                    'error': health_result.get('error', 'Unknown error')
                }

            # Query /liveness endpoint
            success, liveness_result = client.get_liveness()
            if success:
                node_health['liveness'] = {
                    'alive': liveness_result.get('alive', False),
                    'http_status': liveness_result.get('http_status')
                }
            else:
                node_health['liveness'] = {
                    'alive': False,
                    'error': liveness_result.get('error', 'Unknown error')
                }

            # Query /readiness endpoint
            success, readiness_result = client.get_readiness()
            if success:
                node_health['readiness'] = {
                    'ready': readiness_result.get('ready', False),
                    'http_status': readiness_result.get('http_status')
                }
            else:
                node_health['readiness'] = {
                    'ready': False,
                    'error': readiness_result.get('error', 'Unknown error')
                }

        except Exception as e:
            logger.warning(f"Error fetching health for {node['name']}: {e}")
            node_health['health'] = {'healthy': False, 'error': str(e)}
            node_health['liveness'] = {'alive': False, 'error': str(e)}
            node_health['readiness'] = {'ready': False, 'error': str(e)}

        finally:
            client.close()

        health_data.append(node_health)

    return health_data


def _build_health_output(builder: CheckContentBuilder, health_data: List[Dict]):
    """
    Build AsciiDoc output for health status.

    Args:
        builder: CheckContentBuilder instance
        health_data: List of node health dictionaries
    """
    # Summary
    total_nodes = len(health_data)
    healthy_nodes = sum(1 for n in health_data if n.get('health', {}).get('healthy', False))
    alive_nodes = sum(1 for n in health_data if n.get('liveness', {}).get('alive', False))
    ready_nodes = sum(1 for n in health_data if n.get('readiness', {}).get('ready', False))

    health_score = int((healthy_nodes / total_nodes) * 100) if total_nodes > 0 else 0

    summary_data = [
        {'Attribute': 'Total Nodes', 'Value': str(total_nodes)},
        {'Attribute': 'Healthy Nodes', 'Value': f"{healthy_nodes}/{total_nodes}"},
        {'Attribute': 'Alive Nodes', 'Value': f"{alive_nodes}/{total_nodes}"},
        {'Attribute': 'Ready Nodes', 'Value': f"{ready_nodes}/{total_nodes}"},
        {'Attribute': 'Health Score', 'Value': f"{health_score}/100"}
    ]

    builder.text("*Cluster Health Summary*")
    builder.blank()
    builder.table(summary_data)
    builder.blank()

    # Node details table
    builder.text("*Node Health Status*")
    builder.blank()

    table_data = []
    for node in health_data:
        health_info = node.get('health', {})
        liveness_info = node.get('liveness', {})
        readiness_info = node.get('readiness', {})

        table_data.append({
            'Node': node['name'],
            'Role': node.get('role', 'unknown'),
            'Health': _format_status(health_info.get('healthy', False)),
            'Liveness': _format_status(liveness_info.get('alive', False)),
            'Readiness': _format_status(readiness_info.get('ready', False))
        })

    builder.table(table_data)
    builder.blank()

    # Issues section
    issues = _identify_issues(health_data)
    if issues:
        builder.text("*⚠️  Issues Detected*")
        builder.blank()
        for issue in issues:
            builder.text(f"• *{issue['node']}*: {issue['message']}")
            if issue.get('recommendation'):
                builder.text(f"  _Recommendation_: {issue['recommendation']}")
        builder.blank()
    else:
        builder.text("*✅ No Issues Detected*")
        builder.blank()
        builder.text("All nodes are healthy, alive, and ready for traffic.")
        builder.blank()


def _format_status(status: bool) -> str:
    """
    Format status as emoji indicator.

    Args:
        status: Boolean status

    Returns:
        Formatted string with emoji
    """
    return "✅ UP" if status else "❌ DOWN"


def _identify_issues(health_data: List[Dict]) -> List[Dict]:
    """
    Identify issues from health data.

    Args:
        health_data: List of node health dictionaries

    Returns:
        List of issue dictionaries
    """
    issues = []

    for node in health_data:
        node_name = node['name']
        health_info = node.get('health', {})
        liveness_info = node.get('liveness', {})
        readiness_info = node.get('readiness', {})

        # Check health
        if not health_info.get('healthy', False):
            error = health_info.get('error', 'PostgreSQL is not running')
            issues.append({
                'node': node_name,
                'severity': 'critical',
                'type': 'health',
                'message': f"PostgreSQL is not healthy: {error}",
                'recommendation': 'Check PostgreSQL logs and service status'
            })

        # Check liveness
        if not liveness_info.get('alive', False):
            error = liveness_info.get('error', 'Patroni heartbeat missing')
            issues.append({
                'node': node_name,
                'severity': 'critical',
                'type': 'liveness',
                'message': f"Patroni is not alive: {error}",
                'recommendation': 'Check Patroni service status and DCS connectivity'
            })

        # Check readiness (only warning, as node might be catching up)
        if not readiness_info.get('ready', False):
            error = readiness_info.get('error', 'Node not ready for traffic')
            issues.append({
                'node': node_name,
                'severity': 'warning',
                'type': 'readiness',
                'message': f"Node not ready for traffic: {error}",
                'recommendation': 'Check replication lag and node state'
            })

    return issues


def _analyze_health_data(health_data: List[Dict], timestamp: str) -> Dict:
    """
    Analyze health data and create structured findings.

    Args:
        health_data: List of node health dictionaries
        timestamp: ISO 8601 timestamp

    Returns:
        Structured findings dictionary
    """
    total_nodes = len(health_data)
    healthy_nodes = sum(1 for n in health_data if n.get('health', {}).get('healthy', False))
    alive_nodes = sum(1 for n in health_data if n.get('liveness', {}).get('alive', False))
    ready_nodes = sum(1 for n in health_data if n.get('readiness', {}).get('ready', False))

    health_score = int((healthy_nodes / total_nodes) * 100) if total_nodes > 0 else 0

    # Determine overall status
    if healthy_nodes == total_nodes and alive_nodes == total_nodes:
        overall_status = 'healthy'
    elif healthy_nodes == 0 or alive_nodes == 0:
        overall_status = 'critical'
    else:
        overall_status = 'degraded'

    issues = _identify_issues(health_data)

    findings = {
        'patroni_health_status': {
            'status': 'success',
            'timestamp': timestamp,
            'overall_status': overall_status,
            'health_score': health_score,
            'summary': {
                'total_nodes': total_nodes,
                'healthy_nodes': healthy_nodes,
                'alive_nodes': alive_nodes,
                'ready_nodes': ready_nodes
            },
            'nodes': health_data,
            'issues': issues,
            'source': 'patroni_api'
        }
    }

    return findings
