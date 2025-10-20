"""
Azure connection management for database health checks.

Provides a reusable Azure connection manager for PostgreSQL and Monitor metrics.
"""

import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from pathlib import Path
import yaml

try:
    from azure.identity import DefaultAzureCredential
    from azure.monitor.query import MetricsQueryClient
    from azure.mgmt.rdbms.postgresql import PostgreSqlManagementClient
    from azure.core.exceptions import HttpResponseError
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False
    # Define placeholder exception for decorator
    class HttpResponseError(Exception):
        pass


from .retry_utils import retry_on_failure

logger = logging.getLogger(__name__)


class AzureSupportMixin:
    """Mixin for Azure support detection in database connectors."""
    
    def has_azure_support(self) -> bool:
        """Check if Azure operations are supported."""
        return (hasattr(self, 'azure_manager') and 
                self.azure_manager is not None and 
                self.azure_manager.is_configured())

    def get_azure_skip_message(self, operation_name: Optional[str] = None) -> Tuple[str, Dict]:
        """Generate a skip message for Azure-dependent checks."""
        op_text = f" for {operation_name}" if operation_name else ""
        adoc_message = (
            "[IMPORTANT]\n"
            "====\n"
            f"This check requires Azure access{op_text}.\n\n"
            "Configure credentials via:\n\n"
            "* Azure CLI (`az login`)\n"
            "* Environment variables: `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_TENANT_ID`\n"
            "* `config/azure_credentials.yaml` with `subscription_id`, `client_id`, `client_secret`, `tenant_id`\n\n"
            "Required permissions:\n\n"
            "* `Microsoft.DBforPostgreSQL/servers/read`\n"
            "* `Microsoft.Insights/Metrics/Read`\n"
            "====\n"
        )
        structured_data = {
            "status": "skipped",
            "reason": "Azure not configured",
            "required_settings": ["subscription_id", "resource_group", "server_name"],
            "required_permissions": [
                "Microsoft.DBforPostgreSQL/servers/read",
                "Microsoft.Insights/Metrics/Read"
            ]
        }
        return adoc_message, structured_data


class AzureConnectionManager:
    """
    Manages Azure connections for PostgreSQL and Monitor metrics.
    
    Uses DefaultAzureCredential for authentication.
    """
    
    def __init__(self, settings: Dict):
        """
        Initialize Azure connection manager.
        
        Args:
            settings: Dictionary with Azure configuration:
                - subscription_id: Azure subscription ID
                - resource_group: Resource group name
                - server_name: PostgreSQL server name
        """
        if not AZURE_AVAILABLE:
            raise ImportError(
                "Azure libraries are required for Azure support. "
                "Install them with: pip install azure-identity azure-monitor-query azure-mgmt-rdbms"
            )
        
        self.settings = settings
        self.credential = None
        self.postgres_client = None
        self.metrics_client = None
        self._load_credentials()
        self._validate_settings()
        self.connect()

    def _load_credentials(self):
        """Load credentials from config/azure_credentials.yaml or environment."""
        config_yaml = Path('config') / 'azure_credentials.yaml'
        
        if config_yaml.exists():
            try:
                with open(config_yaml, 'r') as f:
                    creds = yaml.safe_load(f)
                
                self.settings.update({
                    'subscription_id': creds.get('subscription_id', self.settings.get('subscription_id')),
                    'client_id': creds.get('client_id'),
                    'client_secret': creds.get('client_secret'),
                    'tenant_id': creds.get('tenant_id')
                })
                logger.info(f"Loaded Azure credentials from {config_yaml}")
            except Exception as e:
                logger.warning(f"Failed to load config/azure_credentials.yaml: {e}")
        
        # Initialize credential (will use environment vars, managed identity, or CLI auth)
        self.credential = DefaultAzureCredential()

    def _validate_settings(self):
        """Validate required Azure settings."""
        required = ['subscription_id', 'resource_group', 'server_name']
        missing = [key for key in required if not self.settings.get(key)]
        
        if missing:
            raise ValueError(
                f"Missing required Azure settings: {', '.join(missing)}. "
                "Ensure credentials are in config/azure_credentials.yaml or environment variables."
            )

    def connect(self) -> None:
        """Initialize Azure clients."""
        try:
            self.postgres_client = PostgreSqlManagementClient(
                self.credential,
                self.settings['subscription_id']
            )
            
            self.metrics_client = MetricsQueryClient(self.credential)
            
            logger.info(f"✅ Azure clients initialized for subscription {self.settings['subscription_id']}")
            
        except Exception as e:
            logger.error(f"Azure client initialization failed: {e}")
            raise ConnectionError(f"Could not initialize Azure clients: {e}")

    def is_configured(self) -> bool:
        """Check if Azure clients are initialized."""
        return self.postgres_client is not None and self.metrics_client is not None

    @retry_on_failure(max_attempts=3, delay=1, exceptions=(HttpResponseError,))

    def get_server_details(self, server_name: Optional[str] = None) -> Optional[Dict]:
        """
        Fetch Azure PostgreSQL server details.
        
        Args:
            server_name: Server name (uses settings if not provided)
        
        Returns:
            Dict with server details or None if not found
        """
        server_name = server_name or self.settings['server_name']
        
        try:
            server = self.postgres_client.servers.get(
                resource_group_name=self.settings['resource_group'],
                server_name=server_name
            )
            
            return {
                "server_name": server.name,
                "location": server.location,
                "version": server.version,
                "sku_name": server.sku.name,
                "sku_tier": server.sku.tier,
                "sku_capacity": server.sku.capacity,
                "storage_mb": server.storage_profile.storage_mb,
                "backup_retention_days": server.storage_profile.backup_retention_days,
                "geo_redundant_backup": server.storage_profile.geo_redundant_backup,
                "ssl_enforcement": server.ssl_enforcement,
                "user_visible_state": server.user_visible_state
            }
        
        except HttpResponseError as e:
            if e.status_code == 404:
                logger.warning(f"Azure server not found: {server_name}")
            elif e.status_code == 403:
                logger.error(f"Permission denied for Azure server {server_name}: {e}")
            else:
                logger.warning(f"Could not fetch Azure server details for {server_name}: {e}")
        
        except Exception as e:
            logger.error(f"Unexpected error fetching Azure server details: {e}")
        
        return None

    def get_metrics(self, 
                   resource_id: str, 
                   metrics_to_fetch: List[str] = None, 
                   hours: int = 24) -> Dict:
        """
        Fetch Azure Monitor metrics with retry logic.
        
        Args:
            resource_id: Azure resource ID
            metrics_to_fetch: List of metric names to fetch
            hours: Number of hours of historical data
        
        Returns:
            Dict mapping metric names to their values and metadata
        """
        default_metrics = [
            'cpu_percent',
            'io_consumption_percent',
            'storage_used',
            'active_connections',
            'connections_failed'
        ]
        metrics_to_fetch = metrics_to_fetch or default_metrics
        
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours)
        
        fetched_metrics = {}
        
        for metric_name in metrics_to_fetch:
            try:
                # Apply retry logic
                @retry_on_failure(
                    max_attempts=3,
                    delay=1,
                    exceptions=(HttpResponseError, AzureError),
                    log_attempts=True
                )
                def fetch_metric():
                    return self.metrics_client.query_resource(
                        resource_id,
                        metric_names=[metric_name],
                        timespan=(start_time, end_time)
                    )
                
                result = fetch_metric()
                
                if result.metrics:
                    metric = result.metrics[0]
                    
                    if metric.timeseries:
                        # Get latest datapoint
                        timeseries = metric.timeseries[0]
                        if timeseries.data:
                            latest = timeseries.data[-1]
                            
                            # Use average, total, or count depending on availability
                            value = latest.average or latest.total or latest.count
                            
                            fetched_metrics[metric_name] = {
                                "value": value,
                                "unit": metric.unit,
                                "timestamp": latest.time_stamp.isoformat(),
                                "status": "success"
                            }
                        else:
                            fetched_metrics[metric_name] = {
                                "value": "N/A",
                                "note": "No data points found.",
                                "status": "no_data"
                            }
                    else:
                        fetched_metrics[metric_name] = {
                            "value": "N/A",
                            "note": "No time series data.",
                            "status": "no_data"
                        }
                else:
                    fetched_metrics[metric_name] = {
                        "value": "N/A",
                        "note": "No metrics returned.",
                        "status": "no_data"
                    }
            
            except HttpResponseError as e:
                # Classify HTTP errors
                if e.status_code == 403:
                    logger.error(f"Azure permission error for {metric_name}: {e}")
                    note = f"Permission denied (403)"
                    status = "permission_error"
                
                elif e.status_code == 429:
                    logger.warning(f"Azure rate limit for {metric_name}: {e}")
                    note = f"Rate limited (429)"
                    status = "rate_limited"
                
                elif e.status_code in (500, 502, 503, 504):
                    logger.warning(f"Azure service error for {metric_name}: {e.status_code}")
                    note = f"Service error ({e.status_code})"
                    status = "service_error"
                
                else:
                    logger.warning(f"Azure HTTP error for {metric_name}: {e.status_code}")
                    note = f"HTTP error ({e.status_code})"
                    status = "error"
                
                fetched_metrics[metric_name] = {
                    "value": "N/A",
                    "note": note,
                    "status": status
                }
            
            except Exception as e:
                logger.error(f"Unexpected error fetching Azure metric {metric_name}: {e}")
                fetched_metrics[metric_name] = {
                    "value": "N/A",
                    "note": f"Unexpected error: {str(e)}",
                    "status": "error"
                }
        
        return fetched_metrics
