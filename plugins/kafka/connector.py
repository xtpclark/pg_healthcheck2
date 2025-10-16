import json
import logging

from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType
from kafka.structs import TopicPartition

logger = logging.getLogger(__name__)


class KafkaConnector:
    """Handles all direct communication with Kafka."""

    def __init__(self, settings):
        self.settings = settings
        self.admin_client = None
        self.version_info = {}

    def connect(self):
        """Establishes a connection to the Kafka cluster."""
        try:
            bootstrap_servers = self.settings.get('bootstrap_servers', ['localhost:9092'])

            # Handle both list and string formats for bootstrap_servers
            if isinstance(bootstrap_servers, str):
                bootstrap_servers = [s.strip() for s in bootstrap_servers.split(',')]

            self.admin_client = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers,
                client_id='healthcheck_client',
                request_timeout_ms=30000
            )

            # Get version/broker info
            self.version_info = self._get_version_info()

            print(f"✅ Successfully connected to Kafka.")
            print(f"   - Brokers: {self.version_info.get('broker_count', 'N/A')}")

        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise ConnectionError(f"Could not connect to Kafka: {e}")

    def disconnect(self):
        """Closes the connection."""
        if self.admin_client:
            try:
                self.admin_client.close()
                print(f"🔌 Disconnected from Kafka.")
            except Exception as e:
                logger.warning(f"Error during disconnect: {e}")
            finally:
                self.admin_client = None

    def close(self):
        """Alias for disconnect() for DB-API 2.0 compatibility."""
        self.disconnect()

    def _get_version_info(self):
        """Fetches broker version information using the internal cluster state."""
        try:
            # NOTE: Accessing internal _client.cluster as it's a reliable way
            # to get the count of known brokers without a separate public API call.
            cluster_metadata = self.admin_client._client.cluster
            brokers = cluster_metadata.brokers()
            return {
                'version_string': 'Kafka (Version API not supported by client)',
                'broker_count': len(brokers) if brokers else 0
            }
        except Exception as e:
            logger.warning(f"Could not fetch broker/version info: {e}")
            return {'version_string': 'Unknown', 'broker_count': 0}

    def get_db_metadata(self):
        """Fetches cluster-level metadata."""
        try:
            cluster_info = self.admin_client.describe_cluster()
            cluster_id = cluster_info.get('cluster_id', 'Unknown')
            return {
                'version': self.version_info.get('version_string', 'N/A'),
                'db_name': f"Cluster ID: {cluster_id}"
            }
        except Exception as e:
            logger.warning(f"Could not fetch cluster metadata: {e}")
            return {'version': 'N/A', 'db_name': 'N/A'}

    def execute_query(self, query, params=None, return_raw=False):
        """
        Executes a Kafka admin operation based on a JSON query.
        Supported operations:
        - list_topics
        - describe_topics
        - list_consumer_groups
        - describe_consumer_groups
        - consumer_lag
        - broker_config
        - topic_config
        - cluster_metadata
        - describe_log_dirs
        - list_consumer_group_offsets
        """
        try:
            query_obj = json.loads(query)
            operation = query_obj.get('operation')

            if operation == 'list_topics':
                return self._list_topics(return_raw)
            elif operation == 'describe_topics':
                return self._describe_topics(query_obj.get('topics', []), return_raw)
            elif operation == 'list_consumer_groups':
                return self._list_consumer_groups(return_raw)
            elif operation == 'describe_consumer_groups':
                return self._describe_consumer_groups(query_obj.get('group_ids', []), return_raw)
            elif operation == 'consumer_lag':
                group_id = query_obj.get('group_id')
                if not group_id:
                    raise ValueError("'consumer_lag' operation requires a 'group_id'")
                if group_id == '*':
                    return self._get_all_consumer_lag(return_raw)
                return self._get_consumer_lag(group_id, return_raw)
            elif operation == 'broker_config':
                broker_id = query_obj.get('broker_id')
                if broker_id is None:
                    raise ValueError("'broker_config' operation requires a 'broker_id'")
                return self._get_broker_config(broker_id, return_raw)
            elif operation == 'topic_config':
                topic = query_obj.get('topic')
                if not topic:
                    raise ValueError("'topic_config' operation requires a 'topic'")
                return self._get_topic_config(topic, return_raw)
            elif operation == 'cluster_metadata':
                return self._get_cluster_metadata(return_raw)
            elif operation == 'describe_log_dirs':
                broker_ids = query_obj.get('broker_ids', [])
                return self._describe_log_dirs(broker_ids, return_raw)
            elif operation == 'list_consumer_group_offsets':
                group_id = query_obj.get('group_id')
                if not group_id:
                    raise ValueError("'list_consumer_group_offsets' operation requires a 'group_id'")
                return self._list_consumer_group_offsets(group_id, return_raw)
            else:
                raise ValueError(f"Unsupported operation: {operation}")

        except json.JSONDecodeError as e:
            msg = f"[ERROR]\n====\nInvalid JSON query: {e}\n====\n"
            logger.error(msg)
            return (msg, {'error': str(e)}) if return_raw else msg
        except Exception as e:
            msg = f"[ERROR]\n====\nOperation failed: {e}\n====\n"
            logger.error(msg, exc_info=True)
            return (msg, {'error': str(e)}) if return_raw else msg

    def _list_topics(self, return_raw=False):
        """Lists all user-visible topics."""
        topics = self.admin_client.list_topics()
        user_topics = sorted([t for t in topics if not t.startswith('__')])
        raw = {'topics': user_topics, 'count': len(user_topics)}
        if not user_topics:
            formatted = "[NOTE]\n====\nNo user topics found.\n====\n"
        else:
            formatted = f"User Topics ({len(user_topics)}):\n" + "\n".join(f"  - {t}" for t in user_topics)
        return (formatted, raw) if return_raw else formatted

    def _describe_topics(self, topics, return_raw=False):
        """Gets detailed information about topics using cluster metadata."""
        cluster = self.admin_client._client.cluster
        cluster.request_update() # Ensure metadata is fresh
        
        target_topics = topics or list(cluster.topics(exclude_internal_topics=True))
        raw_results = []

        for topic_name in sorted(target_topics):
            partitions = cluster.partitions_for_topic(topic_name)
            if not partitions:
                continue

            under_replicated = 0
            tp_example = TopicPartition(topic_name, next(iter(partitions)))
            replication_factor = len(cluster.replicas(tp_example))

            for p_id in partitions:
                tp = TopicPartition(topic_name, p_id)
                if len(cluster.in_sync_replicas(tp)) < len(cluster.replicas(tp)):
                    under_replicated += 1
            
            raw_results.append({
                'topic': topic_name, 'partitions': len(partitions),
                'replication_factor': replication_factor, 'under_replicated_partitions': under_replicated
            })

        if not raw_results:
            formatted = "[NOTE]\n====\nNo topics found or metadata available.\n====\n"
        else:
            formatted = "|===\n|Topic|Partitions|Replication Factor|Under-Replicated\n"
            for t in raw_results:
                formatted += f"|{t['topic']}|{t['partitions']}|{t['replication_factor']}|{t['under_replicated_partitions']}\n"
            formatted += "|===\n"
        return (formatted, raw_results) if return_raw else formatted

    def _list_consumer_groups(self, return_raw=False):
        """Lists all consumer groups."""
        groups = self.admin_client.list_consumer_groups()
        raw = [{'group_id': g[0], 'protocol_type': g[1]} for g in groups]
        if not groups:
            formatted = "[NOTE]\n====\nNo consumer groups found.\n====\n"
        else:
            formatted = f"Consumer Groups ({len(groups)}):\n" + "\n".join(f"  - {g[0]} ({g[1]})" for g in sorted(groups))
        return (formatted, raw) if return_raw else formatted

    def _describe_consumer_groups(self, group_ids, return_raw=False):
        """Gets detailed information about consumer groups."""
        target_groups = group_ids or [g[0] for g in self.admin_client.list_consumer_groups()]
        if not target_groups:
            return ("[NOTE]\n====\nNo consumer groups to describe.\n====\n", []) if return_raw else "[NOTE]\n====\nNo consumer groups to describe.\n====\n"

        descriptions = self.admin_client.describe_consumer_groups(target_groups)
        raw_results = []
        for desc in descriptions:
            if desc.error_code == 0:
                raw_results.append({
                    'group_id': desc.group, 'state': desc.state,
                    'protocol_type': desc.protocol_type, 'members': len(desc.members)
                })
        
        if not raw_results:
            formatted = "[NOTE]\n====\nCould not describe any of the specified groups.\n====\n"
        else:
            formatted = "|===\n|Group ID|State|Members|Protocol\n"
            for g in sorted(raw_results, key=lambda x: x['group_id']):
                formatted += f"|{g['group_id']}|{g['state']}|{g['members']}|{g['protocol_type']}\n"
            formatted += "|===\n"
        return (formatted, raw_results) if return_raw else formatted

    def _get_consumer_lag(self, group_id, return_raw=False):
        """Calculates consumer lag for a specific group efficiently."""
        try:
            committed_offsets = self.admin_client.list_consumer_group_offsets(group_id)
            if not committed_offsets:
                msg = f"[NOTE]\n====\nNo committed offsets found for group '{group_id}'.\n====\n"
                return (msg, {}) if return_raw else msg

            partitions = list(committed_offsets.keys())
            consumer = KafkaConsumer(bootstrap_servers=self.settings.get('bootstrap_servers'))
            end_offsets = consumer.end_offsets(partitions)
            consumer.close()

            lag_data = []
            for partition, offset_meta in committed_offsets.items():
                committed = offset_meta.offset
                end = end_offsets.get(partition, 0)
                lag = max(0, end - committed)
                lag_data.append({
                    'group_id': group_id, 'topic': partition.topic, 'partition': partition.partition,
                    'current_offset': committed, 'log_end_offset': end, 'lag': lag
                })
            
            raw = {'group_id': group_id, 'details': lag_data, 'total_lag': sum(d['lag'] for d in lag_data)}
            formatted = f"Consumer Group: {group_id}\nTotal Lag: {raw['total_lag']} messages\n\n"
            formatted += "|===\n|Topic|Partition|Current Offset|End Offset|Lag\n"
            for item in sorted(lag_data, key=lambda x: (x['topic'], x['partition'])):
                formatted += f"|{item['topic']}|{item['partition']}|{item['current_offset']}|{item['log_end_offset']}|{item['lag']}\n"
            formatted += "|===\n"
            return (formatted, raw) if return_raw else formatted
        except Exception as e:
            msg = f"[ERROR]\n====\nFailed to calculate consumer lag for {group_id}: {e}\n====\n"
            logger.error(msg, exc_info=True)
            return (msg, {'error': str(e)}) if return_raw else msg

    def _get_all_consumer_lag(self, return_raw=False):
        """Calculates lag for all consumer groups."""
        groups = [g[0] for g in self.admin_client.list_consumer_groups()]
        if not groups:
            return ("[NOTE]\n====\nNo consumer groups found.\n====\n", []) if return_raw else "[NOTE]\n====\nNo consumer groups found.\n====\n"

        all_lags = []
        total_cluster_lag = 0
        for group_id in groups:
            _, raw_lag = self._get_consumer_lag(group_id, return_raw=True)
            if raw_lag and 'details' in raw_lag:
                # Add group_id to each record for better context
                for detail in raw_lag['details']:
                    detail['group_id'] = group_id
                all_lags.extend(raw_lag['details'])
                total_cluster_lag += raw_lag.get('total_lag', 0)
        
        raw = {'group_lags': all_lags, 'total_lag': total_cluster_lag}
        formatted = f"Total Lag Across All Groups: {total_cluster_lag} messages\n\n"
        formatted += "|===\n|Group ID|Topic|Partition|Lag\n"
        for item in sorted(all_lags, key=lambda x: (x.get('group_id', ''), x['topic'], x['partition'])):
            formatted += f"|{item.get('group_id')}|{item['topic']}|{item['partition']}|{item['lag']}\n"
        formatted += "|===\n"
        return (formatted, raw) if return_raw else formatted

    def _get_config(self, resource_type, resource_name, return_raw=False):
        """Generic helper to get broker or topic configuration."""
        resource = ConfigResource(resource_type, resource_name)
        try:
            futures = self.admin_client.describe_configs([resource])
            future_result = futures[resource].get(timeout=10)
            error_code, config_entries = future_result

            if error_code != 0:
                raise Exception(f"Describe_configs error code {error_code} for {resource_name}")

            raw = {
                'name': resource_name,
                'configs': {e.name: e.value for e in config_entries}
            }
            formatted = f"{resource_type.name.title()} '{resource_name}' Configuration (Top 25):\n\n"
            formatted += "|===\n|Config Key|Value\n"
            for key, val in sorted(raw['configs'].items())[:25]:
                formatted += f"|{key}|{str(val)[:80]}\n"
            formatted += "|===\n"
            return (formatted, raw) if return_raw else formatted
        except Exception as e:
            msg = f"[ERROR]\n====\nFailed to get config for {resource_type.name} {resource_name}: {e}\n====\n"
            logger.error(msg, exc_info=True)
            return (msg, {'error': str(e)}) if return_raw else msg
    
    def _get_broker_config(self, broker_id, return_raw=False):
        """Gets configuration for a specific broker."""
        return self._get_config(ConfigResourceType.BROKER, str(broker_id), return_raw)

    def _get_topic_config(self, topic_name, return_raw=False):
        """Gets configuration for a specific topic."""
        return self._get_config(ConfigResourceType.TOPIC, topic_name, return_raw)

    def _get_cluster_metadata(self, return_raw=False):
        """Gets high-level cluster metadata."""
        try:
            info = self.admin_client.describe_cluster()
            raw = {
                'cluster_id': info.get('cluster_id'),
                'controller_id': info.get('controller_id'),
                'brokers': [{
                    'id': b.get('node_id'), 'host': b.get('host'), 'port': b.get('port')
                } for b in info.get('brokers', [])]
            }
            formatted = f"Cluster ID: {raw['cluster_id']}\nController: Broker {raw['controller_id']}\n"
            if raw['brokers']:
                formatted += f"Brokers ({len(raw['brokers'])}):\n\n|===\n|Broker ID|Address\n"
                for b in sorted(raw['brokers'], key=lambda x: x['id']):
                    formatted += f"|{b['id']}|{b['host']}:{b['port']}\n"
                formatted += "|===\n"
            return (formatted, raw) if return_raw else formatted
        except Exception as e:
            msg = f"[ERROR]\n====\nFailed to get cluster metadata: {e}\n====\n"
            logger.error(msg, exc_info=True)
            return (msg, {'error': str(e)}) if return_raw else msg

    def _describe_log_dirs(self, broker_ids, return_raw=False):
        """Gets log directory information from brokers, including partition sizes."""
        target_brokers = broker_ids or [b.get('id') for b in self.get_db_metadata().get('brokers', [])]
        if not target_brokers:
             return ("[NOTE]\n====\nNo brokers specified or found to describe log directories.\n====\n", {}) if return_raw else "[NOTE]\n====\nNo brokers specified or found to describe log directories.\n====\n"
        try:
            futures = self.admin_client.describe_log_dirs(target_brokers)
            results = self.admin_client._wait_for_futures(futures)
            
            raw_data = []
            for broker_id, future_result in results.items():
                log_dirs_info = future_result.get()
                for log_dir, info in log_dirs_info.items():
                    for topic_partition, partition_info in info.topics.items():
                        raw_data.append({
                            'broker_id': broker_id,
                            'log_dir': log_dir,
                            'topic': topic_partition[0],
                            'partition': topic_partition[1],
                            'size_bytes': partition_info.size,
                            'offset_lag': partition_info.offset_lag
                        })

            if not raw_data:
                formatted = "[NOTE]\n====\nNo log directory information returned from brokers.\n====\n"
            else:
                formatted = "|===\n|Broker ID|Topic|Partition|Size (MB)\n"
                for item in sorted(raw_data, key=lambda x: (-x['size_bytes'], x['broker_id'], x['topic'])):
                     size_mb = round(item['size_bytes'] / (1024 * 1024), 2)
                     formatted += f"|{item['broker_id']}|{item['topic']}|{item['partition']}|{size_mb}\n"
                formatted += "|===\n"
            return (formatted, raw_data) if return_raw else formatted
        except Exception as e:
            msg = f"[ERROR]\n====\nFailed to describe log directories: {e}\n====\n"
            logger.error(msg, exc_info=True)
            return (msg, {'error': str(e)}) if return_raw else msg
            
    def _list_consumer_group_offsets(self, group_id, return_raw=False):
        """Lists the raw committed offsets for a consumer group."""
        try:
            offsets = self.admin_client.list_consumer_group_offsets(group_id)
            if not offsets:
                msg = f"[NOTE]\n====\nNo committed offsets found for group '{group_id}'.\n====\n"
                return (msg, {}) if return_raw else msg

            raw_data = [{
                'group_id': group_id, 'topic': tp.topic, 'partition': tp.partition,
                'offset': meta.offset, 'metadata': meta.metadata
            } for tp, meta in offsets.items()]

            formatted = f"Committed Offsets for Group: {group_id}\n\n"
            formatted += "|===\n|Topic|Partition|Committed Offset\n"
            for item in sorted(raw_data, key=lambda x: (x['topic'], x['partition'])):
                formatted += f"|{item['topic']}|{item['partition']}|{item['offset']}\n"
            formatted += "|===\n"
            return (formatted, raw_data) if return_raw else formatted
        except Exception as e:
            msg = f"[ERROR]\n====\nFailed to list consumer group offsets for {group_id}: {e}\n====\n"
            logger.error(msg, exc_info=True)
            return (msg, {'error': str(e)}) if return_raw else msg
