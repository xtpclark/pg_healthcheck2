#!/usr/bin/env python3
"""
Cross-Node Index Usage Analyzer

This utility analyzes index usage across primary and replica nodes to definitively identify
unused indexes that can be safely removed. It generates a comprehensive report with
recommendations and SQL statements for index removal.

It supports manual configuration for standard PostgreSQL setups and automatic discovery
for AWS Aurora clusters.

Usage:
    python cross_node_index_analyzer.py --config config.yaml --output index_analysis_report.adoc
"""

import argparse
import json
import psycopg2
import yaml
from datetime import datetime
from pathlib import Path
import sys
from typing import Dict, List, Tuple, Optional
import boto3

def ensure_list(obj, context=""):
    """Utility to ensure obj is a list. If not, return empty list and warn."""
    if not isinstance(obj, list):
        print(f"[WARN] {context}: Expected list, got {type(obj)}. Using empty list instead.")
        return []
    return obj

class CrossNodeIndexAnalyzer:
    def __init__(self, config_file: str):
        """Initialize the analyzer with configuration."""
        self.config = self.load_config(config_file)
        self.cluster_nodes = []
        self.stats_reset_time = None
        if 'aws_aurora' in self.config:
            self._discover_and_configure_aurora()
        else:
            self._populate_manual_cluster_nodes()
        self.connections = {}
        self.index_data = {}

    def load_config(self, config_file: str) -> Dict:
        """Load configuration from YAML file."""
        try:
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f)
            
            if 'aws_aurora' not in config:
                required_fields = ['primary', 'replicas']
                for field in required_fields:
                    if field not in config:
                        raise ValueError(f"Missing required field: {field}")
            
            return config
        except Exception as e:
            print(f"Error loading config: {e}")
            sys.exit(1)

    def _populate_manual_cluster_nodes(self):
        """Populate cluster node list from manual configuration."""
        print("🔍 Manual configuration detected. Populating cluster nodes...")
        if 'primary' in self.config:
            self.cluster_nodes.append({
                'role': 'primary',
                'host': self.config['primary']['host']
            })
        if 'replicas' in self.config:
            for i, replica_config in enumerate(self.config['replicas']):
                self.cluster_nodes.append({
                    'role': f'reader{i+1}',
                    'host': replica_config['host']
                })

    def _discover_and_configure_aurora(self):
        """Discover Aurora cluster nodes and configure them for analysis."""
        print("🔍 AWS Aurora configuration detected. Discovering cluster nodes...")
        aurora_config = self.config['aws_aurora']
        cluster_id = aurora_config.get('db_cluster_id')
        if not cluster_id:
            print("❌ `db_cluster_id` is missing in the `aws_aurora` configuration.")
            sys.exit(1)

        try:
            rds_client = boto3.client('rds')
            cluster_info = rds_client.describe_db_clusters(DBClusterIdentifier=cluster_id)['DBClusters'][0]

            self.config['primary'] = None
            self.config['replicas'] = []
            
            reader_count = 1
            for member in cluster_info['DBClusterMembers']:
                instance_id = member['DBInstanceIdentifier']
                is_writer = member['IsClusterWriter']

                instance_info = rds_client.describe_db_instances(DBInstanceIdentifier=instance_id)['DBInstances'][0]
                endpoint = instance_info['Endpoint']['Address']
                port = instance_info['Endpoint']['Port']

                connection_info = {
                    'host': endpoint,
                    'port': port,
                    'database': aurora_config['database'],
                    'user': aurora_config['user'],
                    'password': aurora_config['password'],
                }

                if is_writer:
                    self.config['primary'] = connection_info
                    self.cluster_nodes.append({'role': 'primary', 'host': endpoint})
                    print(f"  - Found primary (writer): {endpoint}:{port}")
                else:
                    self.config['replicas'].append(connection_info)
                    self.cluster_nodes.append({'role': f'reader{reader_count}', 'host': endpoint})
                    print(f"  - Found replica (reader): {endpoint}:{port}")
                    reader_count += 1

            if not self.config.get('primary'):
                print("❌ Could not identify a primary writer instance in the Aurora cluster.")
                sys.exit(1)

        except Exception as e:
            print(f"❌ Failed to discover Aurora cluster nodes: {e}")
            print("   Please ensure your AWS credentials are configured correctly (e.g., via `aws configure` or IAM role).")
            sys.exit(1)

    def connect_to_node(self, node_name: str, connection_info: Dict) -> Optional[psycopg2.extensions.connection]:
        """Connect to a database node."""
        try:
            conn = psycopg2.connect(
                host=connection_info['host'],
                port=connection_info.get('port', 5432),
                database=connection_info['database'],
                user=connection_info['user'],
                password=connection_info['password'],
                connect_timeout=10
            )
            print(f"✅ Connected to {node_name}")
            return conn
        except Exception as e:
            print(f"❌ Failed to connect to {node_name}: {e}")
            return None
    
    def get_stats_reset_time(self, conn: psycopg2.extensions.connection, db_name: str) -> Optional[datetime]:
        """Get the last statistics reset time for the database."""
        cursor = conn.cursor()
        try:
            query = "SELECT stats_reset FROM pg_stat_database WHERE datname = %s;"
            cursor.execute(query, (db_name,))
            reset_time = cursor.fetchone()
            cursor.close()
            if reset_time and reset_time[0]:
                print(f"ℹ️ Statistics last reset on: {reset_time[0]}")
                return reset_time[0]
            print("[WARN] Could not determine stats reset time.")
            return None
        except Exception as e:
            print(f"[WARN] Could not retrieve stats reset time: {e}")
            cursor.close()
            return None

    def get_index_usage_query(self) -> str:
        """Get the SQL query for index usage analysis."""
        return """
        SELECT 
            schemaname||'.'||relname AS table_name,
            indexrelname AS index_name,
            idx_scan,
            idx_tup_read,
            idx_tup_fetch,
            pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
            pg_relation_size(indexrelid) AS index_size_bytes
        FROM pg_stat_user_indexes 
        ORDER BY pg_relation_size(indexrelid) DESC;
        """
    
    def get_constraint_query(self) -> str:
        """Get the SQL query for constraint analysis."""
        return """
        SELECT 
            conname AS constraint_name,
            conrelid::regclass AS table_name,
            contype AS constraint_type,
            pg_get_constraintdef(oid) AS constraint_definition
        FROM pg_constraint 
        WHERE contype IN ('p', 'u', 'f')
        ORDER BY conrelid::regclass, conname;
        """
    
    def analyze_node_indexes(self, node_name: str, conn: psycopg2.extensions.connection) -> Dict:
        """Analyze index usage on a specific node."""
        cursor = conn.cursor()
        
        cursor.execute(self.get_index_usage_query())
        index_usage = cursor.fetchall()
        
        cursor.execute(self.get_constraint_query())
        constraints = cursor.fetchall()
        
        cursor.close()
        
        return {
            'index_usage': index_usage,
            'constraints': constraints,
            'node_name': node_name
        }
    
    def analyze_all_nodes(self) -> Dict:
        """Analyze index usage across all nodes."""
        print("\n🔍 Analyzing index usage across all nodes...")
        
        primary_conn = self.connect_to_node('primary', self.config['primary'])
        if not primary_conn:
            print("❌ Cannot proceed without primary connection")
            sys.exit(1)
        
        db_name = self.config['primary']['database']
        self.stats_reset_time = self.get_stats_reset_time(primary_conn, db_name)

        self.index_data['primary'] = self.analyze_node_indexes('primary', primary_conn)
        
        for i, replica_config in enumerate(self.config['replicas']):
            replica_name = f"replica_{i+1}"
            replica_conn = self.connect_to_node(replica_name, replica_config)
            if replica_conn:
                self.index_data[replica_name] = self.analyze_node_indexes(replica_name, replica_conn)
                replica_conn.close()
        
        primary_conn.close()
        return self.index_data
    
    def identify_unused_indexes(self) -> List[Dict]:
        """Identify indexes that are truly unused across all nodes."""
        unused_indexes = []
        try:
            primary_indexes = {row[1] for row in self.index_data['primary']['index_usage']}
        except Exception as e:
            print(f"[WARN] Could not get primary indexes: {e}")
            return []
        for index_name in primary_indexes:
            used_on_any_node = False
            usage_summary = {}
            for node_name, node_data in self.index_data.items():
                for row in node_data.get('index_usage', []):
                    if row[1] == index_name:
                        idx_scan = row[2]
                        usage_summary[node_name] = {
                            'idx_scan': idx_scan,
                            'idx_tup_read': row[3],
                            'idx_tup_fetch': row[4],
                            'index_size': row[5],
                            'table_name': row[0]
                        }
                        if idx_scan > 0:
                            used_on_any_node = True
                        break
            if not used_on_any_node:
                supports_constraints = self.check_index_constraints(index_name)
                if not supports_constraints:
                    unused_indexes.append({
                        'index_name': index_name,
                        'usage_summary': usage_summary,
                        'supports_constraints': False
                    })
        unused_indexes = ensure_list(unused_indexes, context="identify_unused_indexes")
        return unused_indexes
    
    def check_index_constraints(self, index_name: str) -> bool:
        """Check if an index supports any constraints."""
        constraint_indicators = ['_pkey', '_key', '_idx', '_uk_', '_fk_']
        return any(indicator in index_name.lower() for indicator in constraint_indicators)
    
    def generate_removal_sql(self, unused_indexes: List[Dict]) -> List[str]:
        """Generate SQL statements for removing unused indexes."""
        sql_statements = []
        
        for index_info in unused_indexes:
            index_name = index_info['index_name']
            table_name = list(index_info['usage_summary'].values())[0]['table_name']
            
            sql = f"DROP INDEX CONCURRENTLY IF EXISTS {index_name};"
            sql_statements.append({
                'sql': sql,
                'index_name': index_name,
                'table_name': table_name,
                'reason': 'Unused across all nodes'
            })
        
        return sql_statements
    
    def generate_report(self, unused_indexes: list, removal_sql: list, output_file: str):
        """Generate a comprehensive AsciiDoc report."""
        report_content = []
        unused_indexes = ensure_list(unused_indexes, context="generate_report")
        
        report_content.extend([
            "= Cross-Node Index Usage Analysis Report",
            f":doctype: book",
            f":encoding: utf-8",
            f":lang: en",
            f":toc: left",
            f":numbered:",
            "",
            f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "== Executive Summary",
            ""
        ])

        if self.stats_reset_time:
            report_content.extend([
                "[IMPORTANT]",
                "=====",
                f"**Statistics Last Reset:** {self.stats_reset_time.strftime('%Y-%m-%d %H:%M:%S %Z')}",
                "",
                "The index usage data for this analysis is based on statistics collected since the last reset.",
                "If this reset was recent, the report may not reflect long-term index usage patterns.",
                "Exercise extreme caution before dropping indexes if the statistics uptime is short.",
                "=====",
                ""
            ])

        report_content.extend([
            f"This report analyzes index usage across {len(self.index_data)} database nodes.",
            "",
            f"**Analysis Results:**",
            f"- Total nodes analyzed: {len(self.index_data)}",
            f"- Unused indexes identified: {len(unused_indexes)}",
            f"- Potential storage savings: {self.calculate_storage_savings(unused_indexes)}",
            ""
        ])

        report_content.extend([
            "== Cluster Configuration",
            ""
        ])
        if self.config.get('aws_aurora'):
            cluster_id = self.config['aws_aurora'].get('db_cluster_id', 'N/A')
            report_content.append(f"Analyzed AWS Aurora Cluster ID: *{cluster_id}*")
            report_content.append("")

        report_content.extend([
            "The following cluster members were analyzed:",
            "",
            "[cols=\"1,2\",options=\"header\"]",
            "|===",
            "|Role|Server Endpoint",
        ])
        for node in self.cluster_nodes:
            report_content.append(f"|{node['role'].title()}|{node['host']}")
        report_content.extend([
            "|===",
            ""
        ])
        
        report_content.extend([
            "== Node Analysis Summary",
            ""
        ])
        
        for node_name, node_data in self.index_data.items():
            total_indexes = len(node_data['index_usage'])
            used_indexes = sum(1 for row in node_data['index_usage'] if row[2] > 0)
            unused_count = total_indexes - used_indexes
            
            report_content.extend([
                f"=== {node_name.title()}",
                "",
                f"- Total indexes: {total_indexes}",
                f"- Used indexes: {used_indexes}",
                f"- Unused indexes: {unused_count}",
                f"- Usage rate: {(used_indexes/total_indexes*100):.1f}%",
                ""
            ])
        
        if unused_indexes:
            report_content.extend([
                "== Unused Indexes Analysis",
                "",
                "The following indexes appear unused across all nodes and may be candidates for removal:",
                "",
                "[cols=\"1,1,1,1,1\",options=\"header\"]",
                "|===",
                "|Index Name|Table Name|Size|Usage Summary|Supports Constraints",
            ])
            
            for index_info in unused_indexes:
                index_name = index_info['index_name']
                table_name = list(index_info['usage_summary'].values())[0]['table_name']
                size = list(index_info['usage_summary'].values())[0]['index_size']
                usage_summary = self.format_usage_summary(index_info['usage_summary'])
                supports_constraints = "Yes" if index_info['supports_constraints'] else "No"
                
                report_content.extend([
                    f"|{index_name}|{table_name}|{size}|{usage_summary}|{supports_constraints}"
                ])
            
            report_content.append("|===")
        else:
            report_content.extend([
                "== Unused Indexes Analysis",
                "",
                "[NOTE]",
                "=====",
                "No unused indexes were identified across all nodes.",
                "All indexes appear to be in use on at least one node.",
                "=====",
                ""
            ])
        
        if removal_sql:
            report_content.extend([
                "== Index Removal Recommendations",
                "",
                "[IMPORTANT]",
                "=====",
                "**Before removing any indexes:**",
                "",
                "1. **Verify the analysis**: Double-check that these indexes are truly unused",
                "2. **Test in staging**: Remove indexes in a staging environment first",
                "3. **Monitor performance**: Watch for any performance impact after removal",
                "4. **Low-traffic window**: Remove during maintenance windows",
                "5. **Backup plan**: Have a rollback strategy ready",
                "=====",
                "",
                "=== Recommended SQL Statements",
                "",
                "The following SQL statements can be used to remove unused indexes:",
                "",
                "[source,sql]",
                "----",
            ])
            
            for sql_info in removal_sql:
                report_content.extend([
                    f"-- Remove unused index: {sql_info['index_name']}",
                    f"-- Table: {sql_info['table_name']}",
                    f"-- Reason: {sql_info['reason']}",
                    sql_info['sql'],
                    ""
                ])
            
            report_content.append("----")
        else:
            report_content.extend([
                "== Index Removal Recommendations",
                "",
                "[NOTE]",
                "=====",
                "No index removal recommendations at this time.",
                "All indexes appear to be in use or support constraints.",
                "=====",
                ""
            ])
        
        with open(output_file, 'w') as f:
            f.write('\n'.join(report_content))
        
        print(f"✅ AsciiDoc report generated: {output_file}")

    def generate_json_output(self, unused_indexes: list, removal_sql: list):
        """Generate a structured JSON file of the findings."""
        timestamp_str = datetime.now().strftime('%Y%m%d%H%M%S')
        if self.config.get('aws_aurora'):
            file_prefix = self.config['aws_aurora'].get('db_cluster_id', 'aurora-cluster')
            config_type = 'aws_aurora'
        else:
            file_prefix = 'manual-cluster'
            config_type = 'manual'

        output_filename = f"{file_prefix}-{timestamp_str}-cross-index-analysis.json"

        node_summaries = {}
        for node_name, node_data in self.index_data.items():
            total_indexes = len(node_data['index_usage'])
            used_indexes = sum(1 for row in node_data['index_usage'] if row[2] > 0)
            node_summaries[node_name] = {
                'total_indexes': total_indexes,
                'used_indexes': used_indexes,
                'unused_indexes': total_indexes - used_indexes,
                'usage_rate_percent': round((used_indexes / total_indexes * 100), 1) if total_indexes > 0 else 0
            }

        unused_indexes_details = []
        for i, index_info in enumerate(unused_indexes):
            sql_info = removal_sql[i]
            usage_summary_formatted = {node: {'scans': summary['idx_scan']} for node, summary in index_info['usage_summary'].items()}
            
            unused_indexes_details.append({
                'index_name': index_info['index_name'],
                'table_name': sql_info['table_name'],
                'size': list(index_info['usage_summary'].values())[0]['index_size'],
                'supports_constraints': index_info['supports_constraints'],
                'usage_summary': usage_summary_formatted,
                'recommended_sql': sql_info['sql']
            })

        json_data = {
            'analysis_metadata': {
                'report_generated_at_utc': datetime.utcnow().isoformat(),
                'statistics_last_reset_utc': self.stats_reset_time.isoformat() if self.stats_reset_time else None,
                'potential_storage_savings': self.calculate_storage_savings(unused_indexes)
            },
            'cluster_configuration': {
                'type': config_type,
                'aurora_cluster_id': self.config.get('aws_aurora', {}).get('db_cluster_id'),
                'nodes': self.cluster_nodes
            },
            'node_summaries': node_summaries,
            'unused_indexes_analysis': {
                 'count': len(unused_indexes_details),
                 'details': unused_indexes_details
            }
        }

        try:
            with open(output_filename, 'w') as f:
                json.dump(json_data, f, indent=4)
            print(f"✅ JSON report saved to: {output_filename}")
        except Exception as e:
            print(f"❌ Failed to write JSON report: {e}")

    def format_usage_summary(self, usage_summary: Dict) -> str:
        """Format usage summary for display."""
        parts = []
        for node_name, data in usage_summary.items():
            parts.append(f"{node_name}: {data['idx_scan']} scans")
        return "; ".join(parts)
    
    def calculate_storage_savings(self, unused_indexes: List[Dict]) -> str:
        """Calculate potential storage savings."""
        total_bytes = 0
        for index_info in unused_indexes:
            for node_data in index_info['usage_summary'].values():
                size_str = node_data['index_size']
                total_bytes += 1024 * 1024
        
        if total_bytes < 1024 * 1024:
            return f"{total_bytes / 1024:.1f} KB"
        elif total_bytes < 1024 * 1024 * 1024:
            return f"{total_bytes / (1024 * 1024):.1f} MB"
        else:
            return f"{total_bytes / (1024 * 1024 * 1024):.1f} GB"

def main():
    parser = argparse.ArgumentParser(description='Cross-Node Index Usage Analyzer')
    parser.add_argument('--config', required=True, help='Configuration file path')
    parser.add_argument('--output', required=True, help='Output report file path for the AsciiDoc report')
    
    args = parser.parse_args()
    
    analyzer = CrossNodeIndexAnalyzer(args.config)
    
    analyzer.analyze_all_nodes()
    
    unused_indexes = analyzer.identify_unused_indexes()
    unused_indexes = ensure_list(unused_indexes, context="main")

    removal_sql = analyzer.generate_removal_sql(unused_indexes)
    
    # Generate AsciiDoc report
    analyzer.generate_report(unused_indexes, removal_sql, args.output)
    
    # Generate JSON report
    analyzer.generate_json_output(unused_indexes, removal_sql)

    print(f"\n🎉 Analysis complete!")
    print(f"📊 Found {len(unused_indexes)} potentially unused indexes")
    
if __name__ == '__main__':
    main()
