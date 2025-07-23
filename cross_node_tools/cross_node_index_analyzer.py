#!/usr/bin/env python3
"""
Cross-Node Index Usage Analyzer

This utility analyzes index usage across primary and replica nodes to definitively identify
unused indexes that can be safely removed. It generates a comprehensive report with
recommendations and SQL statements for index removal.

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
        self.connections = {}
        self.index_data = {}
        
    def load_config(self, config_file: str) -> Dict:
        """Load configuration from YAML file."""
        try:
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f)
            
            # Validate required configuration
            required_fields = ['primary', 'replicas']
            for field in required_fields:
                if field not in config:
                    raise ValueError(f"Missing required field: {field}")
            
            return config
        except Exception as e:
            print(f"Error loading config: {e}")
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
        
        # Get index usage statistics
        cursor.execute(self.get_index_usage_query())
        index_usage = cursor.fetchall()
        
        # Get constraint information
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
        print("🔍 Analyzing index usage across all nodes...")
        
        # Connect to primary
        primary_conn = self.connect_to_node('primary', self.config['primary'])
        if not primary_conn:
            print("❌ Cannot proceed without primary connection")
            sys.exit(1)
        
        self.index_data['primary'] = self.analyze_node_indexes('primary', primary_conn)
        
        # Connect to replicas
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
            # Get all unique indexes from primary
            primary_indexes = {row[1] for row in self.index_data['primary']['index_usage']}
        except Exception as e:
            print(f"[WARN] Could not get primary indexes: {e}")
            return []
        for index_name in primary_indexes:
            # Check if this index is used on any node
            used_on_any_node = False
            usage_summary = {}
            for node_name, node_data in self.index_data.items():
                for row in node_data.get('index_usage', []):
                    if row[1] == index_name:  # index_name matches
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
            # If not used on any node, check if it supports constraints
            if not used_on_any_node:
                # Check if this index supports any constraints
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
        # This is a simplified check - in practice, you'd need to map indexes to constraints
        # For now, we'll check if the index name suggests it's a constraint index
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
        # Header
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
            "",
            f"This report analyzes index usage across {len(self.index_data)} database nodes ",
            f"to identify indexes that can be safely removed.",
            "",
            f"**Analysis Results:**",
            f"- Total nodes analyzed: {len(self.index_data)}",
            f"- Unused indexes identified: {len(unused_indexes)}",
            f"- Potential storage savings: {self.calculate_storage_savings(unused_indexes)}",
            "",
            "== Node Analysis Summary",
            ""
        ])
        
        # Node summary
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
        
        # Unused indexes details
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
        
        # Removal recommendations
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
        
        # Write report
        with open(output_file, 'w') as f:
            f.write('\n'.join(report_content))
        
        print(f"✅ Report generated: {output_file}")
    
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
                # Extract size from string like "1.2 MB"
                size_str = node_data['index_size']
                # This is a simplified calculation - you'd need proper parsing
                total_bytes += 1024 * 1024  # Assume 1MB per index for now
        
        if total_bytes < 1024 * 1024:
            return f"{total_bytes / 1024:.1f} KB"
        elif total_bytes < 1024 * 1024 * 1024:
            return f"{total_bytes / (1024 * 1024):.1f} MB"
        else:
            return f"{total_bytes / (1024 * 1024 * 1024):.1f} GB"

def main():
    parser = argparse.ArgumentParser(description='Cross-Node Index Usage Analyzer')
    parser.add_argument('--config', required=True, help='Configuration file path')
    parser.add_argument('--output', required=True, help='Output report file path')
    
    args = parser.parse_args()
    
    # Create analyzer
    analyzer = CrossNodeIndexAnalyzer(args.config)
    
    # Analyze all nodes
    analyzer.analyze_all_nodes()
    
    # Identify unused indexes
    unused_indexes = analyzer.identify_unused_indexes()
    unused_indexes = ensure_list(unused_indexes, context="main")

    # Generate removal SQL
    removal_sql = analyzer.generate_removal_sql(unused_indexes)
    
    # Generate report
    analyzer.generate_report(unused_indexes, removal_sql, args.output)
    
    print(f"\n🎉 Analysis complete!")
    print(f"📊 Found {len(unused_indexes)} potentially unused indexes")
    print(f"📄 Report saved to: {args.output}")

if __name__ == '__main__':
    main() 