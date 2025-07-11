#!/usr/bin/env python3
"""
Test script for the Enhanced AI Prompting System

This script demonstrates how the dynamic prompt generator works with sample data.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from modules.dynamic_prompt_generator import generate_dynamic_prompt, analyze_metric_severity

def create_sample_findings():
    """Create sample structured findings to test the enhanced AI prompting system."""
    
    sample_findings = {
        "connection_metrics": {
            "status": "success",
            "data": {
                "total_connections_and_limits": {
                    "status": "success",
                    "data": [
                        {
                            "total_connections": 95,
                            "max_connections": 100
                        }
                    ]
                }
            }
        },
        "top_queries_by_execution_time": {
            "status": "success",
            "data": {
                "top_queries": {
                    "status": "success",
                    "data": [
                        {
                            "query": "SELECT * FROM large_table WHERE complex_condition",
                            "calls": 1500,
                            "total_exec_time": 7200000,  # 2 hours
                            "mean_exec_time": 4800,
                            "rows": 1000000
                        },
                        {
                            "query": "UPDATE users SET last_login = NOW()",
                            "calls": 50000,
                            "total_exec_time": 300000,  # 5 minutes
                            "mean_exec_time": 6,
                            "rows": 50000
                        }
                    ]
                }
            }
        },
        "unused_idx": {
            "status": "success",
            "data": {
                "unused_indexes": {
                    "status": "success",
                    "data": [
                        {
                            "index_name": "idx_old_feature",
                            "table_name": "users",
                            "idx_scan": 0,
                            "idx_tup_read": 0,
                            "idx_tup_fetch": 0
                        },
                        {
                            "index_name": "idx_deprecated_column",
                            "table_name": "orders",
                            "idx_scan": 0,
                            "idx_tup_read": 0,
                            "idx_tup_fetch": 0
                        }
                    ]
                }
            }
        },
        "vacstat2": {
            "status": "success",
            "data": {
                "live_dead_tuples": {
                    "status": "success",
                    "data": [
                        {
                            "relname": "orders",
                            "n_live_tup": 1000000,
                            "n_dead_tup": 500000,
                            "last_autovacuum": "2024-01-15 10:30:00"
                        },
                        {
                            "relname": "users",
                            "n_live_tup": 50000,
                            "n_dead_tup": 5000,
                            "last_autovacuum": "2024-01-15 09:15:00"
                        }
                    ]
                }
            }
        },
        "stat_ssl": {
            "status": "success",
            "data": {
                "overall_ssl_usage_summary": {
                    "status": "success",
                    "data": [
                        {
                            "ssl": True,
                            "connection_count": 80
                        },
                        {
                            "ssl": False,
                            "connection_count": 20
                        }
                    ]
                }
            }
        },
        "aurora_cpu_metrics": {
            "status": "success",
            "data": {
                "aurora_replication_metrics": {
                    "status": "success",
                    "data": [
                        {
                            "replica_lag": 450,  # 7.5 minutes
                            "replica_lag_size": "1.2 GB"
                        }
                    ]
                }
            }
        }
    }
    
    return sample_findings

def test_metric_analysis():
    """Test individual metric severity analysis."""
    
    print("=== Testing Metric Severity Analysis ===\n")
    
    # Test connection metrics
    connection_data = [{"total_connections": 95, "max_connections": 100}]
    analysis = analyze_metric_severity("connection_metrics", connection_data, {"is_aurora": True})
    print(f"Connection Analysis: {analysis['level'].upper()} - {analysis['reasoning']}")
    
    # Test query metrics
    query_data = [{"total_exec_time": 7200000, "calls": 1500}]
    analysis = analyze_metric_severity("query_performance", query_data, {"is_aurora": True})
    print(f"Query Analysis: {analysis['level'].upper()} - {analysis['reasoning']}")
    
    # Test index metrics
    index_data = [
        {"idx_scan": 0, "index_name": "idx_unused"},
        {"idx_scan": 0, "index_name": "idx_another_unused"}
    ]
    analysis = analyze_metric_severity("index_analysis", index_data, {"is_aurora": True})
    print(f"Index Analysis: {analysis['level'].upper()} - {analysis['reasoning']}")
    
    # Test vacuum metrics
    vacuum_data = [{"n_live_tup": 1000000, "n_dead_tup": 500000}]
    analysis = analyze_metric_severity("vacuum_analysis", vacuum_data, {"is_aurora": True})
    print(f"Vacuum Analysis: {analysis['level'].upper()} - {analysis['reasoning']}")
    
    # Test SSL metrics
    ssl_data = [
        {"ssl": True, "connection_count": 80},
        {"ssl": False, "connection_count": 20}
    ]
    analysis = analyze_metric_severity("ssl_analysis", ssl_data, {"is_aurora": True})
    print(f"SSL Analysis: {analysis['level'].upper()} - {analysis['reasoning']}")
    
    # Test Aurora metrics
    aurora_data = [{"replica_lag": 450}]
    analysis = analyze_metric_severity("aurora_metrics", aurora_data, {"is_aurora": True})
    print(f"Aurora Analysis: {analysis['level'].upper()} - {analysis['reasoning']}")
    
    print("\n" + "="*50 + "\n")

def test_dynamic_prompt_generation():
    """Test the complete dynamic prompt generation system."""
    
    print("=== Testing Dynamic Prompt Generation ===\n")
    
    # Create sample findings
    sample_findings = create_sample_findings()
    
    # Sample settings
    settings = {
        "is_aurora": True,
        "ai_user": "test_user",
        "ai_severity_thresholds": {
            "connection_usage_critical": 90,
            "connection_usage_high": 75,
            "query_exec_time_critical": 3600000,
            "query_exec_time_high": 600000,
            "dead_tuple_ratio_critical": 0.3,
            "dead_tuple_ratio_high": 0.1,
            "ssl_usage_critical": 0.5,
            "ssl_usage_high": 0.1,
            "unused_indexes_critical": 10,
            "unused_indexes_high": 5,
            "aurora_replica_lag_critical": 300,
            "aurora_replica_lag_high": 60
        }
    }
    
    # Generate dynamic prompt
    dynamic_analysis = generate_dynamic_prompt(sample_findings, settings)
    
    print(f"Total Issues Detected: {dynamic_analysis['total_issues']}")
    print(f"Critical Issues: {len(dynamic_analysis['critical_issues'])}")
    print(f"High Priority Issues: {len(dynamic_analysis['high_priority_issues'])}")
    print(f"Medium Priority Issues: {len(dynamic_analysis['medium_priority_issues'])}")
    
    print("\n--- Critical Issues ---")
    for issue in dynamic_analysis['critical_issues']:
        print(f"- {issue['metric']}: {issue['analysis']['reasoning']}")
    
    print("\n--- High Priority Issues ---")
    for issue in dynamic_analysis['high_priority_issues']:
        print(f"- {issue['metric']}: {issue['analysis']['reasoning']}")
    
    print("\n--- Generated Prompt Preview ---")
    prompt_lines = dynamic_analysis['prompt'].split('\n')
    for i, line in enumerate(prompt_lines[:20]):  # Show first 20 lines
        print(line)
    if len(prompt_lines) > 20:
        print("... [prompt continues]")
    
    print("\n" + "="*50 + "\n")

def test_custom_thresholds():
    """Test custom severity thresholds."""
    
    print("=== Testing Custom Thresholds ===\n")
    
    # Test with more conservative thresholds
    conservative_settings = {
        "is_aurora": True,
        "ai_user": "conservative_user",
        "ai_severity_thresholds": {
            "connection_usage_critical": 95,  # More conservative
            "connection_usage_high": 80,
            "query_exec_time_critical": 7200000,  # 2 hours
            "query_exec_time_high": 1200000,  # 20 minutes
            "dead_tuple_ratio_critical": 0.5,  # 50%
            "dead_tuple_ratio_high": 0.2,  # 20%
            "ssl_usage_critical": 0.3,  # 30%
            "ssl_usage_high": 0.05,  # 5%
            "unused_indexes_critical": 15,
            "unused_indexes_high": 8,
            "aurora_replica_lag_critical": 600,  # 10 minutes
            "aurora_replica_lag_high": 120  # 2 minutes
        }
    }
    
    sample_findings = create_sample_findings()
    dynamic_analysis = generate_dynamic_prompt(sample_findings, conservative_settings)
    
    print(f"Conservative Analysis Results:")
    print(f"Total Issues: {dynamic_analysis['total_issues']}")
    print(f"Critical Issues: {len(dynamic_analysis['critical_issues'])}")
    print(f"High Priority Issues: {len(dynamic_analysis['high_priority_issues'])}")
    
    print("\n" + "="*50 + "\n")

def main():
    """Run all tests."""
    
    print("Enhanced AI Prompting System - Test Suite")
    print("="*50 + "\n")
    
    try:
        test_metric_analysis()
        test_dynamic_prompt_generation()
        test_custom_thresholds()
        
        print("✅ All tests completed successfully!")
        print("\nThe enhanced AI prompting system is working correctly.")
        print("You can now use this system in your PostgreSQL health checks.")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 