#!/usr/bin/env python3
"""
Trend Analysis Viewer Module

This module provides functionality to view and analyze trends from stored health check data.
It can generate reports showing improvements, degradations, or stability over time.
"""

import json
from datetime import datetime, timedelta
from .trend_analysis_storage import TrendAnalysisStorage

def run_trend_viewer(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Generate trend analysis report from stored health check data.
    """
    adoc_content = ["=== Trend Analysis Report", "Analysis of health check trends over time."]
    structured_data = {}
    
    # Check if trend storage is enabled
    if not settings.get('trend_storage_enabled', False):
        adoc_content.append("[NOTE]\n====\nTrend storage is not enabled. Enable it in config.yaml to view trends.\n====\n")
        structured_data["trend_viewer"] = {"status": "disabled", "note": "Trend storage not enabled"}
        return "\n".join(adoc_content), structured_data
    
    # Check if trend database settings are provided
    trend_db_settings = settings.get('trend_database', {})
    if not trend_db_settings:
        adoc_content.append("[ERROR]\n====\nTrend database settings not found in configuration.\n====\n")
        structured_data["trend_viewer"] = {"status": "error", "error": "Trend database settings missing"}
        return "\n".join(adoc_content), structured_data
    
    try:
        # Initialize trend storage
        trend_storage = TrendAnalysisStorage(
            trend_db_settings=trend_db_settings,
            company_name=settings['company_name'],
            database_name=settings['database'],
            host_name=settings['host']
        )
        
        # Connect to trend database
        trend_storage.connect_trend_db()
        
        # Get trend analysis for different time periods
        periods = [7, 30, 90]  # Days to analyze
        trend_reports = {}
        
        for days in periods:
            trend_analysis = trend_storage.get_trend_analysis(days_back=days)
            trend_reports[f"{days}_days"] = trend_analysis
        
        # Generate comprehensive trend report
        adoc_content.append("=== Trend Analysis Summary\n")
        
        for period_name, analysis in trend_reports.items():
            days = int(period_name.split('_')[0])
            adoc_content.append(f"**{days}-Day Analysis:**\n")
            adoc_content.append(f"- Runs analyzed: {analysis['runs_analyzed']}\n")
            adoc_content.append(f"- Time period: {analysis['time_period_days']} days\n")
            
            run_trends = analysis['run_trends']
            adoc_content.append(f"- Total runs: {run_trends['total_runs']}\n")
            adoc_content.append(f"- Successful runs: {run_trends['successful_runs']}\n")
            adoc_content.append(f"- Failed runs: {run_trends['failed_runs']}\n")
            if run_trends['avg_duration'] > 0:
                adoc_content.append(f"- Average run duration: {run_trends['avg_duration']:.2f} seconds\n")
            adoc_content.append("\n")
        
        # Detailed metric trends
        if trend_reports['30_days']['metric_trends']:
            adoc_content.append("=== Key Metric Trends (30 Days)\n")
            
            for metric, data in trend_reports['30_days']['metric_trends'].items():
                trend_icon = {
                    'increasing': '📈',
                    'decreasing': '📉',
                    'stable': '➡️',
                    'insufficient_data': '❓'
                }.get(data['trend'], '❓')
                
                adoc_content.append(f"**{metric.replace('_', ' ').title()}** {trend_icon}\n")
                adoc_content.append(f"- Trend: {data['trend']}\n")
                adoc_content.append(f"- Average: {data['avg']:.2f} {data['unit']}\n")
                adoc_content.append(f"- Range: {data['min']:.2f} - {data['max']:.2f} {data['unit']}\n")
                adoc_content.append(f"- Data points: {len(data['values'])}\n\n")
        
        # Generate recommendations based on trends
        adoc_content.append("=== Trend-Based Recommendations\n")
        recommendations = generate_trend_recommendations(trend_reports)
        
        for category, recs in recommendations.items():
            if recs:
                adoc_content.append(f"**{category}:**\n")
                for rec in recs:
                    adoc_content.append(f"- {rec}\n")
                adoc_content.append("\n")
        
        # Performance alerts
        alerts = generate_trend_alerts(trend_reports, settings)
        if alerts:
            adoc_content.append("=== Trend Alerts\n")
            for alert in alerts:
                adoc_content.append(f"⚠️ **{alert['severity']}:** {alert['message']}\n")
            adoc_content.append("\n")
        
        trend_storage.close()
        
        structured_data["trend_viewer"] = {
            "status": "success",
            "trend_reports": trend_reports,
            "recommendations": recommendations,
            "alerts": alerts
        }
        
    except Exception as e:
        adoc_content.append(f"[ERROR]\n====\nFailed to generate trend analysis: {e}\n====\n")
        structured_data["trend_viewer"] = {"status": "error", "error": str(e)}
    
    return "\n".join(adoc_content), structured_data

def generate_trend_recommendations(trend_reports):
    """Generate recommendations based on trend analysis."""
    recommendations = {
        'Performance': [],
        'Reliability': [],
        'Monitoring': [],
        'Maintenance': []
    }
    
    # Analyze 30-day trends for recommendations
    analysis = trend_reports.get('30_days', {})
    metric_trends = analysis.get('metric_trends', {})
    run_trends = analysis.get('run_trends', {})
    
    # Connection-related recommendations
    if 'active_connections' in metric_trends:
        conn_data = metric_trends['active_connections']
        if conn_data['trend'] == 'increasing':
            recommendations['Performance'].append(
                "Connection usage is trending upward. Consider implementing connection pooling or scaling up."
            )
        elif conn_data['avg'] > 100:  # High connection count
            recommendations['Performance'].append(
                "High connection count detected. Review connection management and consider connection pooling."
            )
    
    # Query performance recommendations
    if 'avg_query_execution_time' in metric_trends:
        query_data = metric_trends['avg_query_execution_time']
        if query_data['trend'] == 'increasing':
            recommendations['Performance'].append(
                "Query execution time is increasing. Review slow queries and consider index optimization."
            )
    
    # Index-related recommendations
    if 'unused_indexes' in metric_trends:
        idx_data = metric_trends['unused_indexes']
        if idx_data['avg'] > 10:
            recommendations['Maintenance'].append(
                f"High number of unused indexes ({idx_data['avg']:.0f} average). Consider index cleanup."
            )
    
    # Reliability recommendations
    if run_trends['failed_runs'] > 0:
        failure_rate = run_trends['failed_runs'] / run_trends['total_runs']
        if failure_rate > 0.1:  # More than 10% failure rate
            recommendations['Reliability'].append(
                f"High health check failure rate ({failure_rate:.1%}). Review configuration and connectivity."
            )
    
    # Monitoring recommendations
    if run_trends['total_runs'] < 5:
        recommendations['Monitoring'].append(
            "Limited historical data available. Run health checks more frequently for better trend analysis."
        )
    
    return recommendations

def generate_trend_alerts(trend_reports, settings):
    """Generate alerts based on trend analysis and thresholds."""
    alerts = []
    
    # Get alert thresholds from settings
    alert_thresholds = settings.get('trend_analysis', {}).get('alert_thresholds', {})
    
    analysis = trend_reports.get('30_days', {})
    metric_trends = analysis.get('metric_trends', {})
    run_trends = analysis.get('run_trends', {})
    
    # Connection utilization alert
    if 'active_connections' in metric_trends and 'max_connections' in metric_trends:
        conn_utilization = metric_trends['active_connections']['avg'] / metric_trends['max_connections']['avg']
        threshold = alert_thresholds.get('connection_utilization', 0.8)
        
        if conn_utilization > threshold:
            alerts.append({
                'severity': 'HIGH',
                'message': f'Connection utilization at {conn_utilization:.1%}, exceeding {threshold:.1%} threshold.'
            })
    
    # Query performance degradation alert
    if 'avg_query_execution_time' in metric_trends:
        query_data = metric_trends['avg_query_execution_time']
        if query_data['trend'] == 'increasing':
            threshold = alert_thresholds.get('query_performance_degradation', 0.2)
            # Calculate if increase exceeds threshold
            if len(query_data['values']) >= 2:
                recent_avg = sum(query_data['values'][-len(query_data['values'])//2:]) / (len(query_data['values'])//2)
                older_avg = sum(query_data['values'][:len(query_data['values'])//2]) / (len(query_data['values'])//2)
                if older_avg > 0 and (recent_avg - older_avg) / older_avg > threshold:
                    alerts.append({
                        'severity': 'MEDIUM',
                        'message': f'Query performance degraded by {((recent_avg - older_avg) / older_avg):.1%} over the last 30 days.'
                    })
    
    # Failed modules increase alert
    if run_trends['total_runs'] > 0:
        failure_rate = run_trends['failed_runs'] / run_trends['total_runs']
        threshold = alert_thresholds.get('failed_modules_increase', 0.1)
        
        if failure_rate > threshold:
            alerts.append({
                'severity': 'HIGH',
                'message': f'Health check failure rate at {failure_rate:.1%}, exceeding {threshold:.1%} threshold.'
            })
    
    return alerts

def run_trend_comparison(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Compare current run with historical data to identify changes.
    """
    adoc_content = ["=== Historical Comparison", "Comparing current run with historical data."]
    structured_data = {}
    
    # This would implement comparison logic
    # For now, return a placeholder
    adoc_content.append("[NOTE]\n====\nHistorical comparison feature coming soon.\n====\n")
    structured_data["trend_comparison"] = {"status": "placeholder"}
    
    return "\n".join(adoc_content), structured_data 