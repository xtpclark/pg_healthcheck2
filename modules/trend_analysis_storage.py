#!/usr/bin/env python3
"""
Trend Analysis Storage Module

This module handles storing structured health check findings in a PostgreSQL database
for trend analysis across multiple runs. It supports both storing new data and
retrieving historical data for comparison.
"""

import json
import psycopg2
from datetime import datetime, timedelta
from decimal import Decimal
import logging

class TrendAnalysisStorage:
    def __init__(self, trend_db_settings, company_name, database_name, host_name):
        """
        Initialize trend analysis storage.
        
        Args:
            trend_db_settings (dict): Database connection settings for trend storage
            company_name (str): Company/system identifier
            database_name (str): Target database being analyzed
            host_name (str): Host/server being analyzed
        """
        self.trend_db_settings = trend_db_settings
        self.company_name = company_name
        self.database_name = database_name
        self.host_name = host_name
        self.conn = None
        self.cursor = None
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
        
    def connect_trend_db(self):
        """Connect to the trend analysis database."""
        try:
            self.conn = psycopg2.connect(
                host=self.trend_db_settings['host'],
                port=self.trend_db_settings['port'],
                dbname=self.trend_db_settings['database'],
                user=self.trend_db_settings['user'],
                password=self.trend_db_settings['password']
            )
            self.cursor = self.conn.cursor()
            self.logger.info(f"Connected to trend analysis database: {self.trend_db_settings['database']}")
        except psycopg2.Error as e:
            self.logger.error(f"Failed to connect to trend database: {e}")
            raise
    
    def create_trend_schema(self, schema_name=None):
        """
        Create the schema and tables for trend analysis.
        
        Args:
            schema_name (str): Schema name. If None, uses company_name sanitized.
        """
        if schema_name is None:
            # Sanitize company name for schema
            schema_name = self._sanitize_name(self.company_name)
        
        try:
            # Create schema
            self.cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            
            # Create health_check_runs table
            self.cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {schema_name}.health_check_runs (
                    run_id SERIAL PRIMARY KEY,
                    company_name VARCHAR(100) NOT NULL,
                    database_name VARCHAR(100) NOT NULL,
                    host_name VARCHAR(255) NOT NULL,
                    run_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    pg_version VARCHAR(20),
                    total_modules INTEGER DEFAULT 0,
                    successful_modules INTEGER DEFAULT 0,
                    failed_modules INTEGER DEFAULT 0,
                    ai_analysis_status VARCHAR(50),
                    ai_model_used VARCHAR(100),
                    run_duration_seconds DECIMAL(10,2),
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
            """)
            
            # Create module_findings table
            self.cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {schema_name}.module_findings (
                    finding_id SERIAL PRIMARY KEY,
                    run_id INTEGER REFERENCES {schema_name}.health_check_runs(run_id) ON DELETE CASCADE,
                    module_name VARCHAR(100) NOT NULL,
                    status VARCHAR(50) NOT NULL,
                    severity_level VARCHAR(20),
                    severity_score INTEGER,
                    data_json JSONB,
                    error_message TEXT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
            """)
            
            # Create trend_metrics table for key metrics
            self.cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {schema_name}.trend_metrics (
                    metric_id SERIAL PRIMARY KEY,
                    run_id INTEGER REFERENCES {schema_name}.health_check_runs(run_id) ON DELETE CASCADE,
                    metric_name VARCHAR(100) NOT NULL,
                    metric_value DECIMAL(15,2),
                    metric_unit VARCHAR(20),
                    metric_category VARCHAR(50),
                    metric_description TEXT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
            """)
            
            # Create indexes for better query performance
            self.cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_health_check_runs_company_timestamp 
                ON {schema_name}.health_check_runs(company_name, run_timestamp)
            """)
            
            self.cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_module_findings_run_module 
                ON {schema_name}.module_findings(run_id, module_name)
            """)
            
            self.cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_trend_metrics_run_metric 
                ON {schema_name}.trend_metrics(run_id, metric_name)
            """)
            
            self.conn.commit()
            self.logger.info(f"Created/verified schema and tables in {schema_name}")
            
        except psycopg2.Error as e:
            self.logger.error(f"Failed to create trend schema: {e}")
            self.conn.rollback()
            raise
    
    def _sanitize_name(self, name):
        """Sanitize name for use as schema/table name."""
        import re
        # Convert to lowercase, replace spaces with underscores, remove special chars
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name.lower())
        # Remove multiple underscores and trim
        sanitized = re.sub(r'_+', '_', sanitized).strip('_')
        return sanitized[:63]  # PostgreSQL identifier limit
    
    def _convert_for_json(self, obj):
        """Convert objects to be JSON serializable, handling Decimal objects."""
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, dict):
            return {k: self._convert_for_json(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_for_json(item) for item in obj]
        elif isinstance(obj, datetime):
            return obj.isoformat()
        else:
            return obj
    
    def _safe_float(self, value):
        """Safely convert a value to float, handling Decimal objects."""
        if isinstance(value, Decimal):
            return float(value)
        elif isinstance(value, (int, float)):
            return float(value)
        else:
            try:
                return float(value)
            except (ValueError, TypeError):
                return 0.0
    
    def store_health_check_run(self, structured_findings, run_metadata=None):
        """
        Store a complete health check run in the trend database.
        
        Args:
            structured_findings (dict): Complete structured findings from health check
            run_metadata (dict): Additional metadata about the run
        """
        try:
            # Calculate run statistics
            total_modules = len(structured_findings)
            successful_modules = sum(1 for module in structured_findings.values() 
                                  if module.get('status') == 'success')
            failed_modules = sum(1 for module in structured_findings.values() 
                              if module.get('status') == 'error')
            
            # Extract AI analysis info
            ai_status = 'not_performed'
            ai_model = None
            if 'run_recommendation_enhanced' in structured_findings:
                ai_data = structured_findings['run_recommendation_enhanced'].get('data', {})
                ai_analysis = ai_data.get('ai_analysis', {})
                ai_status = ai_analysis.get('status', 'not_performed')
                ai_model = ai_analysis.get('model')
            elif 'run_recommendation' in structured_findings:
                ai_data = structured_findings['run_recommendation'].get('data', {})
                ai_analysis = ai_data.get('ai_analysis', {})
                ai_status = ai_analysis.get('status', 'not_performed')
                ai_model = ai_analysis.get('model')
            
            # Get PostgreSQL version
            pg_version = None
            if 'postgres_overview' in structured_findings:
                pg_data = structured_findings['postgres_overview'].get('data', {})
                if isinstance(pg_data, list) and len(pg_data) > 0:
                    for row in pg_data:
                        if isinstance(row, dict) and 'version' in row:
                            pg_version = row['version']
                            break
            
            # Insert run record
            schema_name = self._sanitize_name(self.company_name)
            self.cursor.execute(f"""
                INSERT INTO {schema_name}.health_check_runs 
                (company_name, database_name, host_name, pg_version, total_modules, 
                 successful_modules, failed_modules, ai_analysis_status, ai_model_used, run_duration_seconds)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING run_id
            """, (self.company_name, self.database_name, self.host_name, pg_version,
                  total_modules, successful_modules, failed_modules, ai_status, ai_model,
                  run_metadata.get('duration_seconds') if run_metadata else None))
            
            run_id = self.cursor.fetchone()[0]
            
            # Store module findings
            for module_name, module_data in structured_findings.items():
                status = module_data.get('status', 'unknown')
                data_json = module_data.get('data', {})
                error_message = module_data.get('error', module_data.get('details', ''))
                
                # Extract severity info if available
                severity_level = None
                severity_score = None
                if isinstance(data_json, dict):
                    severity_level = data_json.get('severity_level')
                    severity_score = data_json.get('severity_score')
                
                # Convert data_json to be JSON serializable
                serializable_data = self._convert_for_json(data_json)
                
                self.cursor.execute(f"""
                    INSERT INTO {schema_name}.module_findings 
                    (run_id, module_name, status, severity_level, severity_score, data_json, error_message)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (run_id, module_name, status, severity_level, severity_score, 
                      json.dumps(serializable_data), error_message))
            
            # Extract and store key metrics
            self._extract_and_store_metrics(run_id, structured_findings, schema_name)
            
            self.conn.commit()
            self.logger.info(f"Stored health check run {run_id} with {total_modules} modules")
            return run_id
            
        except psycopg2.Error as e:
            self.logger.error(f"Failed to store health check run: {e}")
            self.conn.rollback()
            raise
    
    def _extract_and_store_metrics(self, run_id, structured_findings, schema_name):
        """Extract key metrics from structured findings and store them."""
        metrics = []
        
        # Extract connection metrics
        if 'connection_metrics' in structured_findings:
            conn_data = structured_findings['connection_metrics'].get('data', {})
            if isinstance(conn_data, list):
                for row in conn_data:
                    if isinstance(row, dict):
                        if 'active_connections' in row:
                            metrics.append(('active_connections', self._safe_float(row['active_connections']), 'count', 'connections'))
                        if 'max_connections' in row:
                            metrics.append(('max_connections', self._safe_float(row['max_connections']), 'count', 'connections'))
        
        # Extract table size metrics
        if 'large_tbl' in structured_findings:
            table_data = structured_findings['large_tbl'].get('data', {})
            if isinstance(table_data, list):
                for row in table_data:
                    if isinstance(row, dict) and 'pg_size_pretty' in str(row):
                        # Extract size value (simplified)
                        size_str = str(row.get('pg_size_pretty', ''))
                        if 'MB' in size_str or 'GB' in size_str:
                            metrics.append(('large_table_count', 1, 'count', 'tables'))
                            break
        
        # Extract index metrics
        if 'unused_idx' in structured_findings:
            idx_data = structured_findings['unused_idx'].get('data', {})
            if isinstance(idx_data, list):
                metrics.append(('unused_indexes', len(idx_data), 'count', 'indexes'))
        
        # Extract query performance metrics
        if 'top_queries_by_execution_time' in structured_findings:
            query_data = structured_findings['top_queries_by_execution_time'].get('data', {})
            if isinstance(query_data, list):
                for row in query_data:
                    if isinstance(row, dict) and 'mean_exec_time' in row:
                        try:
                            exec_time = self._safe_float(row['mean_exec_time'])
                            if exec_time > 0:
                                metrics.append(('avg_query_execution_time', exec_time, 'milliseconds', 'performance'))
                                break
                        except (ValueError, TypeError):
                            pass
        
        # Store metrics
        for metric_name, metric_value, metric_unit, metric_category in metrics:
            self.cursor.execute(f"""
                INSERT INTO {schema_name}.trend_metrics 
                (run_id, metric_name, metric_value, metric_unit, metric_category)
                VALUES (%s, %s, %s, %s, %s)
            """, (run_id, metric_name, metric_value, metric_unit, metric_category))
    
    def get_trend_analysis(self, days_back=30, metrics=None):
        """
        Get trend analysis for the specified time period.
        
        Args:
            days_back (int): Number of days to analyze
            metrics (list): List of metric names to analyze. If None, analyzes all.
            
        Returns:
            dict: Trend analysis results
        """
        try:
            schema_name = self._sanitize_name(self.company_name)
            cutoff_date = datetime.now() - timedelta(days=days_back)
            
            # Get recent runs
            self.cursor.execute(f"""
                SELECT run_id, run_timestamp, total_modules, successful_modules, failed_modules,
                       ai_analysis_status, run_duration_seconds
                FROM {schema_name}.health_check_runs
                WHERE company_name = %s AND database_name = %s AND run_timestamp >= %s
                ORDER BY run_timestamp DESC
            """, (self.company_name, self.database_name, cutoff_date))
            
            runs = self.cursor.fetchall()
            
            # Get metric trends
            if metrics is None:
                self.cursor.execute(f"""
                    SELECT DISTINCT metric_name FROM {schema_name}.trend_metrics
                    WHERE run_id IN (
                        SELECT run_id FROM {schema_name}.health_check_runs
                        WHERE company_name = %s AND database_name = %s AND run_timestamp >= %s
                    )
                """, (self.company_name, self.database_name, cutoff_date))
                metrics = [row[0] for row in self.cursor.fetchall()]
            
            metric_trends = {}
            for metric in metrics:
                self.cursor.execute(f"""
                    SELECT tm.metric_value, tm.metric_unit, hcr.run_timestamp
                    FROM {schema_name}.trend_metrics tm
                    JOIN {schema_name}.health_check_runs hcr ON tm.run_id = hcr.run_id
                    WHERE tm.metric_name = %s AND hcr.company_name = %s AND hcr.database_name = %s
                    AND hcr.run_timestamp >= %s
                    ORDER BY hcr.run_timestamp
                """, (metric, self.company_name, self.database_name, cutoff_date))
                
                metric_data = self.cursor.fetchall()
                if metric_data:
                    values = [float(row[0]) for row in metric_data if row[0] is not None]
                    if values:
                        metric_trends[metric] = {
                            'values': values,
                            'unit': metric_data[0][1],
                            'trend': self._calculate_trend(values),
                            'min': min(values),
                            'max': max(values),
                            'avg': sum(values) / len(values)
                        }
            
            return {
                'runs_analyzed': len(runs),
                'time_period_days': days_back,
                'run_trends': {
                    'total_runs': len(runs),
                    'successful_runs': sum(1 for run in runs if run[4] == run[2]),  # successful == total
                    'failed_runs': sum(1 for run in runs if run[4] < run[2]),  # successful < total
                    'avg_duration': sum(run[6] or 0 for run in runs) / len(runs) if runs else 0
                },
                'metric_trends': metric_trends
            }
            
        except psycopg2.Error as e:
            self.logger.error(f"Failed to get trend analysis: {e}")
            raise
    
    def _calculate_trend(self, values):
        """Calculate trend direction from a list of values."""
        if len(values) < 2:
            return 'insufficient_data'
        
        # Simple linear trend calculation
        first_avg = sum(values[:len(values)//2]) / (len(values)//2)
        second_avg = sum(values[len(values)//2:]) / (len(values) - len(values)//2)
        
        if second_avg > first_avg * 1.1:
            return 'increasing'
        elif second_avg < first_avg * 0.9:
            return 'decreasing'
        else:
            return 'stable'
    
    def close(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

def run_trend_storage(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Stores health check data for trend analysis.
    """
    adoc_content = ["Storing health check data for trend analysis."]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    # Check if trend storage is enabled
    if not settings.get('trend_storage_enabled', False):
        adoc_content.append("[NOTE]\n====\nTrend storage is not enabled in configuration.\n====\n")
        structured_data["trend_storage"] = {"status": "disabled", "note": "Trend storage not enabled"}
        return "\n".join(adoc_content), structured_data
    
    # Check if trend database settings are provided
    trend_db_settings = settings.get('trend_database', {})
    if not trend_db_settings:
        adoc_content.append("[ERROR]\n====\nTrend database settings not found in configuration.\n====\n")
        structured_data["trend_storage"] = {"status": "error", "error": "Trend database settings missing"}
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
        
        # Create schema and tables
        trend_storage.create_trend_schema()
        
        # Store the health check run
        run_id = trend_storage.store_health_check_run(all_structured_findings)
        
        # Get trend analysis for last 30 days
        trend_analysis = trend_storage.get_trend_analysis(days_back=30)
        
        adoc_content.append(f"[SUCCESS]\n====\nSuccessfully stored health check run {run_id} in trend database.\n====\n")
        
        # Add trend summary
        adoc_content.append("=== Trend Analysis Summary (Last 30 Days)\n")
        adoc_content.append(f"**Runs Analyzed:** {trend_analysis['runs_analyzed']}\n")
        adoc_content.append(f"**Time Period:** {trend_analysis['time_period_days']} days\n")
        
        if trend_analysis['metric_trends']:
            adoc_content.append("**Key Metric Trends:**\n")
            for metric, data in trend_analysis['metric_trends'].items():
                adoc_content.append(f"- **{metric}:** {data['trend']} (avg: {data['avg']:.2f} {data['unit']})\n")
        
        trend_storage.close()
        
        structured_data["trend_storage"] = {
            "status": "success",
            "run_id": run_id,
            "trend_analysis": trend_analysis
        }
        
    except Exception as e:
        adoc_content.append(f"[ERROR]\n====\nFailed to store trend data: {e}\n====\n")
        structured_data["trend_storage"] = {"status": "error", "error": str(e)}
    
    return "\n".join(adoc_content), structured_data 