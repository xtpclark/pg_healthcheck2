"""
System Metrics Utility

Provides easy access to system configuration stored in the metric table.
Uses database stored procedures for type-safe metric retrieval.

Usage:
    from trends_app.metrics import get_metric_int, get_metric_bool, get_metric_text

    chars_per_token = get_metric_int('token_estimation_chars_per_token', default=4)
    feature_enabled = get_metric_bool('bulk_analysis_enabled', default=True)
    api_key = get_metric_text('aws_kms_key_arn')
"""

import psycopg2
from flask import current_app
from .utils import load_trends_config


def get_metric_int(metric_name, default=None):
    """
    Fetch an integer metric from the database.

    Args:
        metric_name (str): Name of the metric
        default (int): Default value if metric not found

    Returns:
        int: Metric value or default
    """
    config = load_trends_config()
    db_config = config.get('database')

    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("SELECT fetchmetricvalue(%s);", (metric_name,))
        result = cursor.fetchone()[0]
        conn.close()

        return int(result) if result is not None else default
    except Exception as e:
        current_app.logger.warning(f"Error fetching metric '{metric_name}': {e}")
        return default


def get_metric_bool(metric_name, default=False):
    """
    Fetch a boolean metric from the database.

    Args:
        metric_name (str): Name of the metric
        default (bool): Default value if metric not found

    Returns:
        bool: Metric value or default
    """
    config = load_trends_config()
    db_config = config.get('database')

    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("SELECT fetchmetricbool(%s);", (metric_name,))
        result = cursor.fetchone()[0]
        conn.close()

        return result if result is not None else default
    except Exception as e:
        current_app.logger.warning(f"Error fetching metric '{metric_name}': {e}")
        return default


def get_metric_text(metric_name, default=None):
    """
    Fetch a text metric from the database.

    Args:
        metric_name (str): Name of the metric
        default (str): Default value if metric not found

    Returns:
        str: Metric value or default
    """
    config = load_trends_config()
    db_config = config.get('database')

    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("SELECT fetchmetrictext(%s);", (metric_name,))
        result = cursor.fetchone()[0]
        conn.close()

        return result if result is not None else default
    except Exception as e:
        current_app.logger.warning(f"Error fetching metric '{metric_name}': {e}")
        return default


def set_metric(metric_name, metric_value):
    """
    Set a metric value in the database.

    Args:
        metric_name (str): Name of the metric
        metric_value (str): Value to set (will be converted to string)

    Returns:
        bool: True if successful, False otherwise
    """
    config = load_trends_config()
    db_config = config.get('database')

    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("SELECT setmetric(%s, %s);", (metric_name, str(metric_value)))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        current_app.logger.error(f"Error setting metric '{metric_name}': {e}")
        return False


def get_all_metrics(module=None):
    """
    Get all metrics, optionally filtered by module.

    Args:
        module (str): Optional module filter

    Returns:
        list[dict]: List of metric dictionaries with keys: name, value, module
    """
    config = load_trends_config()
    db_config = config.get('database')

    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        if module:
            cursor.execute("""
                SELECT metric_name, metric_value, metric_module
                FROM metric
                WHERE metric_module = %s
                ORDER BY metric_name;
            """, (module,))
        else:
            cursor.execute("""
                SELECT metric_name, metric_value, metric_module
                FROM metric
                ORDER BY metric_module NULLS FIRST, metric_name;
            """)

        metrics = [
            {'name': row[0], 'value': row[1], 'module': row[2]}
            for row in cursor.fetchall()
        ]

        conn.close()
        return metrics
    except Exception as e:
        current_app.logger.error(f"Error fetching all metrics: {e}")
        return []


# Convenience constants for commonly used metrics
class MetricKeys:
    """Common metric key names for type safety and IDE autocomplete."""

    # AI Analysis
    TOKEN_ESTIMATION_CHARS_PER_TOKEN = 'token_estimation_chars_per_token'
    BULK_ANALYSIS_TOKEN_WARNING = 'bulk_analysis_token_warning_threshold'
    BULK_ANALYSIS_BATCH_SIZE = 'bulk_analysis_recommended_batch_size'
    DEFAULT_MAX_OUTPUT_TOKENS = 'default_max_output_tokens'
    AI_API_TIMEOUT = 'ai_api_timeout_seconds'

    # Features
    BULK_ANALYSIS_ENABLED = 'bulk_analysis_enabled'
    TOKEN_ESTIMATION_ENABLED = 'token_estimation_enabled'

    # Security
    SESSION_TIMEOUT = 'session_timeout_minutes'
    MAINTENANCE_MODE = 'maintenance_mode'

    # Encryption
    AWS_KMS_KEY_ARN = 'aws_kms_key_arn'
    FILESYSTEM_KEY_PATH = 'filesystem_key_path'
