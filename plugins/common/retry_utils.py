"""
Retry utilities for handling transient failures in API calls.

Provides decorators and functions for implementing retry logic
with exponential backoff for cloud provider APIs and other services.
"""

import time
import logging
from functools import wraps
from typing import Callable, Tuple, Type, Union

logger = logging.getLogger(__name__)


def retry_on_failure(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: Union[Type[Exception], Tuple[Type[Exception], ...]] = Exception,
    log_attempts: bool = True
):
    """
    Decorator to retry function calls on failure with exponential backoff.
    
    Args:
        max_attempts: Maximum number of attempts (default: 3)
        delay: Initial delay between retries in seconds (default: 1.0)
        backoff: Multiplier for delay after each retry (default: 2.0)
        exceptions: Exception type(s) to catch and retry (default: Exception)
        log_attempts: Whether to log retry attempts (default: True)
    
    Returns:
        Decorated function that will retry on failure
    
    Example:
        @retry_on_failure(max_attempts=3, delay=1, exceptions=(requests.RequestException,))
        def fetch_data():
            response = requests.get('https://api.example.com/data')
            response.raise_for_status()
            return response.json()
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 1
            current_delay = delay
            
            while attempt <= max_attempts:
                try:
                    return func(*args, **kwargs)
                
                except exceptions as e:
                    if attempt == max_attempts:
                        if log_attempts:
                            logger.error(
                                f"{func.__name__} failed after {max_attempts} attempts: {e}"
                            )
                        raise
                    
                    if log_attempts:
                        logger.warning(
                            f"{func.__name__} attempt {attempt}/{max_attempts} failed: {e}. "
                            f"Retrying in {current_delay:.1f}s..."
                        )
                    
                    time.sleep(current_delay)
                    current_delay *= backoff
                    attempt += 1
        
        return wrapper
    return decorator


def should_retry_error(error: Exception, retryable_codes: list = None) -> bool:
    """
    Determine if an error should trigger a retry.
    
    Args:
        error: Exception that was raised
        retryable_codes: List of error codes that should trigger retry
    
    Returns:
        bool: True if error should trigger retry
    """
    if retryable_codes is None:
        # Default retryable patterns
        retryable_codes = [
            'Throttling',
            'RequestLimitExceeded',
            'ServiceUnavailable',
            'InternalError',
            'TooManyRequests',
            '429',
            '500',
            '502',
            '503',
            '504'
        ]
    
    error_str = str(error)
    error_type = type(error).__name__
    
    # Check if error string contains any retryable code
    for code in retryable_codes:
        if code.lower() in error_str.lower() or code.lower() in error_type.lower():
            return True
    
    return False
