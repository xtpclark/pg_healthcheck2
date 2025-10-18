#!/usr/bin/env python3
"""
Centralized JSON serialization utilities.

This module provides technology-agnostic JSON encoding and conversion utilities
that work with objects from any database technology (Cassandra, PostgreSQL, 
MongoDB, ClickHouse, etc.) without requiring technology-specific code.
"""

import json
from decimal import Decimal
from datetime import datetime, timedelta


class UniversalJSONEncoder(json.JSONEncoder):
    """
    A universal JSON encoder that handles objects from any database technology.
    
    This encoder uses duck typing to convert database-specific objects
    (like Cassandra's OrderedMap, MongoDB's SON, etc.) into JSON-compatible
    formats without requiring technology-specific code.
    
    Usage:
        json.dumps(data, cls=UniversalJSONEncoder)
    """
    
    def default(self, obj):
        """
        Convert non-JSON-serializable objects to JSON-compatible formats.
        
        This method handles common Python types and database-specific types
        in a technology-agnostic way using duck typing and introspection.
        
        Args:
            obj: The object to serialize
            
        Returns:
            JSON-serializable representation of the object
            
        Raises:
            TypeError: If the object cannot be serialized (rare due to fallbacks)
        """
        # Handle None explicitly
        if obj is None:
            return None
        
        # Handle standard Python numeric/time types
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, timedelta):
            return obj.total_seconds()
        
        # Handle bytes
        if isinstance(obj, bytes):
            try:
                return obj.decode('utf-8')
            except:
                return str(obj)
        
        # Handle any mapping-like objects (dict-like but not dict)
        # This catches Cassandra OrderedMap, MongoDB SON, etc.
        if hasattr(obj, 'items') and callable(getattr(obj, 'items')):
            try:
                return {k: self.default(v) if not isinstance(v, (str, int, float, bool, type(None))) 
                        else v for k, v in obj.items()}
            except:
                pass
        
        # Handle any iterable objects (list-like but not list/str/bytes/dict)
        if hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, dict)):
            try:
                return [self.default(item) if not isinstance(item, (str, int, float, bool, type(None))) 
                        else item for item in obj]
            except:
                pass
        
        # Handle sets explicitly
        if isinstance(obj, set):
            return [self.default(item) if not isinstance(item, (str, int, float, bool, type(None))) 
                    else item for item in obj]
        
        # Last resort: convert to string
        # This catches any custom database objects from any technology
        try:
            return str(obj)
        except:
            return repr(obj)


def convert_to_json_serializable(obj):
    """
    Recursively convert non-JSON-serializable objects to JSON-compatible formats.
    
    This function is useful when you need to convert data structures before
    passing them to json.dumps(), especially when dealing with deeply nested
    structures from database queries.
    
    Unlike using UniversalJSONEncoder directly with json.dumps(), this function
    pre-processes the entire data structure, which can be more efficient for
    large nested structures that will be serialized multiple times.
    
    Args:
        obj: Any Python object (can be nested dict/list/tuple/set)
        
    Returns:
        A version of the object with all non-serializable types converted
        
    Example:
        raw_data = connector.execute_query("SELECT * FROM table")
        clean_data = convert_to_json_serializable(raw_data)
        json_str = json.dumps(clean_data)
    """
    # Handle None explicitly
    if obj is None:
        return None
    
    # Handle standard Python types that need conversion
    if isinstance(obj, Decimal): 
        return float(obj)
    if isinstance(obj, datetime): 
        return obj.isoformat()
    if isinstance(obj, timedelta): 
        return obj.total_seconds()
    
    # Handle bytes
    if isinstance(obj, bytes):
        try:
            return obj.decode('utf-8')
        except:
            return str(obj)
    
    # Recursively handle standard collections
    if isinstance(obj, dict): 
        return {k: convert_to_json_serializable(v) for k, v in obj.items()}
    if isinstance(obj, list): 
        return [convert_to_json_serializable(item) for item in obj]
    if isinstance(obj, tuple):
        return [convert_to_json_serializable(item) for item in obj]
    if isinstance(obj, set):
        return [convert_to_json_serializable(item) for item in obj]
    
    # Handle any mapping-like objects (dict-like but not dict)
    if hasattr(obj, 'items') and callable(getattr(obj, 'items')):
        try:
            return {k: convert_to_json_serializable(v) for k, v in obj.items()}
        except:
            pass
    
    # Handle any iterable objects (list-like but not list)
    if hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes)):
        try:
            return [convert_to_json_serializable(item) for item in obj]
        except:
            pass
    
    # Primitive types that are already JSON-serializable
    if isinstance(obj, (str, int, float, bool)):
        return obj
    
    # Last resort: convert to string
    try:
        return str(obj)
    except:
        return repr(obj)


def safe_json_dumps(obj, **kwargs):
    """
    Safely serialize any object to JSON string.
    
    This is a convenience wrapper around json.dumps() that automatically
    uses UniversalJSONEncoder and provides sensible defaults.
    
    Args:
        obj: Any Python object to serialize
        **kwargs: Additional arguments passed to json.dumps()
                  (e.g., indent=2, sort_keys=True)
    
    Returns:
        str: JSON string representation of the object
        
    Example:
        json_str = safe_json_dumps(findings, indent=2)
    """
    # Set default kwargs if not provided
    if 'cls' not in kwargs:
        kwargs['cls'] = UniversalJSONEncoder
    
    return json.dumps(obj, **kwargs)


def safe_json_loads(json_str):
    """
    Safely deserialize JSON string to Python object.
    
    This is a simple wrapper for consistency with safe_json_dumps(),
    and can be extended with custom error handling if needed.
    
    Args:
        json_str: JSON string to deserialize
        
    Returns:
        Python object (dict, list, etc.)
        
    Raises:
        json.JSONDecodeError: If the string is not valid JSON
    """
    return json.loads(json_str)
