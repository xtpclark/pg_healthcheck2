# PostgreSQL Version Compatibility

## Overview

This document outlines the PostgreSQL version compatibility improvements made to the health check system to ensure compatibility with PostgreSQL 13+ and future PostgreSQL 18.

## Minimum Supported Version

- **PostgreSQL 13+**: The system now enforces a minimum version requirement of PostgreSQL 13
- **Future Compatibility**: Designed to work with PostgreSQL 18 and beyond

## Version Compatibility Module

### `modules/postgresql_version_compatibility.py`

A centralized module that provides:

1. **Version Detection**: `get_postgresql_version()` - Detects PostgreSQL version and provides compatibility flags
2. **Query Generation**: Version-specific query functions for different features
3. **Validation**: `validate_postgresql_version()` - Ensures minimum version requirements
4. **Column Mapping**: `get_version_specific_columns()` - Maps columns based on PostgreSQL version

### Key Functions

```python
# Get version information
compatibility = get_postgresql_version(cursor, execute_query)

# Validate minimum version
is_supported, error_msg = validate_postgresql_version(compatibility)

# Get version-specific queries
pg_stat_query = get_pg_stat_statements_query(compatibility, 'standard')
vacuum_query = get_vacuum_progress_query(compatibility)
```

## Updated Modules

The following modules have been updated to use the centralized version compatibility system:

### Core Query Analysis Modules

1. **`vacstat2.py`** - Vacuum progress and statistics
   - Uses `get_vacuum_progress_query()` for PostgreSQL 17+ dead tuple bytes support
   - Validates minimum PostgreSQL 13 requirement

2. **`function_audit.py`** - Function performance analysis
   - Uses `get_pg_stat_statements_query()` for version-specific function linking
   - Handles PostgreSQL 14+ removal of `funcid` column

3. **`top_write_queries.py`** - Write-intensive query analysis
   - Uses version-specific `pg_stat_statements` columns
   - Supports PostgreSQL 14+ `wal_bytes` and `shared_blks_written`

4. **`n_tuples_in.py`** - High tuple write analysis
   - Uses version-specific `pg_stat_statements` queries
   - Handles PostgreSQL 14+ column changes

5. **`top_queries_by_execution_time.py`** - Execution time analysis
   - Uses `get_pg_stat_statements_query()` for version compatibility
   - Supports both `total_time` (pre-14) and `total_exec_time` (14+)

6. **`hot_queries.py`** - Frequently executed queries
   - Uses version-specific `pg_stat_statements` queries
   - Handles column differences across versions

### System Monitoring Modules

7. **`monitoring_metrics.py`** - General monitoring metrics
   - Uses `get_monitoring_metrics_query()` for version-specific queries
   - Handles PostgreSQL 17+ `pg_stat_checkpointer` changes

8. **`section_cache_analysis.py`** - Cache analysis
   - Uses `get_cache_analysis_query()` for version-specific queries
   - Supports PostgreSQL 17+ checkpoint statistics changes

9. **`checkpoint.py`** - Checkpoint activity analysis
   - Handles PostgreSQL 17+ `pg_stat_checkpointer` vs `pg_stat_bgwriter`
   - Version-specific checkpoint statistics

10. **`bgwriter.py`** - Background writer analysis
    - Handles PostgreSQL 17+ `pg_stat_bgwriter` column changes
    - Version-specific background writer metrics

11. **`high_insert_tables.py`** - High insert activity
    - Validates minimum PostgreSQL 13 requirement
    - Uses stable `pg_stat_user_tables` queries

12. **`section_query_analysis.py`** - Query analysis
    - Uses `get_pg_stat_statements_query()` for version compatibility
    - Supports both old and new `pg_stat_statements` column names

## Version-Specific Features

### PostgreSQL 13+ (Minimum)
- All modules validate minimum version requirement
- Basic `pg_stat_statements` support
- Standard monitoring views

### PostgreSQL 14+
- `pg_stat_statements` uses `total_exec_time` instead of `total_time`
- `funcid` column removed from `pg_stat_statements`
- `blk_read_time` and `blk_write_time` removed from `pg_stat_statements`
- `wal_bytes` column added to `pg_stat_statements`

### PostgreSQL 17+
- `pg_stat_checkpointer` view introduced for checkpoint statistics
- `pg_stat_bgwriter` simplified (fewer columns)
- Vacuum progress includes `dead_tuple_bytes`, `max_dead_tuple_bytes`, `num_dead_item_ids`
- Checkpoint statistics moved to dedicated view

### PostgreSQL 18+ (Future)
- System designed to handle future changes
- Version detection includes PostgreSQL 18 flags
- Extensible query generation system

## Error Handling

### Version Validation
```python
# All modules now include version validation
is_supported, error_msg = validate_postgresql_version(compatibility)
if not is_supported:
    adoc_content.append(f"[ERROR]\n====\n{error_msg}\n====\n")
    structured_data["version_error"] = {"status": "error", "details": error_msg}
    return "\n".join(adoc_content), structured_data
```

### Graceful Degradation
- Modules provide clear error messages for unsupported versions
- Structured data includes version error information
- Reports continue to generate with version warnings

## Benefits

1. **Centralized Version Management**: Single source of truth for version compatibility
2. **Future-Proof**: Easy to add support for PostgreSQL 18+ features
3. **Consistent Error Handling**: Standardized version validation across all modules
4. **Maintainable**: Version-specific logic isolated in compatibility module
5. **Robust**: Graceful handling of version differences and missing features

## Testing Recommendations

1. **Test with PostgreSQL 13**: Verify minimum version enforcement
2. **Test with PostgreSQL 14**: Verify `pg_stat_statements` changes
3. **Test with PostgreSQL 17**: Verify checkpoint and vacuum changes
4. **Test with PostgreSQL 18**: Verify future compatibility

## Migration Notes

- Existing configurations continue to work
- No changes required to `config.yaml` files
- Version validation is automatic and transparent
- Error messages clearly indicate version requirements

## Future Enhancements

1. **PostgreSQL 18 Support**: Ready for future PostgreSQL features
2. **Additional Views**: Easy to add support for new monitoring views
3. **Performance Optimizations**: Version-specific query optimizations
4. **Extended Metrics**: Support for new PostgreSQL metrics and statistics 