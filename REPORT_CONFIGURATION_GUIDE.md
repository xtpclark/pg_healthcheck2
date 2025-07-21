# Report Configuration Guide

## Overview

The PostgreSQL Health Check tool now supports flexible report configurations through the `--report-config` parameter. This allows you to create custom report configurations for different use cases, environments, and requirements.

## Quick Start

### Basic Usage

```bash
# Use default report configuration
python3 pg_healthcheck.py

# Use a specific report configuration
python3 pg_healthcheck.py --report-config report_config_minimal.py

# Use custom config and report config
python3 pg_healthcheck.py --config config/prod_config.yaml --report-config report_config_security.py
```

### List Available Configurations

```bash
python3 pg_healthcheck.py --list-report-configs
```

## Available Report Configurations

### 1. Default Configuration (`report_config.py`)
**Use Case**: Comprehensive health check with all features
- **Sections**: All available analysis modules
- **Best For**: Complete database assessment
- **Duration**: 5-10 minutes (depending on database size)

### 2. Minimal Configuration (`report_config_minimal.py`)
**Use Case**: Quick health check with essential items only
- **Sections**: Critical performance, security, and issues only
- **Best For**: Regular monitoring, quick assessments
- **Duration**: 2-3 minutes

**Includes**:
- PostgreSQL Overview
- Critical Performance Settings
- Security Audit (including HBA rules)
- Critical Issues (unused indexes, missing PKs, locks, long queries)
- Recommendations

### 3. Security Configuration (`report_config_security.py`)
**Use Case**: Security-focused audit and compliance
- **Sections**: Security analysis, user management, connection security
- **Best For**: Security audits, compliance checks, penetration testing
- **Duration**: 3-5 minutes

**Includes**:
- Security Analysis (users, SSL, security audit, HBA rules, data checksums)
- Connection Security (metrics, pooling)
- Security Best Practices
- Platform-Specific Security
- Security Recommendations

### 4. Performance Configuration (`report_config_performance.py`)
**Use Case**: Performance optimization and tuning
- **Sections**: Performance analysis, query optimization, index analysis
- **Best For**: Performance tuning, bottleneck identification
- **Duration**: 5-8 minutes

**Includes**:
- Performance Settings (critical settings, suggested values)
- Cache Analysis
- Query Performance (top queries, hot queries, long running, wait events)
- Lock Analysis (lock waits, pg_locks analysis)
- Index Analysis (unused, missing, large, duplicate)
- Table Performance (large tables, metrics, foreign keys)
- Vacuum and Maintenance
- Performance Recommendations

## Creating Custom Report Configurations

### Structure

A report configuration file must define a `REPORT_SECTIONS` list containing section dictionaries:

```python
REPORT_SECTIONS = [
    {
        "title": "Section Title",
        "actions": [
            {"type": "module", "module": "module_name", "function": "function_name"},
            {"type": "comments", "file": "comments_file.txt", "display_title": "Optional Title"}
        ]
    }
]
```

### Action Types

#### 1. Module Actions
```python
{"type": "module", "module": "module_name", "function": "function_name"}
```

**Optional Conditions**:
```python
{
    "type": "module", 
    "module": "module_name", 
    "function": "function_name",
    "condition": {"var": "setting_name", "value": True}
}
```

#### 2. Comments Actions
```python
{"type": "comments", "file": "comments_file.txt", "display_title": "Optional Title"}
```

#### 3. Header Actions
```python
{"type": "header", "file": "report_header.txt"}
```

### Example Custom Configuration

```python
# Custom Report Configuration
# Use with: python3 pg_healthcheck.py --report-config my_custom_config.py

REPORT_SECTIONS = [
    {
        "title": "Report Header",
        "actions": [
            {"type": "header", "file": "report_header.txt"}
        ]
    },
    {
        "title": "Quick Overview",
        "actions": [
            {"type": "module", "module": "postgres_overview", "function": "run_postgres_overview"},
            {"type": "module", "module": "critical_performance_settings", "function": "run_critical_performance_settings"}
        ]
    },
    {
        "title": "My Custom Analysis",
        "actions": [
            {"type": "module", "module": "my_custom_module", "function": "run_my_custom_analysis"},
            {"type": "comments", "file": "my_custom_notes.txt", "display_title": "Custom Notes"}
        ]
    }
]
```

## Use Cases and Examples

### 1. Development Environment
```bash
# Quick check for development database
python3 pg_healthcheck.py --report-config report_config_minimal.py --output dev_health_check.adoc
```

### 2. Production Security Audit
```bash
# Comprehensive security audit
python3 pg_healthcheck.py --config config/prod_config.yaml --report-config report_config_security.py --output security_audit.adoc
```

### 3. Performance Tuning
```bash
# Performance-focused analysis
python3 pg_healthcheck.py --config config/prod_config.yaml --report-config report_config_performance.py --output performance_analysis.adoc
```

### 4. Compliance Reporting
```bash
# Create custom compliance configuration
python3 pg_healthcheck.py --report-config report_config_compliance.py --output compliance_report.adoc
```

## Advanced Features

### Conditional Sections
Sections can be conditionally included based on configuration settings:

```python
{
    "condition": {"var": "run_osinfo", "value": True},
    "title": "System Details",
    "actions": [
        {"type": "module", "module": "get_osinfo", "function": "run_osinfo"}
    ]
}
```

### Conditional Actions
Individual actions can be conditionally executed:

```python
{
    "type": "module", 
    "module": "run_recommendation_enhanced", 
    "function": "run_recommendation_enhanced", 
    "condition": {"var": "ai_analyze", "value": True}
}
```

### Fallback Actions
Provide fallback actions when conditions aren't met:

```python
{
    "type": "module", 
    "module": "run_recommendation", 
    "function": "run_recommendation", 
    "condition": {"var": "ai_analyze", "value": True, "fallback": True}
}
```

## Best Practices

### 1. Naming Conventions
- Use descriptive names: `report_config_production.py`, `report_config_audit.py`
- Include purpose in filename: `report_config_performance_tuning.py`

### 2. Documentation
- Add comments explaining the configuration's purpose
- Include usage examples in comments
- Document any special requirements or dependencies

### 3. Testing
- Test configurations on staging environments first
- Verify all referenced modules exist
- Check that all referenced comment files exist

### 4. Organization
- Group related sections logically
- Use consistent section ordering across configurations
- Include essential sections (overview, recommendations) in all configurations

## Troubleshooting

### Common Issues

#### 1. Module Not Found
```
Error: Module my_module.function_name failed: No module named 'modules.my_module'
```
**Solution**: Ensure the module exists in the `modules/` directory

#### 2. Comments File Not Found
```
Error: Comments file my_file.txt not found.
```
**Solution**: Ensure the comments file exists in the `comments/` directory

#### 3. Configuration File Not Found
```
Error: Report configuration file my_config.py not found.
```
**Solution**: Ensure the configuration file exists in the current directory

### Validation

The tool automatically validates:
- Configuration file existence
- Report configuration file existence
- Module availability (at runtime)
- Comments file availability (at runtime)

## Integration with Existing Workflows

### CI/CD Integration
```bash
# Automated security checks
python3 pg_healthcheck.py --report-config report_config_security.py --output security_report.adoc

# Performance monitoring
python3 pg_healthcheck.py --report-config report_config_performance.py --output performance_report.adoc
```

### Scheduled Monitoring
```bash
# Daily quick check
0 6 * * * cd /path/to/healthcheck && python3 pg_healthcheck.py --report-config report_config_minimal.py

# Weekly comprehensive check
0 8 * * 0 cd /path/to/healthcheck && python3 pg_healthcheck.py --report-config report_config.py
```

### Custom Scripts
```bash
#!/bin/bash
# Custom health check script
case "$1" in
    "security")
        python3 pg_healthcheck.py --report-config report_config_security.py
        ;;
    "performance")
        python3 pg_healthcheck.py --report-config report_config_performance.py
        ;;
    "quick")
        python3 pg_healthcheck.py --report-config report_config_minimal.py
        ;;
    *)
        python3 pg_healthcheck.py
        ;;
esac
```

## Future Enhancements

### Planned Features
- **Template System**: Pre-built templates for common use cases
- **Dynamic Configuration**: Runtime configuration based on database characteristics
- **Configuration Validation**: Enhanced validation of configuration files
- **Configuration Sharing**: Repository of community-contributed configurations

### Contributing
- Share your custom configurations with the community
- Follow the naming and documentation conventions
- Test configurations thoroughly before sharing
- Include usage examples and documentation

---

**Note**: The report configuration feature maintains backward compatibility. Existing scripts will continue to work with the default `report_config.py` configuration. 