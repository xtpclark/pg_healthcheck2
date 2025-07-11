# Cross-Node Tools for PostgreSQL Healthcheck

## Overview

This directory contains advanced tools for cross-node or multi-node analysis of PostgreSQL clusters. The primary utility is the **Cross-Node Index Usage Analyzer**, which connects to a primary and multiple replica nodes, aggregates index usage statistics, and generates a comprehensive AsciiDoc report with actionable recommendations for index cleanup.

---

## Features
- **Aggregates index usage across all nodes (primary + replicas)**
- **Identifies truly unused indexes** (unused everywhere, not supporting constraints)
- **Generates detailed AsciiDoc reports** with node-by-node summaries, unused index tables, and SQL removal recommendations
- **Configurable via YAML** for flexible environments (Docker, cloud, on-prem)
- **Safe recommendations**: never suggests dropping indexes supporting constraints
- **Storage savings estimation**
- **Easy CLI usage**

---

## Installation

1. **Install Python dependencies:**

```bash
pip install -r requirements.txt
```

Contents of `requirements.txt`:
```
psycopg2
PyYAML
```

2. **Ensure network access** to all PostgreSQL nodes (primary and replicas) from the machine running the analyzer.

---

## Configuration

Copy and edit the sample config:

```bash
cp cross_node_index_config.yaml.sample cross_node_index_config.yaml
```

**Sample config:**
```yaml
# Cross-Node Index Usage Analyzer Configuration
primary:
  host: "localhost"
  port: 5435
  database: "testdb"
  user: "testuser"
  password: "testpass"

replicas:
  - host: "localhost"
    port: 5436
    database: "testdb"
    user: "testuser"
    password: "testpass"
  - host: "localhost"
    port: 5437
    database: "testdb"
    user: "testuser"
    password: "testpass"

analysis:
  min_index_size_bytes: 1024
  include_system_schemas: false
  generate_removal_sql: true
  include_constraint_analysis: true

report:
  include_detailed_stats: true
  include_storage_calculation: true
  include_risk_assessment: true
```

- **primary**: Connection info for the primary node
- **replicas**: List of replica connection info
- **analysis/report**: Optional tuning for filtering, output, and risk

---

## Usage

From this directory (or project root):

```bash
python3 cross_node_index_analyzer.py --config cross_node_index_config.yaml --output cross_node.adoc
```

- `--config`: Path to your YAML config file
- `--output`: Path for the generated AsciiDoc report

**Example output:**
- `cross_node.adoc` (AsciiDoc report with executive summary, per-node stats, unused index table, and SQL recommendations)

---

## Example Output (Summary)

```
= Cross-Node Index Usage Analysis Report
:doctype: book
:encoding: utf-8
:lang: en
:toc: left
:numbered:

Generated on: 2025-07-11 13:59:06

== Executive Summary

This report analyzes index usage across 3 database nodes 
to identify indexes that can be safely removed.

**Analysis Results:**
- Total nodes analyzed: 3
- Unused indexes identified: 5
- Potential storage savings: 15.0 MB

== Node Analysis Summary

=== Primary
- Total indexes: 15
- Used indexes: 2
- Unused indexes: 13
- Usage rate: 13.3%

=== Replica_1
- Total indexes: 15
- Used indexes: 5
- Unused indexes: 10
- Usage rate: 33.3%

=== Replica_2
- Total indexes: 15
- Used indexes: 2
- Unused indexes: 13
- Usage rate: 13.3%

== Unused Indexes Analysis

The following indexes appear unused across all nodes and may be candidates for removal:

|Index Name|Table Name|Size|Usage Summary|Supports Constraints
|idx_users_created_at|public.users|16 kB|primary: 0 scans; replica_1: 0 scans; replica_2: 0 scans|No
|... (more rows) ...

== Index Removal Recommendations

[IMPORTANT]
=====
**Before removing any indexes:**
1. **Verify the analysis**
2. **Test in staging**
3. **Monitor performance**
4. **Low-traffic window**
5. **Backup plan**
=====

=== Recommended SQL Statements

[source,sql]
----
-- Remove unused index: idx_users_created_at
DROP INDEX CONCURRENTLY IF EXISTS idx_users_created_at;
... (more SQL) ...
----
```

---

## Best Practices & Notes
- **Never drop indexes in production without review and testing.**
- Always test recommendations in a staging environment first.
- The analyzer is conservative: it will not recommend dropping indexes that support constraints (PK, FK, UK).
- You can tune the minimum index size and other filters in the config.
- The tool requires read access to `pg_stat_user_indexes` and `pg_constraint` on all nodes.

---

## Extending or Contributing
- Place new cross-node or multi-node analysis tools in this directory.
- Follow the pattern of config-driven, report-generating utilities.
- PRs and suggestions welcome!

---

## See Also
- Main project README for single-node healthcheck and AI-powered recommendations.
- [pg_healthcheck2/README.md](../README.md) 
