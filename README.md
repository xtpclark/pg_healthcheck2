# PostgreSQL Health Check Program

This repository contains a Python-based tool designed to assess the operational health, performance, and security posture of PostgreSQL database instances. It generates a detailed report in AsciiDoc format and can leverage Artificial Intelligence (AI) to provide aggregated and prioritized recommendations.

## Table of Contents

1.  [Introduction](#1-introduction)
2.  [Features](#2-features)
3.  [Installation and Setup](#3-installation-and-setup)
    * [Prerequisites](#31-prerequisites)
    * [Clone the Repository](#32-clone-the-repository)
    * [Install Python Dependencies](#33-install-python-dependencies)
    * [PostgreSQL Database Configuration](#34-postgresql-database-configuration)
4.  [Configuration (`config.yaml`)](#4-configuration-configyaml)
5.  [Running the Health Check](#5-running-the-health-check)
6.  [Test Case Generation (`create_test_cases.sh`)](#6-test-case-generation-create_test_casessh)
7.  [Report Sections and Analyses](#7-report-sections-and-analyses)
8.  [How AI Analysis Works](#8-how-ai-analysis-works)
9.  [Contributing](#9-contributing)
10. [License](#10-license)

---

## 1. Introduction

The PostgreSQL Health Check program is a powerful diagnostic tool for PostgreSQL databases. It automates the collection of critical metrics, configuration settings, and security-related information, compiling them into an easy-to-read AsciiDoc report. Its unique AI integration provides intelligent, context-aware recommendations to help database administrators and developers optimize their PostgreSQL environments.

## 2. Features

* **Comprehensive Data Collection**: Gathers data on database overview, settings, cache, vacuum, WAL, checkpoints, indexes, tables, queries, connections, and security.
* **Modular Design**: Each analysis area is encapsulated in a separate Python module, making the tool extensible and maintainable.
* **Configurable Report Structure**: The `report_config.py` file allows full control over the report's sections, order, and included content.
* **AsciiDoc Output**: Generates human-readable reports in AsciiDoc format, which can be easily converted to HTML, PDF, or other formats.
* **Structured JSON Output**: All collected raw data is saved in a JSON file, enabling further programmatic analysis, historical trending, or integration with other tools.
* **AI-Powered Recommendations**: Integrates with large language models (LLMs) like Google Gemini or OpenAI to provide intelligent, prioritized, and actionable recommendations based on a holistic view of the collected data.
* **Flexible AI Execution**: Supports both integrated (online) AI analysis during report generation and offline/separate AI processing for environments with network restrictions (e.g., corporate VPNs).
* **Test Case Generation**: Includes a script (`create_test_cases.sh`) to easily populate a test database with diverse scenarios for comprehensive testing.
* **Platform-Aware Advice**: Provides general and platform-specific best practices (e.g., for AWS RDS/Aurora, and others).
* **Detailed AI Metrics**: Reports on the AI endpoint, model used, prompt/response character counts, and analysis time directly within the generated report.
* **Custom SSL Certificate Support**: Allows specifying a custom SSL certificate for secure communication with AI API endpoints that require it.

## 3. Installation and Setup

### 3.1. Prerequisites

Ensure you have the following installed on the machine where you will run the health check script:

* **Python 3.x**: Recommended Python version (e.g., 3.8 or newer).
* **`pip`**: Python package installer (usually comes with Python).
* **`psql` client**: PostgreSQL command-line client (for `pgbench` and `pgbouncer` commands).
* **`pgbench`**: PostgreSQL benchmarking tool (optional, but recommended for generating test data).
* **`boto3`**: AWS SDK for Python (required for AWS CloudWatch/RDS metrics integration if `is_aurora: true`).
    * `pip install boto3`
* **AWS Credentials**: If connecting to AWS RDS/Aurora, ensure your AWS credentials are configured (e.g., via environment variables `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`, or `~/.aws/credentials` file).
    * **Required IAM Permissions for Cloud Metrics**:
        * `cloudwatch:GetMetricStatistics`
        * `rds:DescribeDBInstances` (to get DB instance/cluster identifier from endpoint)
        * `rds:DescribeDBClusters` (for Aurora clusters)

### 3.2. Clone the Repository

Clone the project repository to your local machine:

```bash
git clone [https://github.com/your-repo/pg_healthcheck2.git](https://github.com/your-repo/pg_healthcheck2.git) # Replace with your actual repo URL
cd pg_healthcheck2
```

### 3.3. Install Python Dependencies

Navigate to the project directory and install the required Python libraries:

```bash
pip install -r requirements.txt
```

If `requirements.txt` is not present, create it with the following content and then run `pip install -r requirements.txt`:

```
# requirements.txt
psycopg2-binary
PyYAML
requests
boto3 # Only if you plan to use AWS CloudWatch/RDS metrics integration
```

### 3.4. PostgreSQL Database Configuration

For comprehensive analysis, ensure your PostgreSQL database is configured to expose necessary statistics. These changes typically require a database restart or reload.

* **`pg_stat_statements`**: This extension is crucial for detailed query analysis.
    * Add `pg_stat_statements` to `shared_preload_libraries` in your `postgresql.conf` (or DB parameter group for RDS/Aurora).
    * **Restart PostgreSQL**.
    * Connect to your target database and run: `CREATE EXTENSION pg_stat_statements;`
* **Logging**: For `pgBadger` analysis (if configured), ensure appropriate logging is enabled in `postgresql.conf`. These settings are read by `pgBadger` from log files, not directly by this health check tool.
    * `log_destination = 'csvlog'` (or `stderr` if you have a log collector)
    * `logging_collector = on` (if using `csvlog` or `stderr` to files)
    * `log_directory = 'pg_log'`
    * `log_filename = 'postgresql-%Y-%m-%d_%H%M%S.log'`
    * `log_min_duration_statement = 0` (or a value to capture slow queries; set to `0` to log all statements, but be cautious of log volume)
    * `log_connections = on`
    * `log_disconnections = on`
    * `log_line_prefix = '%t [%p]: [%l-1] user=%u,db=%d,app=%a,client=%h '`
    * `log_lock_waits = on`
    * `log_autovacuum_min_duration = 0`

## 4. Configuration (`config.yaml`)

The `config/config.yaml` file is the central place to configure the health check program.

```yaml
# config/config.yaml example
host: your_db_host
port: 5432
database: your_db_name
user: your_db_user
password: your_db_password
company_name: YourCompany
report_title: Database Health Check Report
load_dba_views: true        # Set to true if you have custom DBA views to load (not implemented in current modules)
create_history_db: true     # Set to true to create a history database (not fully implemented in current modules)
show_qry: true              # Set to true to include SQL queries in the report
row_limit: 10               # Maximum number of rows to display for tabular query results
logo_image: MyLogo.svg[900,900] # Path to a logo image (relative to adoc_out/company_name/images)
run_osinfo: true            # Set to true to run OS information collection (from the client machine)
run_settings: true          # Set to true to run the pgset module in Appendix
show_avail_ext: true        # Set to true to show available extensions in Appendix
statio_type: user           # Type of statistics to collect (e.g., 'user' for user tables)
pgbouncer_cmd: psql -h localhost -p 6432 -U pgbouncer_user pgbouncer # Command for PgBouncer admin access
is_aurora: false            # Set to true if analyzing an AWS RDS Aurora instance (enables boto3 calls)
min_tup_ins_threshold: 500000 # Minimum tuples inserted for a table to be considered "high insert activity"

# AI Configuration
ai_analyze: true            # Master switch: Set to true to enable AI analysis (whether integrated or offline)
ai_api_key: "YOUR_AI_API_KEY" # Your AI API key for authentication with the AI endpoint
ai_endpoint: "[https://your-ai-api-endpoint.com/v1](https://your-ai-api-endpoint.com/v1)" # The URL of your AI API (e.g., Google Gemini, OpenAI-compatible proxy)
ai_model: "your-ai-model-name" # The specific AI model to use (e.g., "gemini-2.0-flash", "gpt-4.1")
ai_user: "healthcheck_runner" # Optional: User identifier to send with AI requests for context/logging
ai_run_integrated: true     # Set to true for AI analysis during main report generation; false for offline processing
ai_user_header: "X-User-ID" # Optional: Custom HTTP header name for ai_user (e.g., for corporate proxies/AIs)
ssl_cert_path: "/path/to/your/custom/cert.pem" # Optional: Path to a custom SSL certificate for verifying AI endpoint (e.g., for corporate proxies)

```

## 5. Running the Health Check

To run the health check program and generate the report:

```bash
python3 ./pg_healthcheck.py
```

The generated AsciiDoc report (`health_check.adoc`) will be saved in the `adoc_out/<company_name>/` directory. A structured JSON file (`structured_health_check_findings.json`) containing all raw data collected will also be saved in the same directory.

### 5.1. Running Offline AI Analysis

If `ai_analyze` is `true` but `ai_run_integrated` is `false` in your `config.yaml`, the main `pg_healthcheck.py` script will generate the `structured_health_check_findings.json` file but *skip* the direct AI API call. You can then use a separate script to process this JSON file offline (e.g., from a machine within your corporate VPN).

To run the offline AI analysis:

```bash
python3 ./offline_ai_processor.py --config config/config.yaml --findings adoc_out/YourCompany/structured_health_check_findings.json
```
* Replace `config/config.yaml` with the path to your configuration file.
* Replace `adoc_out/YourCompany/structured_health_check_findings.json` with the actual path to your generated JSON findings file.

The script will print the AI's recommendations and analysis statistics to the console. You can then manually integrate these into your `health_check.adoc` report if desired.

## 6. Test Case Generation (`create_test_cases.sh`)

The `create_test_cases.sh` script is provided to populate your test database with various objects and activity, allowing you to test the full functionality of the health check report.

**WARNING**: This script will modify your database. It is highly recommended to run this on a non-production or test database.

```bash
chmod +x create_test_cases.sh
./create_test_cases.sh
```
This script will:
* Create a `pgbench_test_db` database and run `pgbench` to generate transaction load.
* Create a partitioned table (`public.sensor_data`) and populate it.
* Create a materialized view (`public.daily_sales_summary`) and refresh it.
* Create test users (`insecure_user`, `admin_user`) with specific privileges.
* Create tables with indexed and unindexed foreign keys (`public.orders_indexed_fk`, `public.orders_unindexed_fk`) to test FK audit.

Cleanup commands are provided at the end of the script to remove these test objects.

## 7. Report Sections and Analyses

The health check report is structured into several key sections, each providing specific insights into your PostgreSQL database. The content and order of these sections are defined in the `report_config.py` file.

### 7.1. Core Database Metrics & Configuration

* **Background**: Provides contextual information about the database environment (from `comments/background.txt`).
* **PostgreSQL Overview**: Basic database information (version, size, uptime, key config settings).
* **System Details**: Operating system and hardware information of the *client machine* running the script (from `get_osinfo.py`).
* **PostgreSQL Settings**:
    * **General Configuration Settings**: A broad range of PostgreSQL configuration parameters.
    * **Critical Performance Settings**: Focused analysis of key performance-impacting parameters.
    * **Aurora CPU and IOPS Metrics**: Database-internal indicators of CPU/IOPS usage, and if `is_aurora: true`, fetches actual CloudWatch metrics from AWS.
    * **Datadog Monitoring Setup**: Checks PostgreSQL configuration relevant to Datadog monitoring.
    * **Monitoring Setup**: Checks PostgreSQL configuration relevant to general database monitoring solutions.
* **Cache Analysis**: PostgreSQL buffer cache usage and hit ratios.
* **Vacuum, Bloat and TXID Wrap Analysis**:
    * **Vacuum and Bloat Analysis**: Vacuum activity, dead tuples, and transaction ID wraparound risks.
    * **Autovacuum Configuration Analysis**: Key autovacuum settings for efficient bloat management.
    * **Vacuum Progress and Statistics & Per-Table Stats Suggestions**: Ongoing vacuum operations, historical stats, and tables potentially needing per-table statistics.
    * **Table Metrics**: Table sizes, live/dead tuples, and vacuum/analyze status.
* **WAL and Checkpoints**:
    * **Checkpoint Activity**: Analysis of checkpoint activity to optimize WAL performance.
    * **WAL Usage and Archiving**: WAL usage and archiving status for recovery.
    * **Background Writer Statistics**: Performance metrics for the background writer process.
    

### 7.2. Performance & Object Analysis

* **Index Analysis for db: `<DATABASE_NAME>`**:
    * **Unused Indexes**: Identifies indexes with zero scan count.
    * **Duplicate Indexes**: Finds redundant indexes.
    * **Tables with Potentially Missing Indexes**: Highlights tables with high sequential scans.
    * **Largest Indexes**: Lists indexes by size.
    * **BRIN Index Analysis**: Specific analysis for Block Range Indexes.
    * **GIN Index Analysis**: Specific analysis for Generalized Inverted Indexes.
* **Table Analysis for db: `<DATABASE_NAME>`**:
    * **Large Tables Analysis**: Identifies largest tables by size.
    * **Database Object Counts**: Summary counts of tables, views, functions, materialized views, indexes, sequences, schemas, foreign keys, and partitions.
    * **Materialized View Analysis**: Size and refresh status of materialized views.
    * **Partitioned Tables Analysis**: Details on parent tables and their individual partitions.
    * **High Tuple Write Queries**: Identifies tables with high insert rates and associated queries.
    * **Table Metrics**: Live/dead tuples and vacuum/analyze status for tables.
    * **Foreign Key Audit**: Identifies foreign keys missing indexes on child tables, with generated SQL recommendations.
    * **Tables Without Primary or Unique Keys**: Flags tables lacking these crucial constraints, important for logical replication.
* **Query Analysis**:
    * **Top Queries by Execution Time**: Identifies slowest queries (requires `pg_stat_statements`).
    * **Active Query States**: Shows current states of database connections.
    * **Long-Running Queries**: Lists queries active for extended periods.
    * **Lock Wait Configuration**: Status of `log_lock_waits` setting.
    * **Current Lock Waits**: Identifies sessions currently waiting for locks.
    * **Hot Queries**: Identifies most frequently executed queries (requires `pg_stat_statements`).

### 7.3. Security & Connectivity

* **Connections and Security for db: `<DATABASE_NAME>`**:
    * **User Analysis**: Database user roles and permissions.
    * **SSL Connection Statistics**: SSL usage by connections.
    * **Security Audit**: Superuser roles, password issues, public schema permissions, key security settings.
    * **Connection Metrics**: Total connections, states, and connections by user/database.
    * **Connection Pooling Analysis**: PgBouncer statistics (if configured).

### 7.4. Recommendations & Best Practices

* **Recommendations**:
    * **AI-Generated Recommendations**: Provides aggregated, prioritized, and actionable recommendations based on the AI's analysis of all collected structured data, including details on the AI model and analysis metrics.
    * **General Recommendations Overview**: General best practices and advice (from `comments/recommendations.txt`).
    * **pgBadger Setup and Analysis Recommendations**: Guidance on configuring PostgreSQL logging for `pgBadger` and how to use the tool for log analysis (from `comments/pgbadger_setup.txt`).
* **General PostgreSQL Best Practices**:
    * **Index Management Best Practices** (from `comments/indexes.txt`)
    * **Table Management Best Practices** (from `comments/tables.txt`)
    * **User and Role Management Best Practices** (from `comments/users.txt`)
    * **General Security Best Practices** (from `comments/security.txt`)
    * **Connection Management Best Practices** (from `comments/connections.txt`)
    * **High Availability (HA) Best Practices** (from `comments/ha.txt`)
* **Platform-Specific Best Practices**:
    * **AWS RDS/Aurora Best Practices** (from `comments/rds_aurora_best_practices.txt`)
    * **Instaclustr Managed PostgreSQL Best Practices** (from `comments/instaclustr_best_practices.txt`)
    * **NetApp ANF / FSx Storage Best Practices for PostgreSQL** (from `comments/netapp_anf_best_practices.txt`)


### 7.5. Appendix

* **All PostgreSQL Settings**: A comprehensive dump of all `pg_settings` parameters (from `pgset.py`).
* **System-Wide Extensions**: Lists all installed PostgreSQL extensions (from `systemwide_extensions.py`).
* **RDS/Aurora Upgrade Considerations**: General advice for upgrading managed cloud databases (from `rds_upgrade.py`).
* **AWS Region Considerations**: Provides notes on AWS region-specific considerations and fetches key CloudWatch metrics if configured for RDS/Aurora (from `check_aws_region.py`).

## 8. How AI Analysis Works

The AI analysis feature is a core differentiator of this health check program, providing intelligent and contextual recommendations.

### 8.1. Structured Data Collection

* Each analysis module in the health check is designed to not only generate human-readable AsciiDoc output but also to return its findings in a machine-readable, structured format (Python dictionaries or lists of dictionaries).
* This raw, structured data from all executed modules is aggregated into a central dictionary (`self.all_structured_findings`) within the main `HealthCheck` class.
* Before saving or sending to AI, any non-JSON-serializable data types (like `Decimal` or `datetime` objects from `psycopg2`) are automatically converted to standard JSON-compatible types (e.g., `float` for `Decimal`, ISO 8601 strings for `datetime`) by a custom JSON encoder.
* This aggregated structured data is always saved to a JSON file (`structured_health_check_findings.json`) in the output directory, enabling offline analysis or integration with other tools.

### 8.2. AI Prompt Construction

* The `run_recommendation.py` module is responsible for orchestrating the AI analysis.
* It iterates through the `self.all_structured_findings` dictionary.
* It constructs a comprehensive text prompt string, embedding the collected structured data (formatted as JSON snippets) along with a clear request for prioritized recommendations. This prompt provides the AI with a holistic view of the database's state across all analyzed areas.

### 8.3. AI API Interaction (Conditional)

The interaction with the AI API is conditional, based on settings in `config.yaml`:

* **`ai_analyze: true` (Master Switch)**: If this is `false`, AI analysis is entirely skipped, and a note is added to the report.
* **`ai_run_integrated: true` (Integrated Mode)**: If `ai_analyze` is `true` and `ai_run_integrated` is `true`, the `run_recommendation.py` module makes an HTTP POST request directly to the configured AI endpoint during the main report generation.
    * The `ai_api_key`, `ai_endpoint`, `ai_model`, `ai_user`, `ai_user_header`, and `ssl_cert_path` are loaded from `config.yaml`.
    * The prompt is sent as the user's message, and the `ai_user` is included in the payload for compatible APIs.
    * The request uses the specified `ssl_cert_path` for SSL verification.
    * Logic is included to handle both Google Gemini API and other OpenAI-compatible API responses.
    * Custom headers (like `ai_user_header`) can be configured for corporate proxy/AI authentication.
    * Metrics such as prompt/response character counts, analysis time, and (if available) token usage are collected and included in the report.
* **`ai_run_integrated: false` (Offline Mode)**: If `ai_analyze` is `true` but `ai_run_integrated` is `false`, the `run_recommendation.py` module *does not* make the API call. Instead, it generates a note in the report instructing the user to use the saved `structured_health_check_findings.json` file with a separate `offline_ai_processor.py` script to perform the AI analysis.




### 8.4. Recommendation Generation and Integration

* The AI model processes the comprehensive prompt and generates a text response containing its analysis and prioritized recommendations.
* This AI-generated text is then captured by the `run_recommendation.py` module (in integrated mode) or by the `offline_ai_processor.py` script (in offline mode).
* Finally, the AI's recommendations, along with the collected AI analysis metrics, are integrated directly into the AsciiDoc report under the "AI-Generated Recommendations" sub-section, providing actionable insights alongside the raw data.

This AI integration elevates the health check from a diagnostic tool to a prescriptive one, offering intelligent guidance to optimize your PostgreSQL database.

## 9. Contributing

(Section to be filled with contribution guidelines)

## 10. License

(Section to be filled with license information)
