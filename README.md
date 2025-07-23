# PostgreSQL Health Check Program v2.0

This repository contains an advanced, Python-based tool designed to assess the operational health, performance, and security posture of PostgreSQL database instances. It generates detailed reports in AsciiDoc format and leverages a configurable AI analysis engine to provide intelligent, context-aware, and audience-specific recommendations.

## Table of Contents

1. [Introduction](https://www.google.com/search?q=%231-introduction)
2. [Key Features](https://www.google.com/search?q=%232-key-features)
3. [Installation and Setup](https://www.google.com/search?q=%233-installation-and-setup)
4. [Configuration (`config.yaml`)](https://www.google.com/search?q=%234-configuration-configyaml)
5. [Running the Health Check](https://www.google.com/search?q=%235-running-the-health-check)
6. [How the AI Analysis Works (v2.0 Architecture)](https://www.google.com/search?q=%236-how-the-ai-analysis-works-v20-architecture)
7. [Contributing](https://www.google.com/search?q=%237-contributing)
8. [License](https://www.google.com/search?q=%238-license)

---

## 1. Introduction

The PostgreSQL Health Check program is a powerful diagnostic tool for PostgreSQL databases. It automates the collection of critical metrics and configuration settings, compiling them into a human-readable AsciiDoc report. Its unique AI integration provides intelligent, context-aware recommendations to help database administrators, developers, and security teams optimize their PostgreSQL environments.

Version 2.0 introduces a completely refactored AI analysis engine, decoupling data collection from prompt rendering. This allows for a "collect once, report many times" workflow, generating tailored reports for different audiences from a single set of collected data.

## 2. Key Features

* **Comprehensive Data Collection**: Gathers metrics on database overview, settings, cache, vacuum/bloat, WAL, indexes, tables, queries, connections, security, and AWS CloudWatch metrics for RDS/Aurora.
* **Modular & Extensible**: Each check is a self-contained module, making it easy to add new analyses.
* **Structured JSON Output**: Saves all raw findings to a `structured_health_check_findings.json` file, enabling offline analysis, historical trending, and integration with other tools.
* **Advanced AI Analysis Engine**:
    * **Configurable Severity Rules**: A central dictionary in `dynamic_prompt_generator.py` defines the rules for flagging issues as critical, high, or medium severity, making the logic transparent and easy to customize.
    * **Jinja2 Prompt Templating**: AI prompts are externalized into Jinja2 templates (`/templates` directory), allowing for easy customization of the AI's tone, format, and focus.
    * **Audience-Specific Reports**: Ships with sample templates for different audiences (DBA, Developer, Executive, Security Auditor).
    * **Smart Token Management**: Automatically summarizes non-critical data to ensure the prompt stays within the AI model's token limit, preventing errors on large databases.
* **Flexible Execution Modes**:
    * **Online Mode**: Run data collection and AI analysis in a single step.
    * **Offline Mode**: Collect data once, then run a separate script to generate AI reports later, with the ability to specify different prompt templates for different audiences.
* **Platform-Aware**: Provides context-aware checks for AWS RDS/Aurora, including RDS Proxy metrics.

## 3. Installation and Setup

### Prerequisites

* Python 3.8+
* `pip` package installer
* PostgreSQL client tools (`psql`)
* `boto3` for AWS integration (`pip install boto3`)
* Configured AWS credentials if using AWS features.

### Installation

1.  **Clone the Repository**:
    ```bash
    git clone <your-repo-url>
    cd pg_healthcheck2
    ```

2.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```
    (Ensure `requirements.txt` includes `psycopg2-binary`, `PyYAML`, `requests`, `boto3`, and `Jinja2`)

3.  **Configure `pg_stat_statements`**: For detailed query analysis, ensure the `pg_stat_statements` extension is enabled in your target database. This requires adding it to `shared_preload_libraries` and running `CREATE EXTENSION pg_stat_statements;`.

## 4. Configuration (`config.yaml`)

The `config/config.yaml` file controls the tool's behavior. Key settings for the new architecture are highlighted below.

```yaml
# --- Database Connection ---
host: your_db_host
port: 5432
database: your_db_name
user: your_db_user
password: your_db_password
company_name: YourCompany
row_limit: 10               # Row limit for detailed "top N" lists

# --- Environment Context ---
is_aurora: true             # Set to true for RDS/Aurora to enable AWS API calls
using_connection_pooler: true # Set to true to suppress warnings on high connection counts (e.g., with RDS Proxy)
rds_proxy_name: 'my-app-proxy' # Optional: The name of your RDS Proxy to fetch its specific metrics

# --- AI Configuration ---
ai_analyze: true            # Master switch to enable AI features
ai_run_integrated: true     # true = online mode; false = offline mode (collect data only)
ai_api_key: "YOUR_AI_API_KEY"
ai_endpoint: https://api.cool.ai/v1/
ai_model: "cool-ai-3"
prompt_template: "prompt_template.j2" # Default Jinja2 template to use for prompts
ai_max_prompt_tokens: 8000  # The token budget for the AI prompt. The tool will summarize data to stay under this limit.
ai_max_output_tokens: 4096  # The maximum number of tokens for the AI's response.
```

## 5. Running the Health Check

### Online Mode

This mode collects data and generates the AI report in one step.

```bash
# Use the default template specified in config.yaml
python3 ./pg_healthcheck.py --config=config/my_config.yaml
```

### Offline Mode

This mode decouples data collection from AI analysis, allowing you to generate multiple reports for different audiences from a single data snapshot.

**Step 1: Collect Data**
First, run the main script. This will connect to the database and save the raw `structured_health_check_findings.json` file. No AI call is made.

```bash
python3 ./pg_healthcheck.py --config=config/my_config.yaml
```

**Step 2: Generate AI Reports (Offline)**
Next, run the `offline_ai_processor.py` script. You can run this multiple times with different templates.

```bash
# Generate the standard technical report
python3 ./offline_ai_processor.py \
    --config=config/my_config.yaml \
    --findings=adoc_out/YourCompany/structured_health_check_findings.json

# Generate a high-level executive summary from the SAME data
python3 ./offline_ai_processor.py \
    --config=config/my_config.yaml \
    --findings=adoc_out/YourCompany/structured_health_check_findings.json \
    --template templates/executive_summary_template.j2

# Generate a security-focused audit from the SAME data
python3 ./offline_ai_processor.py \
    --config=config/my_config.yaml \
    --findings=adoc_out/YourCompany/structured_health_check_findings.json \
    --template templates/security_audit_template.j2
```

## 6. How the AI Analysis Works (v2.0 Architecture)

The AI analysis workflow has been designed for flexibility and power.

1.  **Data Collection**: The `pg_healthcheck.py` script runs all configured modules. Each module returns its findings as structured data, which is aggregated into a single `structured_health_check_findings.json` file. This file contains the complete, raw snapshot of the database's health.

2.  **Prompt Generation (On-the-Fly)**: The `dynamic_prompt_generator.py` module is now the central "brain." It is called just before the AI interaction (either in online or offline mode). It performs several key tasks:
    * **Severity Analysis**: It programmatically analyzes the raw findings against the configurable rules in `METRIC_ANALYSIS_CONFIG` to identify critical and high-priority issues.
    * **Smart Summarization**: To respect the `ai_max_prompt_tokens` limit, it includes the full data for modules with detected issues and creates summarized notes for "healthy" modules.
    * **Template Rendering**: It loads the specified Jinja2 template (e.g., `developer_focused_template.j2`) and injects the summarized findings, pre-analyzed critical issues, and database metadata to create the final, complete prompt.

3.  **AI Interaction**: The final, rendered prompt is sent to the AI model, which then generates the analysis tailored to the specific instructions and data provided in the template.

This decoupled architecture provides a robust and highly flexible system for generating intelligent, audience-specific database health reports.

## 7. Contributing

(Contribution guidelines to be added)

## 8. License

(License information to be added)
