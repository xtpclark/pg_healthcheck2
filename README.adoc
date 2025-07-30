= PostgreSQL Health Check Tool v2.0
:toc: left
:toclevels: 3
:sectnums:

An advanced, Python-based tool to assess the operational health, performance, and security of various database technologies. It generates detailed reports in AsciiDoc format and leverages a configurable AI analysis engine to provide intelligent, context-aware recommendations.

'''
== Key Features

* **Pluggable Architecture**: Easily extend the tool to support new database technologies like MySQL, Cassandra, etc., without modifying the core engine.
* **Comprehensive Data Collection**: Gathers critical metrics on configuration, performance, security, and more.
* **Structured JSON Output**: Saves all raw findings to a `structured_health_check_findings.json` file, enabling offline analysis and integration.
* **Advanced AI Analysis Engine**:
    ** **Configurable Severity Rules**: A central `analysis_rules.py` for each plugin defines the logic for flagging issues.
    ** **Jinja2 Prompt Templating**: AI prompts are externalized into Jinja2 templates for easy customization of tone and focus.
    ** **Smart Token Management**: Automatically summarizes non-critical data to ensure the prompt stays within the AI model's token limit.
* **Flexible Execution Modes**: Supports both online (integrated) and offline AI analysis.

'''
== The Pluggable Architecture

The tool is designed around a powerful pluggable system that allows for easy extension to new database technologies. The core application is completely database-agnostic.

=== How It Works

. **Plugin Discovery**: On startup, the `main.py` script discovers all available plugins in the `plugins/` directory. Each plugin is a self-contained package that implements a common interface (`plugins/base.py`).
. **Configuration Driven**: The `config.yaml` file specifies which plugin to use via the `db_type` key (e.g., `postgres`, `mysql`).
. **Standardized Components**: Each plugin contains its own:
    * **`connector.py`**: Handles all technology-specific connection and query execution logic.
    * **`checks/`**: A directory of health check modules written for that technology.
    * **`reports/`**: Report definitions that specify which checks to run and in what order.
    * **`rules/`**: AI analysis rules tailored to the findings of that plugin's checks.

This decoupled architecture means that adding support for a new database is as simple as creating a new plugin folder with the required components. The core engine, AI analysis, and reporting logic remain unchanged.

'''
== Installation and Setup

=== Prerequisites

* Python 3.8+
* `pip` package installer
* Client libraries for the desired database technology (e.g., `psycopg2-binary` for PostgreSQL, `mysql-connector-python` for MySQL).

=== Installation

. **Clone the Repository**:
+
[source,bash]
----
git clone <your-repo-url>
cd pg_healthcheck2
----

. **Install Dependencies**:
+
[source,bash]
----
pip install -r requirements.txt
----

'''
== Configuration (`config.yaml`)

The `config/config.yaml` file controls the tool's behavior. The most important setting is `db_type`.

[source,yaml]
----
# --- Select the Database Plugin ---
db_type: postgres # Options: postgres, mysql, cassandra, etc.

# --- Connection Settings (example for postgres) ---
host: your_db_host
port: 5432
database: your_db_name
user: your_db_user
password: your_db_password
company_name: YourCompany

# --- AI Configuration ---
ai_analyze: true
ai_run_integrated: false # Use offline processor for more control
# ... other settings
----

'''
== Running the Health Check

. **Collect Data**: Run the main script. It will use the connector and checks from the plugin specified in your config.
+
[source,bash]
----
python3 main.py --config=config/my_postgres_config.yaml
----

. **Generate AI Reports (Offline)**: Use the offline processor to generate reports from the collected data. You can use different templates for different audiences.
+
[source,bash]
----
# Generate a technical DBA report
python3 offline_ai_processor.py \
    --config=config/my_postgres_config.yaml \
    --findings=adoc_out/YourCompany/structured_health_check_findings.json \
    --template templates/prompt_template.j2

# Generate a high-level executive summary from the SAME data
python3 offline_ai_processor.py \
    --config=config/my_postgres_config.yaml \
    --findings=adoc_out/YourCompany/structured_health_check_findings.json \
    --template templates/executive_summary_template.j2
----
