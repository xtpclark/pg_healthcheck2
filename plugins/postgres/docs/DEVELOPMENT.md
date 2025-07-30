PostgreSQL Health Check Development Guidelines

Development of modules for the PostgreSQL Health Check program adheres to strict guidelines to ensure robustness, maintainability, and accurate reporting across various PostgreSQL versions.

    PostgreSQL Version Awareness:

        Modules must be version-aware, adapting queries for differences across PostgreSQL 14+, 17+, and older versions.

        Version detection is standardized by querying SHOW server_version_num;.

        Specific views requiring version-specific queries include pg_stat_bgwriter (columns removed in PG17+), pg_stat_wal (no direct checkpoint counters), pg_stat_checkpointer (new in PG17+ for checkpoint stats), pg_stat_statements (removed funcid in PG14+), and pg_stat_progress_vacuum (columns changed in PG17+).

    Module Structure & Reporting:

        Modules receive cursor, settings, execute_query, execute_pgbouncer, and all_structured_findings as inputs.

        They must return a tuple (adoc_content_string, structured_data_dict).

        SQL queries for show_qry are conditionally appended within [,sql]\n---- blocks.

        Errors are caught and reported using [ERROR] in AsciiDoc and a status: "error" in structured data.

        [TIP] and [NOTE] blocks are used for best practices and contextual information.

    AsciiDoc Table Formatting:

        To prevent table breakage from long strings, newlines, or pipe characters in query results, the query column in SQL selections must be sanitized using REPLACE(REPLACE(LEFT(query, 150), E'\\n', ' '), '|', ' ') || '...' AS query.

    Structured Data & AI Integration:

        all_structured_findings centrally aggregates raw, machine-readable data from all modules.

        Modules contribute findings as structured_data_dict.

        A CustomJsonEncoder (in pg_healthcheck.py) handles JSON serialization for Decimal (to float), datetime (to ISO 8601), and timedelta (to total seconds).

        AI settings (ai_api_key, ai_endpoint, ai_model, ai_user, ai_user_header, ssl_cert_path, ai_temperature, ai_max_output_tokens) are loaded from config.yaml and passed to AI API calls.

        AI analysis metrics (time, prompt/response character counts, token usage) are collected and reported in both structured data and AsciiDoc tables.

    Idempotency & Testing:

        The create_test_suite.sh script ensures database idempotency and generates diverse test data, including specific cases for new modules (e.g., SECURITY DEFINER functions, various index types, high insert activity).

    General Principles:

        Maintain strong modularity and separation of concerns.

        Implement robust error handling throughout the codebase.

        Ensure consistency in reporting formats and data structures.
