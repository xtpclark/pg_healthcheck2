import json
from decimal import Decimal # Import Decimal
from datetime import datetime # Import datetime

# Helper function to convert Decimal and datetime objects to JSON-serializable types recursively
def convert_to_json_serializable(obj):
    if isinstance(obj, Decimal):
        return float(obj) # Convert Decimal to float
    elif isinstance(obj, datetime):
        return obj.isoformat() # Convert datetime to ISO 8601 string
    elif isinstance(obj, dict):
        return {k: convert_to_json_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_json_serializable(elem) for elem in obj]
    else:
        return obj

def run_recommendation(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Aggregates findings from various health check modules, constructs a prompt,
    sends it to an AI for analysis, and integrates the AI's recommendations
    into the report.
    """
    adoc_content = ["=== Recommendations", "Provides aggregated recommendations based on the health check findings."]
    structured_data = {"ai_analysis": {}, "prompt_sent": ""} # To store AI related data

    if settings['show_qry'] == 'true':
        adoc_content.append("Recommendation generation logic involves AI analysis based on collected data.")
        adoc_content.append("----")

    # --- Step 1: Construct the AI Prompt ---
    prompt_parts = []
    prompt_parts.append("Analyze the following PostgreSQL health check report data and provide actionable, prioritized recommendations.\n\n")
    prompt_parts.append("--- PostgreSQL Health Check Findings ---\n\n")

    # Prepare findings for prompt by converting non-JSON-serializable types
    # Create a copy to avoid modifying the original all_structured_findings in place
    findings_for_prompt = convert_to_json_serializable(all_structured_findings)

    # Iterate through all collected structured findings
    for module_name, module_findings in findings_for_prompt.items():
        prompt_parts.append(f"** Module: {module_name.replace('_', ' ').title()} **\n")
        
        if module_findings.get("status") == "failed_to_load":
            prompt_parts.append(f"  Status: Failed to load/execute. Error: {module_findings.get('error', 'Unknown error')}\n")
        elif module_findings.get("note"):
            prompt_parts.append(f"  Note: {module_findings.get('note')}\n")
        elif module_findings.get("status") == "not_applicable":
            prompt_parts.append(f"  Status: Not Applicable. Reason: {module_findings.get('reason', 'N/A')}\n")
        elif module_findings.get("status") == "error":
            prompt_parts.append(f"  Status: Query Error. Details: {json.dumps(module_findings.get('details', {}), indent=2)}\n")
        elif module_findings.get("status") == "success" and module_findings.get("data") is not None: # Check for None explicitly
            # For successful data, dump the raw JSON data
            prompt_parts.append(f"  Data:\n{json.dumps(module_findings['data'], indent=2)}\n")
        else:
            prompt_parts.append("  Status: No specific data or unhandled status.\n")
        prompt_parts.append("\n") # Add a newline for separation between modules

    prompt_parts.append("\nBased on these findings, provide a concise, prioritized list of recommendations. For each recommendation, briefly explain its importance and suggest specific actions. Focus on performance, stability, and security improvements relevant to a PostgreSQL database, especially considering it might be an AWS RDS Aurora instance if 'is_aurora' is true in settings.\n")

    full_prompt = "".join(prompt_parts)
    structured_data["prompt_sent"] = full_prompt # Store the prompt that was sent

    # --- Step 2: Make the AI API Call (Placeholder) ---
    # For now, simulate AI response for local testing:
    ai_recommendations = """
    **Prioritized Recommendations:**

    1.  **Enable `pg_stat_statements`**:
        * **Importance**: Critical for detailed query performance analysis. Without it, sections like "Top CPU-Intensive Queries" and "Top Queries by Execution Time" are empty, limiting insights into performance bottlenecks.
        * **Action**: Add `pg_stat_statements` to `shared_preload_libraries` in your `postgresql.conf` (or parameter group for RDS/Aurora) and restart the database. Then, run `CREATE EXTENSION pg_stat_statements;` in your database.

    2.  **Index `public.orders_unindexed_fk.product_id`**:
        * **Importance**: Prevents write amplification. When parent table `public.parent_products` is updated or deleted, PostgreSQL performs a full table scan on `public.orders_unindexed_fk` to check for referencing rows, consuming excessive I/O and CPU.
        * **Action**: Execute `CREATE INDEX CONCURRENTLY idx_orders_unindexed_fk_product_id_fk ON public.orders_unindexed_fk (product_id);` (as recommended in the Foreign Key Audit section).

    3.  **Review Unused and Duplicate Indexes**:
        * **Importance**: Unused indexes consume storage and add overhead to write operations. Duplicate indexes are redundant and waste resources.
        * **Action**: Analyze the listed unused and duplicate indexes (e.g., `orders_indexed_fk_pkey`, `idx_orders_indexed_fk_product_id`) and drop them if confirmed unnecessary. Always test thoroughly.

    4.  **Investigate Tables with Potentially Missing Indexes**:
        * **Importance**: Tables like `public.sales_data` and `public.orders_indexed_fk` show sequential scans. While not necessarily an error, it suggests queries might benefit from new indexes to reduce I/O.
        * **Action**: Analyze the queries hitting these tables (e.g., using `pg_stat_statements` once enabled) and create appropriate indexes on frequently queried columns.

    5.  **Address Missing Comments Files**:
        * **Importance**: These files provide crucial context, tips, and recommendations within the report, making it more comprehensive and user-friendly.
        * **Action**: Create the missing files: `indexes.txt`, `tables.txt`, `users.txt`, `security.txt`, `connections.txt`, `recommendations.txt`, `ha.txt`.

    6.  **Complete Missing Modules**:
        * **Importance**: Several sections are currently incomplete, limiting the scope of the health check.
        * **Action**: Implement `datadog_setup.py`, `monitoring_metrics.py`, `monitoring_recommendations.py`, `high_availability.py`, `connection_pooling.py`, `rds_upgrade.py`, `check_aws_reg.py`, `get_osinfo.py`, `pgset.py`, and `systemwide_extensions.py`.
    """
    structured_data["ai_analysis"]["recommendations"] = ai_recommendations # Store the AI's raw response

    # --- Step 3: Integrate AI Response into AsciiDoc Content ---
    adoc_content.append("\n=== AI-Generated Recommendations\n")
    adoc_content.append(ai_recommendations)
    
    adoc_content.append("[TIP]\n====\n"
                       "Review all sections of this report for specific findings and recommendations. "
                       "Prioritize issues that directly impact your application's performance, stability, or security, "
                       "such as high CPU usage, long-running queries, or unindexed foreign keys. "
                       "Always test recommendations in a non-production environment before applying them to your main database.\n"
                       "====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\n"
                           "For AWS RDS Aurora, many recommendations involve adjusting parameters in the DB cluster parameter group, "
                           "optimizing queries, or scaling instance types. "
                           "Leverage AWS CloudWatch and Performance Insights for deeper analysis of metrics and query performance. "
                           "Consider using AWS Database Migration Service (DMS) for major version upgrades or schema changes.\n"
                           "====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data
