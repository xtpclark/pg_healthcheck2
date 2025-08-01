from plugins.postgres.utils.qrylib.pg_stat_statements import get_pg_stat_statements_query
from plugins.postgres.utils.qrylib.function_audit import (
    get_security_definer_functions_query,
    get_superuser_owned_functions_query,
    get_function_volatility_query
)

def get_weight():
    """Returns the importance score for this module."""
    return 5

def run_function_audit(connector, settings):
    """
    Audits database functions for security risks (SECURITY DEFINER, superuser ownership),
    performance anti-patterns (volatile functions), and execution statistics.
    """
    adoc_content = ["=== Function and Stored Procedure Audit", "Audits functions for security, performance, and usage patterns.\n"]
    structured_data = {}
    params = {'limit': settings.get('row_limit', 10)}

    # --- Security Checks ---
    try:
        adoc_content.append("==== Security Analysis")
        
        # Check for SECURITY DEFINER functions
        sec_def_query = get_security_definer_functions_query(connector)
        sec_def_formatted, sec_def_raw = connector.execute_query(sec_def_query, params=params, return_raw=True)

        if sec_def_raw:
            adoc_content.append("[WARNING]\n====\n**SECURITY DEFINER Functions Found:** These functions execute with the privileges of their owner. Review them carefully to prevent potential privilege escalation vulnerabilities.\n====\n")
            adoc_content.append(sec_def_formatted)
        else:
            adoc_content.append("[NOTE]\n====\nNo `SECURITY DEFINER` functions found. This is a good security practice.\n====\n")
        structured_data["security_definer_functions"] = {"status": "success", "data": sec_def_raw}

        # Check for superuser-owned functions
        su_owned_query = get_superuser_owned_functions_query(connector)
        su_owned_formatted, su_owned_raw = connector.execute_query(su_owned_query, params=params, return_raw=True)

        if su_owned_raw:
            adoc_content.append("\n[CAUTION]\n====\n**Superuser-Owned Functions Found:** Functions owned by superusers can be a security risk if not properly secured. Consider reassigning ownership to less-privileged roles where appropriate.\n====\n")
            adoc_content.append(su_owned_formatted)
        structured_data["superuser_owned_functions"] = {"status": "success", "data": su_owned_raw}

    except Exception as e:
        adoc_content.append(f"\n[ERROR]\n====\nCould not perform function security audit: {e}\n====\n")

    # --- Volatility Check ---
    try:
        adoc_content.append("\n==== Performance: Volatility Analysis")
        volatility_query = get_function_volatility_query(connector)
        volatility_formatted, volatility_raw = connector.execute_query(volatility_query, params=params, return_raw=True)

        if volatility_raw:
            adoc_content.append("[IMPORTANT]\n====\n**Volatile Functions Found:** Functions marked as `VOLATILE` can inhibit query parallelization and other optimizations. Review these functions to see if they can be safely changed to `STABLE` or `IMMUTABLE`.\n====\n")
            adoc_content.append(volatility_formatted)
        else:
            adoc_content.append("[NOTE]\n====\nNo volatile functions found in user schemas. This is a good sign for query optimization.\n====\n")
        structured_data["volatile_functions"] = {"status": "success", "data": volatility_raw}

    except Exception as e:
        adoc_content.append(f"\n[ERROR]\n====\nCould not analyze function volatility: {e}\n====\n")

    # --- pg_stat_statements Performance Check ---
    try:
        adoc_content.append("\n==== Performance: Execution Statistics")
        if connector.has_pgstat:
            # The get_pg_stat_statements_query already handles version differences (PG14+ vs older)
            stats_query = get_pg_stat_statements_query(connector, 'function_performance') + " LIMIT %(limit)s;"
            stats_formatted, stats_raw = connector.execute_query(stats_query, params=params, return_raw=True)
            
            if not connector.version_info.get('is_pg14_or_newer'):
                 adoc_content.append("[NOTE]\n====\nShowing top functions by total execution time based on `pg_stat_statements`.\n====\n")
            else:
                 adoc_content.append("[NOTE]\n====\nFor PostgreSQL 14+, `pg_stat_statements` does not directly track function calls. The list below shows the top statements by execution time, which may include function calls.\n====\n")

            adoc_content.append(stats_formatted)
            structured_data["function_performance_stats"] = {"status": "success", "data": stats_raw}
        else:
            adoc_content.append("[NOTE]\n====\n`pg_stat_statements` is not enabled. No function performance data is available.\n====\n")
            structured_data["function_performance_stats"] = {"status": "not_applicable", "reason": "pg_stat_statements not enabled."}
    except Exception as e:
        adoc_content.append(f"\n[ERROR]\n====\nCould not analyze function performance statistics: {e}\n====\n")

    return "\n".join(adoc_content), structured_data
