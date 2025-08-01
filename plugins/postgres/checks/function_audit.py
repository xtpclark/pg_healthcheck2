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
    Audits database functions for security risks (SECURITY DEFINER, superuser ownership)
    and performance anti-patterns (volatile functions).
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
            adoc_content.append("[WARNING]\n====\n**SECURITY DEFINER Functions Found:** These functions execute with the privileges of their *owner*, not the user calling them. While powerful, this can create security risks like privilege escalation if the functions are not written securely.\n====\n")
            adoc_content.append(sec_def_formatted)
            adoc_content.append("\n[TIP]\n====\n*Guidance on SECURITY DEFINER Functions:*\n\n"
                                "* **Legitimate Use Case**: To grant a low-privilege user temporary, controlled access to specific tables. For example, a function owned by an admin could allow a user to insert records into a protected logging table without giving them direct table access.\n"
                                "* **Primary Risk**: A poorly written function can be exploited. For example, if a function builds and executes a query using unsanitized input, an attacker could run arbitrary code with the owner's higher privileges.\n"
                                "* **Best Practice**: Always set a secure `search_path` at the beginning of `SECURITY DEFINER` functions (e.g., `SET search_path = pg_catalog;`) to prevent hijacking.\n"
                                "* **Note on Extensions**: Many extensions, especially in managed environments like AWS Aurora, use `SECURITY DEFINER` functions to work correctly. Functions belonging to trusted extensions are generally safe, but should be understood.\n"
                                "====\n")
        else:
            adoc_content.append("[NOTE]\n====\nNo `SECURITY DEFINER` functions found. This is a good security practice.\n====\n")
        structured_data["security_definer_functions"] = {"status": "success", "data": sec_def_raw}

        # Check for superuser-owned functions
        su_owned_query = get_superuser_owned_functions_query(connector)
        su_owned_formatted, su_owned_raw = connector.execute_query(su_owned_query, params=params, return_raw=True)

        if su_owned_raw:
            # MODIFIED: Expanded the [CAUTION] block for clarity.
            adoc_content.append("\n[CAUTION]\n====\n**Superuser-Owned Functions Found:** Functions owned by a superuser can create unintended security holes by violating the principle of least privilege. This risk is magnified if the function is also `SECURITY DEFINER`.\n====\n")
            adoc_content.append(su_owned_formatted)
            # MODIFIED: Added a [TIP] block explaining the risks and fixes.
            adoc_content.append("\n[TIP]\n====\n*Guidance on Superuser-Owned Functions:*\n\n"
                                "* **The Main Risk**: A `SECURITY DEFINER` function owned by a superuser is extremely dangerous. It allows any user with `EXECUTE` permissions on the function to perform actions as a superuser.\n"
                                "* **Accidental Ownership**: This often happens when a DBA creates a utility function while logged in with their superuser account. The ownership should be reassigned to a less-privileged role.\n"
                                "* **How to Fix**: Change the ownership using the command: `ALTER FUNCTION function_name(arg_types) OWNER TO new_role_name;`\n"
                                "====\n")
        structured_data["superuser_owned_functions"] = {"status": "success", "data": su_owned_raw}

    except Exception as e:
        adoc_content.append(f"\n[ERROR]\n====\nCould not perform function security audit: {e}\n====\n")

    # --- Volatility Check ---
    try:
        adoc_content.append("\n==== Performance: Volatility Analysis")
        volatility_query = get_function_volatility_query(connector)
        volatility_formatted, volatility_raw = connector.execute_query(volatility_query, params=params, return_raw=True)

        if volatility_raw:
            adoc_content.append("[IMPORTANT]\n====\n**Volatile Functions Found:** Functions marked as `VOLATILE` are treated as 'black boxes' by the query planner. This is the most restrictive level and prevents key performance optimizations like query parallelization and the use of expression indexes. Review the functions below to see if a more specific volatility level can be safely applied.\n====\n")
            adoc_content.append(volatility_formatted)
            adoc_content.append("\n[TIP]\n====\n*Choose the correct volatility level to improve performance:*\n\n"
                                "* **`IMMUTABLE`**: Use for functions whose result depends *only* on their input arguments (e.g., `abs(x)`). The result is constant forever for the same inputs.\n"
                                "* **`STABLE`**: Use for functions that do not modify the database and whose results are consistent *within a single query scan* (e.g., a function that only performs `SELECT`s).\n"
                                "* **`VOLATILE`**: Use only when a function has side effects (e.g., `INSERT`, `UPDATE`, `DELETE`) or its value can change at any time (e.g., `random()`, `now()`). This is the default if not specified.\n"
                                "====\n")
        else:
            adoc_content.append("[NOTE]\n====\nNo volatile functions found in user schemas. This is a good sign for query optimization.\n====\n")
        structured_data["volatile_functions"] = {"status": "success", "data": volatility_raw}

    except Exception as e:
        adoc_content.append(f"\n[ERROR]\n====\nCould not analyze function volatility: {e}\n====\n")

    return "\n".join(adoc_content), structured_data
