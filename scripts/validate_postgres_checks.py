#!/usr/bin/env python3
"""
Validator for pg_healthcheck2 PostgreSQL Check Modules.

This script inspects all Python modules in the `plugins/postgres/checks/`
directory to ensure they conform to the preferred structure and conventions.

The preferred conventions are:
1.  A `get_weight()` function that returns an integer importance score.
2.  A single, primary analysis function starting with the prefix `run_`.
3.  The analysis function must accept `(connector, settings)` as arguments.
4.  The analysis function must return a tuple: `(adoc_content, structured_data)`.
5.  Version-specific or multi-line queries should be located in a helper module
    within `plugins/postgres/utils/qrylib/`.
6.  Query-getter functions imported from `qrylib` must exist in their source files.
7.  Imports from the old `postgresql_version_compatibility` are deprecated.
8.  The `connector.execute_query` method should be called with `return_raw=True`.
"""
import ast
from pathlib import Path
import sys

# --- Configuration ---
C_RED = "\033[91m"
C_GREEN = "\033[92m"
C_YELLOW = "\033[93m"
C_BLUE = "\033[94m"
C_RESET = "\033[0m"

PASS_ICON = "✅"
FAIL_ICON = "❌"
WARN_ICON = "⚠️"

def find_project_root() -> Path:
    """Find the project's root directory from the script's location."""
    # The script is in pg_healthcheck2/scripts/, so the root is two levels up.
    return Path(__file__).resolve().parent.parent

def validate_check_module(
    file_path: Path, project_root: Path, qrylib_ast_cache: dict
) -> list:
    """
    Validates a single check module file against the preferred conventions.
    Returns a list of error/warning messages.
    """
    issues = []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            source_code = f.read()
            tree = ast.parse(source_code)
    except Exception as e:
        issues.append(f"{FAIL_ICON} Could not parse file: {e}")
        return issues

    import_map = _build_import_map(tree)

    # --- Run all validation checks ---
    _check_get_weight(tree, issues)
    run_func_node = _check_run_function(tree, issues)
    _check_imports(tree, issues)

    if run_func_node:
        _check_execute_query_call(run_func_node, issues)
        _check_return_statement(run_func_node, issues)
        _check_for_inline_sql(run_func_node, issues)
        _check_query_getter_calls(
            run_func_node, import_map, project_root, qrylib_ast_cache, issues
        )

    return issues

def _build_import_map(tree: ast.AST) -> dict[str, str]:
    """Scans the AST and builds a map of imported names to their source modules."""
    import_map = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            for alias in node.names:
                name_in_code = alias.asname or alias.name
                import_map[name_in_code] = node.module
    return import_map

def _check_query_getter_calls(
    run_func_node: ast.FunctionDef,
    import_map: dict[str, str],
    project_root: Path,
    qrylib_ast_cache: dict[Path, ast.AST | None],
    issues: list,
):
    """
    Validates that functions used to get queries exist in their imported modules.
    """
    for node in ast.walk(run_func_node):
        if (isinstance(node, ast.Assign) and
            len(node.targets) == 1 and
            isinstance(node.targets[0], ast.Name) and
            node.targets[0].id == 'query' and
            isinstance(node.value, ast.Call) and
            isinstance(node.value.func, ast.Name)):

            getter_func_name = node.value.func.id
            if getter_func_name not in import_map:
                continue

            source_module = import_map[getter_func_name]
            if not source_module.startswith("plugins.postgres.utils.qrylib"):
                continue

            module_path_part = source_module.replace('.', '/') + '.py'
            qrylib_file_path = project_root / module_path_part

            if not qrylib_file_path.exists():
                issues.append(
                    f"{FAIL_ICON} Imported module file not found for function `{getter_func_name}`. "
                    f"Expected at: `{qrylib_file_path}`"
                )
                continue

            if qrylib_file_path not in qrylib_ast_cache:
                try:
                    with open(qrylib_file_path, "r", encoding="utf-8") as f:
                        qrylib_ast_cache[qrylib_file_path] = ast.parse(f.read())
                except Exception as e:
                    issues.append(f"{WARN_ICON} Could not parse qrylib module `{qrylib_file_path}`: {e}")
                    qrylib_ast_cache[qrylib_file_path] = None
            
            qrylib_tree = qrylib_ast_cache.get(qrylib_file_path)
            if not qrylib_tree:
                continue

            func_exists = any(
                isinstance(n, ast.FunctionDef) and n.name == getter_func_name
                for n in ast.iter_child_nodes(qrylib_tree)
            )

            if not func_exists:
                issues.append(
                    f"{FAIL_ICON} Imported function `{getter_func_name}` not found "
                    f"in module `{source_module}`."
                )

def _check_get_weight(tree: ast.AST, issues: list):
    """Checks for a valid get_weight() function."""
    found = False
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "get_weight":
            found = True
            if len(node.args.args) != 0:
                issues.append(f"{FAIL_ICON} `get_weight` should have no arguments.")
            
            returns_int = any(
                isinstance(body_item, ast.Return) and
                isinstance(body_item.value, ast.Constant) and
                isinstance(body_item.value.value, int)
                for body_item in node.body
            )
            if not returns_int:
                 issues.append(f"{WARN_ICON} `get_weight` should return a constant integer.")
    if not found:
        issues.append(f"{FAIL_ICON} `get_weight()` function not found.")

def _check_run_function(tree: ast.AST, issues: list) -> ast.FunctionDef | None:
    """Checks for a single main analysis function starting with `run_`."""
    run_funcs = [
        n for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and n.name.startswith("run_")
    ]

    if not run_funcs:
        issues.append(f"{FAIL_ICON} No main analysis function starting with `run_` was found.")
        return None

    if len(run_funcs) > 1:
        func_names = ", ".join(f"'{f.name}'" for f in run_funcs)
        issues.append(f"{FAIL_ICON} Multiple analysis functions starting with `run_` found: {func_names}. Each module must have only one.")
        return None

    func_node = run_funcs[0]
    args = [arg.arg for arg in func_node.args.args]
    if args != ["connector", "settings"]:
        issues.append(
            f"{FAIL_ICON} `{func_node.name}` must accept `(connector, settings)` as arguments, but has `({', '.join(args)})`."
        )
    return func_node

def _check_imports(tree: ast.AST, issues: list):
    """Checks for deprecated import paths from old modules."""
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            if "postgresql_version_compatibility" in node.module:
                issues.append(
                    f"{FAIL_ICON} Deprecated import from `{node.module}` found. "
                    f"All query logic should be moved to a helper module in `plugins/postgres/utils/qrylib/`."
                )

def _check_for_inline_sql(run_func_node: ast.FunctionDef, issues: list):
    """
    Warns if a multi-line SQL query is defined inside the run function,
    while ignoring the function's docstring.
    """
    # Identify the function's docstring node to ignore it.
    docstring_node = ast.get_docstring(run_func_node, clean=False)

    for node in ast.walk(run_func_node):
        # We are looking for string constants that are NOT the docstring
        if (isinstance(node, ast.Constant) and
            isinstance(node.value, str) and
            node.value != docstring_node):
            
            upper_val = node.value.upper()
            # A much more reliable heuristic for a query: it's multi-line,
            # contains SELECT, and also contains FROM or WHERE.
            is_likely_query = (
                '\n' in upper_val and
                'SELECT' in upper_val and
                ('FROM' in upper_val or 'WHERE' in upper_val)
            )

            if is_likely_query:
                issues.append(
                    f"{WARN_ICON} Potential inline SQL query found in `{run_func_node.name}`. "
                    "Complex or multi-line queries should be moved to a `qrylib` helper module."
                )
                return # Only warn once per function

def _check_execute_query_call(run_func_node: ast.FunctionDef, issues: list):
    """Checks for `connector.execute_query` with `return_raw=True`."""
    found_call = False
    for node in ast.walk(run_func_node):
        if (isinstance(node, ast.Call) and
            isinstance(node.func, ast.Attribute) and
            isinstance(node.func.value, ast.Name) and
            node.func.value.id == "connector" and
            node.func.attr == "execute_query"):
            found_call = True
            has_return_raw = any(
                kw.arg == "return_raw" and isinstance(kw.value, ast.Constant) and kw.value.value is True
                for kw in node.keywords
            )
            if not has_return_raw:
                issues.append(f"{FAIL_ICON} Call to `connector.execute_query` is missing `return_raw=True` keyword argument.")
                return
    if not found_call:
        issues.append(f"{WARN_ICON} No call to `connector.execute_query` found in `{run_func_node.name}`.")

def _check_return_statement(run_func_node: ast.FunctionDef, issues: list):
    """Ensures the run function returns a tuple of two items."""
    for node in ast.walk(run_func_node):
        if isinstance(node, ast.Return):
            if not (isinstance(node.value, ast.Tuple) and len(node.value.elts) == 2):
                issues.append(f"{FAIL_ICON} `{run_func_node.name}` must return a 2-item tuple: `(adoc_content, structured_data)`.")
                return

def main():
    """Main function to run the validation process."""
    project_root = find_project_root()
    checks_dir = project_root / "plugins" / "postgres" / "checks"

    print(f"{C_BLUE}--- Running pg_healthcheck2 Module Validator ---\n"
          f"Target Directory: {checks_dir}{C_RESET}\n")

    if not checks_dir.is_dir():
        print(f"{C_RED}{FAIL_ICON} Directory not found: {checks_dir}{C_RESET}")
        sys.exit(1)

    check_files = sorted(list(checks_dir.glob("*.py")))
    failed_files = 0
    qrylib_ast_cache = {}

    for file_path in check_files:
        if file_path.name == "__init__.py":
            continue

        print(f"🔎 Validating: {C_YELLOW}{file_path.name}{C_RESET}")
        issues = validate_check_module(file_path, project_root, qrylib_ast_cache)

        if not issues:
            print(f"  {C_GREEN}{PASS_ICON} All checks passed!{C_RESET}")
        else:
            failed_files += 1
            for issue in issues:
                color = C_RED if FAIL_ICON in issue else C_YELLOW
                print(f"  {color}{issue}{C_RESET}")
        print("-" * 40)

    total_files = len([f for f in check_files if f.name != "__init__.py"])
    print("\n--- Validation Summary ---")
    if failed_files == 0:
        print(f"{C_GREEN}{PASS_ICON} Success! All {total_files} check modules conform to the standard.{C_RESET}")
    else:
        passed_files = total_files - failed_files
        print(f"{C_YELLOW}{WARN_ICON} Validation complete. "
              f"{passed_files}/{total_files} files passed.{C_RESET}")
        print(f"{C_RED}{FAIL_ICON} Found issues in {failed_files} file(s). Please review the logs above.{C_RESET}")
        sys.exit(1)

if __name__ == "__main__":
    main()
