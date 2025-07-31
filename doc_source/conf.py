# -- Path setup --------------------------------------------------------------
import os
import sys
sys.path.insert(0, os.path.abspath('..'))

# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'DB Heath Check Tool'
copyright = '2025, Perry Clark'
author = 'Perry Clark'
release = '2.1.0'


# -- General configuration ---------------------------------------------------
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.doctest',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
    'myst_parser',
    'sphinx_asciidoc',
]

# Use a simple list for robust file discovery
source_suffix = ['.rst', '.md', '.adoc']

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

#html_theme = 'alabaster'
#html_static_path = ['_static']
html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']


# ========= DYNAMIC RULE DOCUMENTATION SCRIPT (SIMPLIFIED) =========
import os
import json
import glob
from collections import defaultdict

def generate_rule_docs():
    """
    Finds all `rules/*.json` files, parses them, and generates RST content.
    This function is called directly when conf.py is executed.
    """
    # Define paths relative to this conf.py file
    doc_source_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(doc_source_dir, '..'))
    output_path = os.path.join(doc_source_dir, 'api', 'generated_rules.rst')
    
    rules_glob_pattern = os.path.join(project_root, 'plugins', '*', 'rules', '*.json')

    rules_by_plugin = defaultdict(list)
    for json_file in sorted(glob.glob(rules_glob_pattern)):
        plugin_name = json_file.split(os.sep)[-3] # A simpler way to get the plugin name
        rules_by_plugin[plugin_name].append(json_file)

    # Build the RST content string
    rst_content = []
    rst_content.append("Plugin Rule Documentation")
    rst_content.append("=================================\n")
    rst_content.append("This page is automatically generated from the JSON rule files for all plugins.\n")

    for plugin_name, json_files in rules_by_plugin.items():
        plugin_title = f"{plugin_name.capitalize()} Plugin Rules"
        rst_content.append(plugin_title)
        rst_content.append("~" * len(plugin_title) + "\n")

        for json_file in json_files:
            try:
                with open(json_file, 'r') as f:
                    data = json.load(f)

                rule_key = list(data.keys())[0]
                rule_data = data[rule_key]
                
                rst_content.append(f"{rule_key}")
                rst_content.append("-" * len(rule_key) + "\n")

                for i, rule in enumerate(rule_data.get('rules', [])):
                    rst_content.append(f"**Rule #{i+1}**\n")
                    rst_content.append(f"   :Level: ``{rule.get('level', 'N/A')}``")
                    rst_content.append(f"   :Score: {rule.get('score', 'N/A')}")
                    rst_content.append(f"   :Expression: ``{rule.get('expression', 'N/A')}``\n")
                    rst_content.append(f"**Reasoning:** {rule.get('reasoning', '')}\n")
                    rst_content.append("**Recommendations:**")
                    rst_content.append("")
                    for rec in rule.get('recommendations', []):
                        rst_content.append(f"* {rec}")
                    rst_content.append("\n---\n")

            except Exception as e:
                rst_content.append(f"Error processing {os.path.basename(json_file)}: {e}\n")

    # Write the content to the output file
    with open(output_path, 'w') as f:
        f.write("\n".join(rst_content))


def generate_template_docs():
    """
    Finds all `templates/prompts/*.j2` files for all plugins and generates
    an RST file that includes their source code.
    """
    # Define paths
    doc_source_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(doc_source_dir, '..'))
    output_path = os.path.join(doc_source_dir, 'api', 'generated_templates.rst')
    
    # Generic glob pattern to find all template files
    templates_glob_pattern = os.path.join(project_root, 'plugins', '*', 'templates', 'prompts', '*.j2')

    # Group found files by their plugin name
    templates_by_plugin = defaultdict(list)
    for template_file in sorted(glob.glob(templates_glob_pattern)):
        plugin_name = template_file.split(os.sep)[-4] # Gets the plugin directory name
        templates_by_plugin[plugin_name].append(template_file)

    # Build the RST content string
    rst_content = []
    rst_content.append("Plugin Template Documentation")
    rst_content.append("=====================================\n")
    rst_content.append("This page displays the source code of prompt templates for all plugins.\n")

    for plugin_name, template_files in templates_by_plugin.items():
        plugin_title = f"{plugin_name.capitalize()} Plugin Templates"
        rst_content.append(plugin_title)
        rst_content.append("~" * len(plugin_title) + "\n")

        for template_file in template_files:
            try:
                filename = os.path.basename(template_file)
                relative_path = os.path.relpath(template_file, os.path.dirname(output_path))
                
                # Add a subsection for this specific template file
                rst_content.append(filename)
                rst_content.append("-" * len(filename) + "\n")
                
                # Use literalinclude to display the file's source code
                rst_content.append(f".. literalinclude:: {relative_path}")
                rst_content.append("   :language: jinja\n")

            except Exception as e:
                rst_content.append(f"Error processing {os.path.basename(template_file)}: {e}\n")

    # Write the content to the output file
    with open(output_path, 'w') as f:
        f.write("\n".join(rst_content))


# --- Run the functions immediately ---
generate_rule_docs()
generate_template_docs()
