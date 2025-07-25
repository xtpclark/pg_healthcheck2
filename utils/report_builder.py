import importlib
from pathlib import Path

class ReportBuilder:
    def __init__(self, connector, settings, active_plugin, report_sections):
        self.connector = connector
        self.settings = settings
        self.active_plugin = active_plugin
        self.report_sections = report_sections
        self.adoc_content = []
        self.all_structured_findings = {}

    def build(self):
        for section in self.report_sections:
            if section.get('title'):
                self.adoc_content.append(f"== {section['title']}")
            for action in section['actions']:
                if action['type'] == 'module':
                    content = self._run_module(action['module'], action['function'])
                    self.adoc_content.append(content)
                # --- MODIFIED: Unified handling for report parts ---
                elif action['type'] in ['header', 'comments']:
                    # Headers and comments are both just report parts now
                    content = self._read_report_part(action['file'])
                    self.adoc_content.append(content)
        
        return "\n\n".join(self.adoc_content), self.all_structured_findings

    def _run_module(self, module_name, function_name):
        # ... (this method remains the same)
        pass

    # --- NEW: Consolidated method for reading report part files ---
    def _read_report_part(self, filename):
        """Reads a report part file from the active plugin's template directory."""
        try:
            # Use the method from the plugin to get the correct base path
            template_path = self.active_plugin.get_template_path()
            file_path = template_path / "report_parts" / filename
            
            with open(file_path, 'r') as f:
                content = f.read()
            # Replace placeholders
            for key, value in self.settings.items():
                content = content.replace(f'${{{key.upper()}}}', str(value))
            return content
        except FileNotFoundError:
            return f"[ERROR]\n====\nReport part file '{filename}' not found in plugin's templates/report_parts/ directory.\n====\n"
