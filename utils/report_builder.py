import importlib
import inspect
from pathlib import Path
from datetime import datetime

class ReportBuilder:
    """
    Handles the construction of the health check report by processing a
    report definition and executing the necessary actions from the active plugin.
    """
    def __init__(self, connector, settings, active_plugin, report_sections):
        self.connector = connector
        self.settings = settings
        self.active_plugin = active_plugin
        self.report_sections = report_sections
        self.adoc_content = []
        self.all_structured_findings = {}

    def build(self):
        """
        Builds the full report by iterating through sections and actions.
        Returns the final AsciiDoc content and structured data.
        """
        for section in self.report_sections:
            if section.get('title'):
                self.adoc_content.append(f"== {section['title']}")
            for action in section['actions']:
                action_type = action.get('type')
                if action_type == 'module':
                    content = self._run_module(action['module'], action['function'])
                    self.adoc_content.append(content)
                elif action_type in ['header', 'comments']:
                    # Both 'header' and 'comments' are treated as report parts now
                    content = self._read_report_part(action['file'])
                    self.adoc_content.append(content)
        
        return "\n\n".join(self.adoc_content), self.all_structured_findings

    def _run_module(self, module_name, function_name):
        """Executes a check module and captures its output."""
        try:
            module = importlib.import_module(module_name)
            func = getattr(module, function_name)
            adoc_content, structured_data = func(self.connector, self.settings)
            key = module_name.split('.')[-1]
            self.all_structured_findings[key] = structured_data
            return adoc_content
        except Exception as e:
            key = module_name.split('.')[-1]
            error_msg = f"[ERROR]\n====\nModule {module_name}.{function_name} failed: {e}\n====\n"
            self.all_structured_findings[key] = {"status": "error", "error": str(e)}
            return error_msg

    def _read_report_part(self, filename):
        """
        Reads a report part file from the active plugin's
        'templates/report_parts/' directory.
        """
        try:
            # Get the base template path from the active plugin
            template_path = self.active_plugin.get_template_path()
            file_path = template_path / "report_parts" / filename
            
            with open(file_path, 'r') as f:
                content = f.read()

            # Replace placeholders like ${COMPANY_NAME}
            for key, value in self.settings.items():
                content = content.replace(f'${{{key.upper()}}}', str(value))
            
            # Add dynamic values like current date
            content = content.replace('${CURRENT_DATE}', datetime.utcnow().strftime('%Y-%m-%d'))
            
            return content
        except FileNotFoundError:
            return f"[ERROR]\n====\nReport part file '{filename}' not found in plugin's templates/report_parts/ directory.\n====\n"
        except Exception as e:
            return f"[ERROR]\n====\nCould not read report part file '{filename}': {e}\n====\n"
