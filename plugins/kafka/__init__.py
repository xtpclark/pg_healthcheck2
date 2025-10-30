import importlib.util
from pathlib import Path
import json
from plugins.base import BasePlugin
from .connector import KafkaConnector

class KafkaPlugin(BasePlugin):
    """The kafka implementation of the plugin interface."""

    @property
    def technology_name(self):
        return "kafka"

    def get_connector(self, settings):
        """Returns an instance of the kafka connector."""
        return KafkaConnector(settings)

    def get_rules_config(self):
        """
        Dynamically discovers and loads all .json rule files
        from the 'rules' directory.
        
        Returns:
            dict: All rules merged into a single dictionary
        """
        all_rules = {}
        rules_dir = Path(__file__).parent / 'rules'

        if not rules_dir.is_dir():
            print(f"⚠️ Warning: Rules directory not found at {rules_dir}")
            return {}

        for rule_file in rules_dir.glob('*.json'):
            try:
                with open(rule_file, 'r') as f:
                    loaded_rules = json.load(f)
                    all_rules.update(loaded_rules)
            except json.JSONDecodeError as e:
                print(f"⚠️ Warning: Could not parse rule file {rule_file.name}. Error: {e}")
            except IOError as e:
                print(f"⚠️ Warning: Could not read rule file {rule_file.name}. Error: {e}")

        return all_rules

    def get_report_definition(self, report_config_file=None):
        """
        Dynamically loads a report definition from a Python file.
        Falls back to reports/default.py if not specified.
        
        Args:
            report_config_file: Optional path to custom report file
            
        Returns:
            list: REPORT_SECTIONS list from the report module
        """
        if report_config_file:
            config_path = Path(report_config_file)
        else:
            config_path = Path(__file__).parent / "reports" / "default.py"

        if not config_path.is_file():
            raise FileNotFoundError(f"Report configuration file not found: {config_path}")

        # Dynamically import the report module
        spec = importlib.util.spec_from_file_location("report_config_module", config_path)
        report_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(report_module)
        
        return getattr(report_module, 'REPORT_SECTIONS')

    def get_template_path(self) -> Path:
        """Returns the path to this plugin's templates directory."""
        return Path(__file__).parent / "templates"

    # Optional: Override these methods for enhanced functionality
    def get_module_weights(self) -> dict:
        """
        Dynamically discovers check weights by importing each check module.
        Override this if you want custom weight calculation logic.
        """
        weights = {}
        try:
            report_sections = self.get_report_definition()
            for section in report_sections:
                module_name = section.get('module')
                if not module_name:
                    continue
                
                try:
                    # Extract just the module name from full path
                    if 'plugins.kafka.checks.' in module_name:
                        short_name = module_name.split('.')[-1]
                        spec = importlib.util.find_spec(f"plugins.kafka.checks.{short_name}")
                        if spec:
                            check_module = spec.loader.load_module()
                            if hasattr(check_module, 'get_weight'):
                                weights[short_name] = check_module.get_weight()
                except Exception as e:
                    print(f"⚠️ Could not load weight for '{module_name}': {e}")
        except Exception:
            pass  # Return empty dict if report loading fails
        
        return weights

    def get_db_version_from_findings(self, findings: dict) -> str:
        """
        Extracts the Kafka version from the findings.
        Looks in kafka_overview check results.
        """
        try:
            # Kafka-specific path to version info
            version_info = findings.get("kafka_overview", {}).get("version_info", {})
            if version_info.get("status") == "success":
                data = version_info.get("data", [{}])
                if data and len(data) > 0:
                    return data[0].get("version", "N/A")
            return "N/A"
        except (IndexError, AttributeError, KeyError):
            return "N/A"
    
    
    def get_db_name_from_findings(self, findings: dict) -> str:
        """
        Extracts the Kafka cluster ID from findings.
        For Kafka, cluster_id serves as the "database name".
        """
        try:
            cluster_info = findings.get("kafka_overview", {}).get("cluster_metadata", {})
            if cluster_info.get("status") == "success":
                data = cluster_info.get("data", [{}])
                if data and len(data) > 0:
                    return data[0].get("cluster_id", "N/A")
            return "N/A"
        except (IndexError, AttributeError, KeyError):
            return "N/A"

    def old_get_db_version_from_findings(self, findings: dict) -> str:
        """
        Extracts database version from findings structure.
        Override this to match your specific findings structure.
        
        Args:
            findings: The structured_findings dictionary
            
        Returns:
            str: Database version or "N/A"
        """
        # TODO: Implement based on your findings structure
        # Example patterns:
        # return findings.get("system_info", {}).get("version", "N/A")
        # return findings.get("kafka_overview", {}).get("version", {}).get("data", [{}])[0].get("version", "N/A")
        return "N/A"

    def old_get_db_name_from_findings(self, findings: dict) -> str:
        """
        Extracts database name from findings structure.
        Override this to match your specific findings structure.
        
        Args:
            findings: The structured_findings dictionary
            
        Returns:
            str: Database name or "N/A"
        """
        # TODO: Implement based on your findings structure
        return "N/A"
