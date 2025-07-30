from abc import ABC, abstractmethod
from pathlib import Path

class BasePlugin(ABC):
    """Abstract base class for all database technology plugins."""

    @property
    @abstractmethod
    def technology_name(self):
        """A lowercase, URL-friendly name for the technology (e.g., 'postgres')."""
        pass

    @abstractmethod
    def get_connector(self, settings):
        """Returns an instance of the technology-specific connector."""
        pass

    @abstractmethod
    def get_rules_config(self):
        """Returns the technology-specific analysis rules."""
        pass
    
    @abstractmethod
    def get_report_definition(self, report_config_file=None):
        """Returns the structure of the report, defining which checks to run."""
        pass

    @abstractmethod
    def get_template_path(self) -> Path:
        """Returns the path to this plugin's templates directory."""
        pass

    def get_module_weights(self) -> dict:
        """
        Returns a dictionary of weights for each check module to guide
        the AI prompt's token budgeting.
        """
        return {}

    def get_db_version_from_findings(self, findings: dict) -> str:
        """
        Extracts the database version from a structured findings dictionary.
        Each plugin must implement this to parse its own findings structure.
        """
        return "N/A"

    def get_db_name_from_findings(self, findings: dict) -> str:
        """
        Extracts the database name from a structured findings dictionary.
        Each plugin must implement this to parse its own findings structure.
        """
        return "N/A"
