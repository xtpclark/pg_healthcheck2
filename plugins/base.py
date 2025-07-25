from abc import ABC, abstractmethod

class BasePlugin(ABC):
    """The interface that every technology plugin must implement."""

    @property
    @abstractmethod
    def technology_name(self):
        """A string name for the technology, e.g., 'postgres'."""
        pass

    @abstractmethod
    def get_connector(self, settings):
        """Return an instance of the technology's connector class."""
        pass

    @abstractmethod
    def get_rules_config(self):
        """Return the analysis rules configuration dictionary."""
        pass

    @abstractmethod
    def get_report_definition(self):
        """Return the list of sections for the report blueprint."""
        pass

    @abstractmethod
    def get_report_definition(self, report_config_file=None):
        """
        Return the list of sections for the report.
        Can optionally load from a specific file path.
        """
        pass

    @abstractmethod
    def get_template_path(self) -> Path:
        """Returns the path to the plugin's templates directory."""
        pass
