from abc import ABC, abstractmethod
from typing import Dict, Any, TypeVar, Generic, Optional
from typing_extensions import TypedDict

# Generic type variables for configuration and resource data
ConfigT = TypeVar("ConfigT", bound=Dict[str, Any])
ResourceT = TypeVar("ResourceT", bound=Dict[str, Any])


class DEBenchFixture(ABC, Generic[ConfigT, ResourceT]):
    """
    Abstract base class for all DE-Bench fixtures.

    This ensures a consistent interface across all resource types:
    - setup_resource: Creates and initializes the resource
    - teardown_resource: Cleans up and destroys the resource
    - get_resource_type: Returns a string identifier for the resource type

    Fixtures can optionally be initialized with custom configuration.
    """

    def __init__(self, custom_config: Optional[ConfigT] = None):
        """
        Initialize the fixture with optional custom configuration.

        Args:
            custom_config: Optional configuration to use instead of default config.
                          If provided, this will be used by setup_resource().
        """
        self.custom_config = custom_config

    @abstractmethod
    def setup_resource(self, resource_config: Optional[ConfigT] = None) -> ResourceT:
        """
        Set up and initialize the resource based on configuration.

        Args:
            resource_config: Configuration dictionary specific to this resource type.
                           If None, will use custom_config from __init__ or get_default_config().

        Returns:
            Resource data dictionary containing metadata and connection info
        """
        pass

    @abstractmethod
    def teardown_resource(self, resource_data: ResourceT) -> None:
        """
        Clean up and destroy the resource.

        Args:
            resource_data: Resource data returned from setup_resource
        """
        pass

    @classmethod
    @abstractmethod
    def get_resource_type(cls) -> str:
        """
        Return the resource type identifier (e.g., 'mongo_resource', 'postgres_resource').

        Returns:
            String identifier for this resource type
        """
        pass

    @classmethod
    @abstractmethod
    def get_default_config(cls) -> ConfigT:
        """
        Return a default configuration for this resource type.
        Useful for tests that need minimal setup.

        Returns:
            Default configuration dictionary
        """
        pass
