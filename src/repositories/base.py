"""
Base Repository Pattern implementation.

This module provides the abstract base class for all repositories,
defining the common interface for data access operations.
"""

from abc import ABC, abstractmethod
from typing import Optional, TYPE_CHECKING

from src.core.logging import get_logger

if TYPE_CHECKING:
    from src.core.config import Config

logger = get_logger(__name__)


class BaseRepository(ABC):
    """
    Abstract base class for all repositories.
    
    Repositories abstract data access logic and provide a clean interface
    for accessing external services and data sources.
    """

    def __init__(self, config: Optional["Config"] = None):
        """
        Initialize the repository.
        
        Args:
            config: Optional Config instance. Subclasses should handle
                   configuration loading if None.
        """
        self.config = config
        self.logger = get_logger(self.__class__.__name__)

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the data source."""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close connection to the data source."""
        pass

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
        return False
