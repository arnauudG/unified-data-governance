"""
Client Factory - Factory for creating API clients and repositories.

This factory provides a centralized way to create repositories and clients
with proper configuration and dependency injection.
"""

from typing import Optional, TYPE_CHECKING

from src.core.config import get_config, Config
from src.repositories.soda_repository import SodaRepository
from src.repositories.collibra_repository import CollibraRepository
from src.core.logging import get_logger

logger = get_logger(__name__)


class ClientFactory:
    """
    Factory for creating API clients and repositories.
    
    This factory implements the Factory Pattern and Singleton Pattern
    to provide centralized creation and caching of repository instances.
    """

    def __init__(self, config: Optional[Config] = None):
        """
        Initialize Client Factory.

        Args:
            config: Optional Config instance. If None, uses get_config().
        """
        self.config = config or get_config()
        self._soda_repository: Optional[SodaRepository] = None
        self._collibra_repository: Optional[CollibraRepository] = None

    def get_soda_repository(self) -> SodaRepository:
        """
        Get or create Soda Cloud repository (singleton).

        Returns:
            SodaRepository instance
        """
        if self._soda_repository is None:
            self._soda_repository = SodaRepository(config=self.config)
            logger.debug("Created SodaRepository instance")
        return self._soda_repository

    def get_collibra_repository(self) -> CollibraRepository:
        """
        Get or create Collibra repository (singleton).

        Returns:
            CollibraRepository instance
        """
        if self._collibra_repository is None:
            self._collibra_repository = CollibraRepository(config=self.config)
            logger.debug("Created CollibraRepository instance")
        return self._collibra_repository

    def reset(self) -> None:
        """Reset all cached repositories (useful for testing)."""
        self._soda_repository = None
        self._collibra_repository = None
        logger.debug("Reset all repository instances")
