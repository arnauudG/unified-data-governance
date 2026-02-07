"""
Service Factory - Factory for creating service layer instances.

This factory provides a centralized way to create services with proper
dependency injection and configuration.
"""

from typing import Optional, TYPE_CHECKING

from src.core.config import get_config, Config
from src.services.quality_service import QualityService
from src.services.metadata_service import MetadataService
from src.services.pipeline_service import PipelineService
from src.factories.client_factory import ClientFactory
from src.core.logging import get_logger

logger = get_logger(__name__)


class ServiceFactory:
    """
    Factory for creating service layer instances.
    
    This factory implements the Factory Pattern and Singleton Pattern
    to provide centralized creation and caching of service instances
    with proper dependency injection.
    """

    def __init__(
        self, 
        config: Optional[Config] = None, 
        client_factory: Optional[ClientFactory] = None
    ):
        """
        Initialize Service Factory.

        Args:
            config: Optional Config instance. If None, uses get_config().
            client_factory: Optional ClientFactory instance. If None, creates new one.
        """
        self.config = config or get_config()
        self.client_factory = client_factory or ClientFactory(config=self.config)
        self._quality_service: Optional[QualityService] = None
        self._metadata_service: Optional[MetadataService] = None
        self._pipeline_service: Optional[PipelineService] = None

    def get_quality_service(self) -> QualityService:
        """
        Get or create Quality Service (singleton).

        Returns:
            QualityService instance
        """
        if self._quality_service is None:
            soda_repo = self.client_factory.get_soda_repository()
            self._quality_service = QualityService(
                soda_repository=soda_repo, config=self.config
            )
            logger.debug("Created QualityService instance")
        return self._quality_service

    def get_metadata_service(self) -> MetadataService:
        """
        Get or create Metadata Service (singleton).

        Returns:
            MetadataService instance
        """
        if self._metadata_service is None:
            collibra_repo = self.client_factory.get_collibra_repository()
            self._metadata_service = MetadataService(
                collibra_repository=collibra_repo, config=self.config
            )
            logger.debug("Created MetadataService instance")
        return self._metadata_service

    def get_pipeline_service(self) -> PipelineService:
        """
        Get or create Pipeline Service (singleton).

        Returns:
            PipelineService instance
        """
        if self._pipeline_service is None:
            quality_service = self.get_quality_service()
            metadata_service = self.get_metadata_service()
            self._pipeline_service = PipelineService(
                quality_service=quality_service,
                metadata_service=metadata_service,
                config=self.config,
            )
            logger.debug("Created PipelineService instance")
        return self._pipeline_service

    def reset(self) -> None:
        """Reset all cached services (useful for testing)."""
        self._quality_service = None
        self._metadata_service = None
        self._pipeline_service = None
        logger.debug("Reset all service instances")
