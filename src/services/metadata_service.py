"""
Metadata Service - Business logic for metadata synchronization.

This service orchestrates metadata synchronization operations,
coordinating between Collibra API and configuration management.
"""

import yaml
from typing import List, Dict, Any, Optional, TYPE_CHECKING
from pathlib import Path

from src.core.logging import get_logger
from src.core.config import get_config, Config
from src.core.exceptions import ConfigurationError
from src.repositories.collibra_repository import CollibraRepository

logger = get_logger(__name__)


class MetadataService:
    """
    Service for managing metadata synchronization operations.
    
    This service orchestrates metadata synchronization operations,
    coordinating between Collibra API and configuration management.
    """

    def __init__(
        self,
        collibra_repository: Optional[CollibraRepository] = None,
        config: Optional[Config] = None,
    ):
        """
        Initialize Metadata Service.

        Args:
            collibra_repository: Optional CollibraRepository instance
            config: Optional Config instance
        """
        self.config = config or get_config()
        self.collibra_repository = (
            collibra_repository or CollibraRepository(config=self.config)
        )
        self.config_path = self.config.paths.collibra_config_path

    def load_collibra_config(self) -> Dict[str, Any]:
        """
        Load Collibra configuration from config.yml.

        Returns:
            Dictionary containing Collibra configuration

        Raises:
            ConfigurationError: If config file not found or invalid
        """
        if not self.config_path.exists():
            raise ConfigurationError(
                f"Collibra config file not found at {self.config_path}",
                details={
                    "config_path": str(self.config_path),
                    "message": "Please create collibra/config.yml with your database and schema IDs.",
                },
            )

        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)

            if not config:
                raise ConfigurationError(
                    "Collibra config file is empty",
                    details={"config_path": str(self.config_path)},
                )

            return config
        except yaml.YAMLError as e:
            raise ConfigurationError(
                f"Invalid YAML in config file: {e}",
                details={"config_path": str(self.config_path)},
                cause=e,
            )
        except Exception as e:
            raise ConfigurationError(
                f"Error loading config file: {e}",
                details={"config_path": str(self.config_path)},
                cause=e,
            )

    def sync_layer_metadata(
        self, layer: str, database_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Sync metadata for a specific layer.

        Args:
            layer: Layer name ('raw', 'staging', 'mart', 'quality')
            database_id: Optional database ID (if None, loads from config)

        Returns:
            Dictionary containing sync result
        """
        config = self.load_collibra_config()

        if database_id is None:
            database_id = config.get("database_id")
            if not database_id:
                raise ConfigurationError(
                    "database_id not found in Collibra config",
                    details={"config_path": str(self.config_path)},
                )

        # Get schema asset IDs for the layer
        layer_config = config.get(layer.lower(), {})
        schema_asset_ids = layer_config.get("schema_connection_ids", [])

        if not schema_asset_ids:
            logger.warning(
                f"No schema asset IDs configured for {layer} layer. Skipping sync."
            )
            return {
                "status": "skipped",
                "layer": layer,
                "reason": "No schema asset IDs configured",
            }

        logger.info(f"Starting metadata sync for {layer} layer")

        # Resolve schema asset IDs to connection IDs
        schema_connection_ids = self.collibra_repository.resolve_schema_connection_ids(
            database_id, schema_asset_ids
        )

        # Trigger sync
        result = self.collibra_repository.trigger_metadata_sync(
            database_id, schema_connection_ids
        )

        logger.info(f"Metadata sync completed for {layer} layer")
        return {
            "status": "success",
            "layer": layer,
            "result": result,
        }

    def sync_all_layers(self, layers: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Sync metadata for all configured layers.

        Args:
            layers: Optional list of layers to sync. If None, syncs all configured layers.

        Returns:
            Dictionary containing sync results for each layer
        """
        if layers is None:
            layers = ["raw", "staging", "mart", "quality"]

        results = {}
        for layer in layers:
            try:
                results[layer] = self.sync_layer_metadata(layer)
            except Exception as e:
                logger.error(f"Failed to sync {layer} layer: {e}", exc_info=True)
                results[layer] = {
                    "status": "error",
                    "layer": layer,
                    "error": str(e),
                }

        return results
