"""
Pipeline Service - Business logic for pipeline orchestration.

This service orchestrates end-to-end pipeline operations,
coordinating between quality checks, metadata sync, and data export.
"""

from typing import Dict, Any, Optional, List, TYPE_CHECKING
from pathlib import Path

from src.core.logging import get_logger
from src.core.config import get_config, Config
from src.services.quality_service import QualityService
from src.services.metadata_service import MetadataService

logger = get_logger(__name__)


class PipelineService:
    """
    Service for orchestrating pipeline operations.
    
    This service coordinates between quality checks, metadata synchronization,
    and data export operations, following the Service Layer pattern.
    """

    def __init__(
        self,
        quality_service: Optional[QualityService] = None,
        metadata_service: Optional[MetadataService] = None,
        config: Optional[Config] = None,
    ):
        """
        Initialize Pipeline Service.

        Args:
            quality_service: Optional QualityService instance
            metadata_service: Optional MetadataService instance
            config: Optional Config instance
        """
        self.config = config or get_config()
        self.quality_service = quality_service or QualityService(config=self.config)
        self.metadata_service = metadata_service or MetadataService(config=self.config)

    def run_quality_checks(self, layer: str) -> Dict[str, Any]:
        """
        Run quality checks for a layer.

        Args:
            layer: Layer name ('raw', 'staging', 'mart', 'quality')

        Returns:
            Dictionary containing quality check results
        """
        logger.info(f"Running quality checks for {layer} layer")

        # Validate quality
        quality_passed = self.quality_service.validate_quality_before_sync(layer)

        return {
            "layer": layer,
            "quality_passed": quality_passed,
            "status": "passed" if quality_passed else "failed",
        }

    def sync_metadata_with_quality_gate(
        self, layer: str, strict: bool = False
    ) -> Dict[str, Any]:
        """
        Sync metadata for a layer with quality gate.

        Args:
            layer: Layer name ('raw', 'staging', 'mart', 'quality')
            strict: If True, fails if quality gate fails. If False, continues anyway.

        Returns:
            Dictionary containing sync result
        """
        logger.info(f"Syncing metadata for {layer} layer (strict={strict})")

        # Run quality checks
        quality_result = self.run_quality_checks(layer)

        # Check quality gate
        if strict and not quality_result["quality_passed"]:
            logger.error(
                f"Quality gate failed for {layer} layer. Skipping metadata sync."
            )
            return {
                "status": "skipped",
                "layer": layer,
                "reason": "Quality gate failed",
                "quality_result": quality_result,
            }

        # Sync metadata
        sync_result = self.metadata_service.sync_layer_metadata(layer)

        return {
            "status": "success",
            "layer": layer,
            "quality_result": quality_result,
            "sync_result": sync_result,
        }

    def run_complete_pipeline(
        self, layers: Optional[List[str]] = None, strict: bool = False
    ) -> Dict[str, Any]:
        """
        Run complete pipeline: quality checks → metadata sync → export.

        Args:
            layers: Optional list of layers to process. If None, processes all layers.
            strict: If True, fails pipeline if quality gate fails.

        Returns:
            Dictionary containing pipeline results
        """
        if layers is None:
            layers = ["raw", "staging", "mart", "quality"]

        logger.info(f"Starting complete pipeline for layers: {layers}")

        results = {}
        for layer in layers:
            try:
                results[layer] = self.sync_metadata_with_quality_gate(
                    layer, strict=strict
                )
            except Exception as e:
                logger.error(f"Pipeline failed for {layer} layer: {e}", exc_info=True)
                results[layer] = {
                    "status": "error",
                    "layer": layer,
                    "error": str(e),
                }

        # Determine overall status
        all_passed = all(
            r.get("status") in ["success", "skipped"] for r in results.values()
        )

        return {
            "status": "success" if all_passed else "partial_failure",
            "layers": results,
        }
