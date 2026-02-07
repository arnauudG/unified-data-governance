"""
Unit tests for PipelineService.
"""

import pytest
from unittest.mock import Mock, MagicMock
from pathlib import Path

from src.services.pipeline_service import PipelineService
from src.services.quality_service import QualityService
from src.services.metadata_service import MetadataService


class TestPipelineService:
    """Test cases for PipelineService."""

    def test_service_initialization(self, mock_config):
        """Test service initialization."""
        service = PipelineService(config=mock_config)
        assert service.config == mock_config
        assert isinstance(service.quality_service, QualityService)
        assert isinstance(service.metadata_service, MetadataService)

    def test_service_initialization_with_dependencies(self, mock_config):
        """Test service initialization with custom dependencies."""
        mock_quality = Mock(spec=QualityService)
        mock_metadata = Mock(spec=MetadataService)
        
        service = PipelineService(
            quality_service=mock_quality,
            metadata_service=mock_metadata,
            config=mock_config,
        )
        
        assert service.quality_service == mock_quality
        assert service.metadata_service == mock_metadata

    def test_run_quality_checks_success(self, mock_config):
        """Test successful quality checks."""
        mock_quality = Mock(spec=QualityService)
        mock_quality.validate_quality_before_sync.return_value = True
        
        service = PipelineService(quality_service=mock_quality, config=mock_config)
        result = service.run_quality_checks("raw")
        
        assert result["status"] == "success"
        assert result["layer"] == "raw"
        mock_quality.validate_quality_before_sync.assert_called_once_with("raw")

    def test_run_quality_checks_failure(self, mock_config):
        """Test quality checks failure."""
        mock_quality = Mock(spec=QualityService)
        mock_quality.validate_quality_before_sync.return_value = False
        
        service = PipelineService(quality_service=mock_quality, config=mock_config)
        result = service.run_quality_checks("raw")
        
        assert result["status"] == "failed"
        assert result["layer"] == "raw"

    def test_sync_metadata_with_quality_gate_passes(self, mock_config):
        """Test metadata sync when quality gate passes."""
        mock_quality = Mock(spec=QualityService)
        mock_quality.validate_quality_before_sync.return_value = True
        
        mock_metadata = Mock(spec=MetadataService)
        mock_metadata.sync_layer_metadata.return_value = {
            "status": "success",
            "layer": "raw",
        }
        
        service = PipelineService(
            quality_service=mock_quality,
            metadata_service=mock_metadata,
            config=mock_config,
        )
        result = service.sync_metadata_with_quality_gate("raw", strict=True)
        
        assert result["status"] == "success"
        mock_quality.validate_quality_before_sync.assert_called_once()
        mock_metadata.sync_layer_metadata.assert_called_once()

    def test_sync_metadata_with_quality_gate_fails(self, mock_config):
        """Test metadata sync when quality gate fails."""
        mock_quality = Mock(spec=QualityService)
        mock_quality.validate_quality_before_sync.return_value = False
        
        service = PipelineService(quality_service=mock_quality, config=mock_config)
        result = service.sync_metadata_with_quality_gate("raw", strict=True)
        
        assert result["status"] == "quality_gate_failed"
        assert result["layer"] == "raw"

    def test_sync_metadata_with_quality_gate_lenient(self, mock_config):
        """Test metadata sync in lenient mode (no quality gate)."""
        mock_metadata = Mock(spec=MetadataService)
        mock_metadata.sync_layer_metadata.return_value = {
            "status": "success",
            "layer": "raw",
        }
        
        service = PipelineService(metadata_service=mock_metadata, config=mock_config)
        result = service.sync_metadata_with_quality_gate("raw", strict=False)
        
        assert result["status"] == "success"
        mock_metadata.sync_layer_metadata.assert_called_once()

    def test_run_complete_pipeline_success(self, mock_config):
        """Test successful complete pipeline run."""
        mock_quality = Mock(spec=QualityService)
        mock_quality.validate_quality_before_sync.return_value = True
        
        mock_metadata = Mock(spec=MetadataService)
        mock_metadata.sync_layer_metadata.return_value = {
            "status": "success",
            "layer": "raw",
        }
        
        service = PipelineService(
            quality_service=mock_quality,
            metadata_service=mock_metadata,
            config=mock_config,
        )
        result = service.run_complete_pipeline(layers=["raw"], strict=False)
        
        assert result["status"] == "success"
        assert "raw" in result["layers"]
