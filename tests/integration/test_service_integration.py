"""
Integration tests for service layer interactions.
"""

import pytest
from unittest.mock import Mock, MagicMock

from src.services.quality_service import QualityService
from src.services.metadata_service import MetadataService
from src.services.pipeline_service import PipelineService
from src.repositories.soda_repository import SodaRepository
from src.repositories.collibra_repository import CollibraRepository


class TestServiceIntegration:
    """Integration tests for service layer interactions."""

    def test_quality_service_with_repository(self, mock_config):
        """Test QualityService integration with SodaRepository."""
        mock_repo = Mock(spec=SodaRepository)
        mock_repo.get_all_checks.return_value = [
            {
                "name": "test_check",
                "evaluationStatus": "pass",
                "attributes": {"critical": True},
                "dataset": {"name": "CUSTOMERS"},
            }
        ]
        
        service = QualityService(soda_repository=mock_repo, config=mock_config)
        result = service.validate_quality_before_sync("raw")
        
        assert result is True
        mock_repo.get_all_checks.assert_called_once()

    def test_metadata_service_with_repository(self, mock_config):
        """Test MetadataService integration with CollibraRepository."""
        mock_repo = Mock(spec=CollibraRepository)
        mock_repo.resolve_schema_connection_ids.return_value = ["conn_1"]
        mock_repo.trigger_metadata_sync.return_value = {
            "jobId": "job_123",
            "status": "triggered",
        }
        
        # Mock config loading
        with patch("builtins.open", create=True), patch("yaml.safe_load") as mock_yaml:
            mock_yaml.return_value = {
                "database_id": "db_123",
                "raw": {"schema_connection_ids": ["schema_1"]},
            }
            
            with patch.object(MetadataService, "load_collibra_config") as mock_load:
                mock_load.return_value = {
                    "database_id": "db_123",
                    "raw": {"schema_connection_ids": ["schema_1"]},
                }
                
                service = MetadataService(
                    collibra_repository=mock_repo, config=mock_config
                )
                result = service.sync_layer_metadata("raw")
                
                assert result["status"] == "success"
                mock_repo.resolve_schema_connection_ids.assert_called_once()
                mock_repo.trigger_metadata_sync.assert_called_once()

    def test_pipeline_service_end_to_end(self, mock_config):
        """Test PipelineService end-to-end workflow."""
        # Setup mocks
        mock_soda_repo = Mock(spec=SodaRepository)
        mock_soda_repo.get_all_checks.return_value = [
            {
                "name": "test_check",
                "evaluationStatus": "pass",
                "attributes": {"critical": True},
                "dataset": {"name": "CUSTOMERS"},
            }
        ]
        
        mock_collibra_repo = Mock(spec=CollibraRepository)
        mock_collibra_repo.resolve_schema_connection_ids.return_value = ["conn_1"]
        mock_collibra_repo.trigger_metadata_sync.return_value = {
            "jobId": "job_123",
            "status": "triggered",
        }
        
        # Create services
        quality_service = QualityService(soda_repository=mock_soda_repo, config=mock_config)
        
        metadata_service = MetadataService(
            collibra_repository=mock_collibra_repo, config=mock_config
        )
        
        pipeline_service = PipelineService(
            quality_service=quality_service,
            metadata_service=metadata_service,
            config=mock_config,
        )
        
        # Mock metadata service config loading
        with patch.object(metadata_service, "load_collibra_config") as mock_load:
            mock_load.return_value = {
                "database_id": "db_123",
                "raw": {"schema_connection_ids": ["schema_1"]},
            }
            
            # Run pipeline
            result = pipeline_service.sync_metadata_with_quality_gate("raw", strict=True)
            
            assert result["status"] == "success"
            mock_soda_repo.get_all_checks.assert_called()
            mock_collibra_repo.trigger_metadata_sync.assert_called_once()
