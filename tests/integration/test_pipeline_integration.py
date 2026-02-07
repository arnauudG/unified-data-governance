"""
Integration tests for complete pipeline workflows.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

from src.factories import ServiceFactory, ClientFactory
from src.services.pipeline_service import PipelineService


class TestPipelineIntegration:
    """Integration tests for complete pipeline workflows."""

    def test_factory_creates_services_with_dependencies(self, mock_config):
        """Test that factories create services with proper dependencies."""
        factory = ServiceFactory(config=mock_config)
        
        quality_service = factory.get_quality_service()
        metadata_service = factory.get_metadata_service()
        pipeline_service = factory.get_pipeline_service()
        
        assert quality_service is not None
        assert metadata_service is not None
        assert pipeline_service is not None
        
        # Verify dependencies are injected
        assert pipeline_service.quality_service == quality_service
        assert pipeline_service.metadata_service == metadata_service

    def test_factory_singleton_instances(self, mock_config):
        """Test that factories return singleton instances."""
        factory = ServiceFactory(config=mock_config)
        
        service1 = factory.get_quality_service()
        service2 = factory.get_quality_service()
        
        assert service1 is service2  # Should be same instance

    def test_pipeline_service_complete_workflow(self, mock_config):
        """Test complete pipeline workflow with mocked dependencies."""
        # Create factory with mocked repositories
        client_factory = ClientFactory(config=mock_config)
        
        # Mock repositories
        mock_soda_repo = Mock()
        mock_soda_repo.get_all_checks.return_value = [
            {
                "name": "test_check",
                "evaluationStatus": "pass",
                "attributes": {"critical": True},
                "dataset": {"name": "CUSTOMERS"},
            }
        ]
        client_factory._soda_repository = mock_soda_repo
        
        mock_collibra_repo = Mock()
        mock_collibra_repo.resolve_schema_connection_ids.return_value = ["conn_1"]
        mock_collibra_repo.trigger_metadata_sync.return_value = {
            "jobId": "job_123",
            "status": "triggered",
        }
        client_factory._collibra_repository = mock_collibra_repo
        
        # Create service factory
        service_factory = ServiceFactory(config=mock_config, client_factory=client_factory)
        
        # Get pipeline service
        pipeline_service = service_factory.get_pipeline_service()
        
        # Mock metadata service config loading
        with patch.object(
            pipeline_service.metadata_service, "load_collibra_config"
        ) as mock_load:
            mock_load.return_value = {
                "database_id": "db_123",
                "raw": {"schema_connection_ids": ["schema_1"]},
            }
            
            # Run pipeline
            result = pipeline_service.sync_metadata_with_quality_gate("raw", strict=True)
            
            assert result["status"] == "success"
            mock_soda_repo.get_all_checks.assert_called()
            mock_collibra_repo.trigger_metadata_sync.assert_called_once()
