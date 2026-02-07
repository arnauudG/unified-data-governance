"""
Unit tests for MetadataService.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

from src.services.metadata_service import MetadataService
from src.repositories.collibra_repository import CollibraRepository
from src.core.exceptions import ConfigurationError


class TestMetadataService:
    """Test cases for MetadataService."""

    def test_service_initialization(self, mock_config):
        """Test service initialization."""
        service = MetadataService(config=mock_config)
        assert service.config == mock_config
        assert isinstance(service.collibra_repository, CollibraRepository)

    def test_service_initialization_with_repository(self, mock_config):
        """Test service initialization with custom repository."""
        mock_repo = Mock(spec=CollibraRepository)
        service = MetadataService(collibra_repository=mock_repo, config=mock_config)
        assert service.collibra_repository == mock_repo

    @patch("builtins.open", create=True)
    @patch("yaml.safe_load")
    def test_load_collibra_config_success(self, mock_yaml_load, mock_open, mock_config):
        """Test successful config loading."""
        mock_config_data = {
            "database_id": "db_123",
            "raw": {"schema_connection_ids": ["schema_1", "schema_2"]},
        }
        mock_yaml_load.return_value = mock_config_data
        
        # Mock Path.exists to return True
        with patch.object(Path, "exists", return_value=True):
            service = MetadataService(config=mock_config)
            config = service.load_collibra_config()
            
            assert config["database_id"] == "db_123"
            assert config["raw"]["schema_connection_ids"] == ["schema_1", "schema_2"]

    def test_load_collibra_config_not_found(self, mock_config):
        """Test error when config file not found."""
        with patch.object(Path, "exists", return_value=False):
            service = MetadataService(config=mock_config)
            
            with pytest.raises(ConfigurationError):
                service.load_collibra_config()

    @patch("builtins.open", create=True)
    @patch("yaml.safe_load")
    def test_load_collibra_config_empty(self, mock_yaml_load, mock_open, mock_config):
        """Test error when config file is empty."""
        mock_yaml_load.return_value = None
        
        with patch.object(Path, "exists", return_value=True):
            service = MetadataService(config=mock_config)
            
            with pytest.raises(ConfigurationError):
                service.load_collibra_config()

    @patch("builtins.open", create=True)
    @patch("yaml.safe_load")
    def test_sync_layer_metadata_success(self, mock_yaml_load, mock_open, mock_config):
        """Test successful layer metadata sync."""
        mock_config_data = {
            "database_id": "db_123",
            "raw": {"schema_connection_ids": ["schema_asset_1"]},
        }
        mock_yaml_load.return_value = mock_config_data
        
        mock_repo = Mock(spec=CollibraRepository)
        mock_repo.resolve_schema_connection_ids.return_value = ["schema_conn_1"]
        mock_repo.trigger_metadata_sync.return_value = {
            "jobId": "job_123",
            "status": "triggered",
        }
        
        with patch.object(Path, "exists", return_value=True):
            service = MetadataService(collibra_repository=mock_repo, config=mock_config)
            result = service.sync_layer_metadata("raw")
            
            assert result["status"] == "success"
            assert result["layer"] == "raw"
            mock_repo.resolve_schema_connection_ids.assert_called_once()
            mock_repo.trigger_metadata_sync.assert_called_once()

    @patch("builtins.open", create=True)
    @patch("yaml.safe_load")
    def test_sync_layer_metadata_no_schemas(self, mock_yaml_load, mock_open, mock_config):
        """Test sync when no schemas configured."""
        mock_config_data = {
            "database_id": "db_123",
            "raw": {"schema_connection_ids": []},
        }
        mock_yaml_load.return_value = mock_config_data
        
        mock_repo = Mock(spec=CollibraRepository)
        
        with patch.object(Path, "exists", return_value=True):
            service = MetadataService(collibra_repository=mock_repo, config=mock_config)
            result = service.sync_layer_metadata("raw")
            
            assert result["status"] == "skipped"
            assert result["reason"] == "No schema asset IDs configured"
            mock_repo.trigger_metadata_sync.assert_not_called()

    @patch("builtins.open", create=True)
    @patch("yaml.safe_load")
    def test_sync_all_layers_success(self, mock_yaml_load, mock_open, mock_config):
        """Test successful sync of all layers."""
        mock_config_data = {
            "database_id": "db_123",
            "raw": {"schema_connection_ids": ["schema_1"]},
            "staging": {"schema_connection_ids": ["schema_2"]},
        }
        mock_yaml_load.return_value = mock_config_data
        
        mock_repo = Mock(spec=CollibraRepository)
        mock_repo.resolve_schema_connection_ids.return_value = ["schema_conn_1"]
        mock_repo.trigger_metadata_sync.return_value = {
            "jobId": "job_123",
            "status": "triggered",
        }
        
        with patch.object(Path, "exists", return_value=True):
            service = MetadataService(collibra_repository=mock_repo, config=mock_config)
            results = service.sync_all_layers(["raw", "staging"])
            
            assert len(results) == 2
            assert results["raw"]["status"] == "success"
            assert results["staging"]["status"] == "success"
