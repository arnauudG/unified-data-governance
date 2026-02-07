"""
Basic tests for SodaCollibraIntegration class
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from integration import SodaCollibraIntegration
from constants import IntegrationConstants

class TestSodaCollibraIntegration:
    """Test cases for SodaCollibraIntegration"""
    
    @pytest.fixture
    def mock_config(self):
        """Mock configuration object"""
        config = Mock()
        config.collibra.base_url = "https://test-collibra.com"
        config.soda.base_url = "https://test-soda.com"
        config.collibra.asset_types.table_asset_type = "table-type-id"
        config.collibra.asset_types.soda_check_asset_type = "check-type-id"
        config.soda.general.filter_datasets_to_sync_to_collibra = False
        config.soda.general.soda_no_collibra_dataset_skip_checks = False
        return config
    
    @pytest.fixture
    def mock_clients(self):
        """Mock client instances"""
        return {
            'collibra': Mock(),
            'soda': Mock()
        }
    
    @patch('integration.load_config')
    @patch('integration.CollibraClient')
    @patch('integration.SodaClient')
    def test_integration_initialization(self, mock_soda_client, mock_collibra_client, mock_load_config, mock_config):
        """Test integration initialization"""
        mock_load_config.return_value = mock_config
        
        integration = SodaCollibraIntegration()
        
        assert integration.config == mock_config
        mock_load_config.assert_called_once()
        mock_collibra_client.assert_called_once_with(mock_config.collibra)
        mock_soda_client.assert_called_once_with(mock_config.soda)
    
    @patch('integration.load_config')
    @patch('integration.CollibraClient')
    @patch('integration.SodaClient')
    def test_run_integration_success(self, mock_soda_client, mock_collibra_client, mock_load_config, mock_config):
        """Test successful integration run"""
        mock_load_config.return_value = mock_config
        
        # Mock client instances
        mock_soda_instance = Mock()
        mock_collibra_instance = Mock()
        mock_soda_client.return_value = mock_soda_instance
        mock_collibra_client.return_value = mock_collibra_instance
        
        # Mock API responses
        mock_soda_instance.test_connection.return_value = Mock()
        mock_soda_instance.get_datasets.return_value = []
        
        integration = SodaCollibraIntegration()
        result = integration.run()
        
        assert isinstance(result, dict)
        assert 'datasets_processed' in result
        mock_soda_instance.test_connection.assert_called_once()
        mock_soda_instance.get_datasets.assert_called_once()
    
    @patch('integration.load_config')
    @patch('integration.CollibraClient')  
    @patch('integration.SodaClient')
    def test_filter_datasets_disabled(self, mock_soda_client, mock_collibra_client, mock_load_config, mock_config):
        """Test dataset filtering when disabled"""
        mock_load_config.return_value = mock_config
        mock_config.soda.general.filter_datasets_to_sync_to_collibra = False
        
        integration = SodaCollibraIntegration()
        
        # Mock datasets
        datasets = [Mock(name="dataset1"), Mock(name="dataset2")]
        filtered = integration._filter_datasets(datasets)
        
        assert len(filtered) == 2
        assert filtered == datasets
    
    @patch('integration.load_config')
    @patch('integration.CollibraClient')
    @patch('integration.SodaClient')
    def test_filter_datasets_enabled(self, mock_soda_client, mock_collibra_client, mock_load_config, mock_config):
        """Test dataset filtering when enabled"""
        mock_load_config.return_value = mock_config
        mock_config.soda.general.filter_datasets_to_sync_to_collibra = True
        mock_config.soda.attributes.soda_collibra_sync_dataset_attribute = "sync_attr"
        
        integration = SodaCollibraIntegration()
        
        # Mock datasets - one with sync attribute, one without
        dataset1 = Mock(name="dataset1")
        dataset1.attributes = {"sync_attr": True}
        dataset2 = Mock(name="dataset2") 
        dataset2.attributes = {}
        
        datasets = [dataset1, dataset2]
        filtered = integration._filter_datasets(datasets)
        
        assert len(filtered) == 1
        assert filtered[0] == dataset1
        assert integration.metrics.get_overall_metrics().datasets_skipped == 1

# Run tests with: python -m pytest tests/test_integration.py -v 