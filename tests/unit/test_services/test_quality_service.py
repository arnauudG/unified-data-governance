"""
Unit tests for QualityService.
"""

import pytest
from unittest.mock import Mock, MagicMock
from pathlib import Path

from src.services.quality_service import QualityService
from src.repositories.soda_repository import SodaRepository


class TestQualityService:
    """Test cases for QualityService."""

    def test_service_initialization(self, mock_config):
        """Test service initialization."""
        service = QualityService(config=mock_config)
        assert service.config == mock_config
        assert isinstance(service.soda_repository, SodaRepository)

    def test_service_initialization_with_repository(self, mock_config):
        """Test service initialization with custom repository."""
        mock_repo = Mock(spec=SodaRepository)
        service = QualityService(soda_repository=mock_repo, config=mock_config)
        assert service.soda_repository == mock_repo

    def test_get_failed_critical_checks_no_failures(self, mock_config):
        """Test when no critical checks have failed."""
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
        failed = service.get_failed_critical_checks("raw")
        
        assert len(failed) == 0

    def test_get_failed_critical_checks_with_failures(self, mock_config):
        """Test when critical checks have failed."""
        mock_repo = Mock(spec=SodaRepository)
        mock_repo.get_all_checks.return_value = [
            {
                "name": "failed_check",
                "evaluationStatus": "fail",
                "attributes": {"critical": True},
                "dataset": {"name": "CUSTOMERS"},
            },
            {
                "name": "passed_check",
                "evaluationStatus": "pass",
                "attributes": {"critical": True},
                "dataset": {"name": "PRODUCTS"},
            },
        ]
        
        service = QualityService(soda_repository=mock_repo, config=mock_config)
        failed = service.get_failed_critical_checks("raw")
        
        assert len(failed) == 1
        assert failed[0]["name"] == "failed_check"
        assert failed[0]["dataset"] == "CUSTOMERS"

    def test_get_failed_critical_checks_filters_by_layer(self, mock_config):
        """Test that checks are filtered by expected datasets for layer."""
        mock_repo = Mock(spec=SodaRepository)
        mock_repo.get_all_checks.return_value = [
            {
                "name": "failed_check",
                "evaluationStatus": "fail",
                "attributes": {"critical": True},
                "dataset": {"name": "FACT_ORDERS"},  # MART layer dataset
            }
        ]
        
        service = QualityService(soda_repository=mock_repo, config=mock_config)
        failed = service.get_failed_critical_checks("raw")  # RAW layer
        
        # Should be empty because FACT_ORDERS is not in RAW expected datasets
        assert len(failed) == 0

    def test_validate_quality_before_sync_passes(self, mock_config):
        """Test quality validation when checks pass."""
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

    def test_validate_quality_before_sync_fails(self, mock_config):
        """Test quality validation when critical checks fail."""
        mock_repo = Mock(spec=SodaRepository)
        mock_repo.get_all_checks.return_value = [
            {
                "name": "failed_check",
                "evaluationStatus": "fail",
                "attributes": {"critical": True},
                "dataset": {"name": "CUSTOMERS"},
            }
        ]
        
        service = QualityService(soda_repository=mock_repo, config=mock_config)
        result = service.validate_quality_before_sync("raw")
        
        assert result is False

    def test_export_quality_data_success(self, mock_config, temp_output_dir):
        """Test successful quality data export (uses real pandas to write CSV)."""
        mock_repo = Mock(spec=SodaRepository)
        mock_repo.get_all_datasets.return_value = [
            {"id": "1", "name": "DATASET_1"},
            {"id": "2", "name": "DATASET_2"},
        ]
        mock_repo.get_all_checks.return_value = [
            {"id": "1", "name": "CHECK_1"},
            {"id": "2", "name": "CHECK_2"},
        ]

        service = QualityService(soda_repository=mock_repo, config=mock_config)
        files = service.export_quality_data(temp_output_dir)

        assert "datasets" in files
        assert "checks" in files
        assert files["datasets"].exists()
        assert files["checks"].exists()

    def test_export_quality_data_empty(self, mock_config, temp_output_dir):
        """Test export when no data is available."""
        mock_repo = Mock(spec=SodaRepository)
        mock_repo.get_all_datasets.return_value = []
        mock_repo.get_all_checks.return_value = []
        
        service = QualityService(soda_repository=mock_repo, config=mock_config)
        files = service.export_quality_data(temp_output_dir)
        
        # Should still return dict but may be empty
        assert isinstance(files, dict)
