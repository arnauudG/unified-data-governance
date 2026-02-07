"""
Unit tests for SodaRepository.

Comprehensive test coverage for Soda Cloud API repository.
"""

import pytest
import time
from unittest.mock import Mock, patch, MagicMock
import requests

from src.repositories.soda_repository import SodaRepository
from src.core.exceptions import RetryableError, NonRetryableError, APIError
from src.core.constants import APIEndpoints


class TestSodaRepository:
    """Test cases for SodaRepository."""

    def test_repository_initialization(self, mock_config):
        """Test repository initialization."""
        repo = SodaRepository(config=mock_config)
        assert repo.base_url == mock_config.soda_cloud.host
        assert repo.api_key_id == mock_config.soda_cloud.api_key_id
        assert repo.api_key_secret == mock_config.soda_cloud.api_key_secret
        assert repo.session is None  # Not connected yet

    def test_repository_initialization_without_config(self):
        """Test repository initialization without config (uses get_config)."""
        with patch("src.repositories.soda_repository.get_config") as mock_get_config:
            mock_config = Mock()
            mock_config.soda_cloud.host = "https://cloud.soda.io"
            mock_config.soda_cloud.api_key_id = "test_key"
            mock_config.soda_cloud.api_key_secret = "test_secret"
            mock_get_config.return_value = mock_config
            
            repo = SodaRepository()
            assert repo.base_url == "https://cloud.soda.io"

    def test_connect(self, mock_config):
        """Test repository connection."""
        repo = SodaRepository(config=mock_config)
        repo.connect()
        assert repo.session is not None
        assert repo.session.auth == (
            mock_config.soda_cloud.api_key_id,
            mock_config.soda_cloud.api_key_secret,
        )
        assert "Content-Type" in repo.session.headers
        assert "Accept" in repo.session.headers

    def test_disconnect(self, mock_config):
        """Test repository disconnection."""
        repo = SodaRepository(config=mock_config)
        repo.connect()
        assert repo.session is not None
        repo.disconnect()
        assert repo.session is None

    def test_context_manager(self, mock_config):
        """Test repository as context manager."""
        repo = SodaRepository(config=mock_config)
        with repo:
            assert repo.session is not None
        # Session should be closed after context exit
        assert repo.session is None

    def test_context_manager_exception_handling(self, mock_config):
        """Test context manager handles exceptions properly."""
        repo = SodaRepository(config=mock_config)
        try:
            with repo:
                assert repo.session is not None
                raise ValueError("Test exception")
        except ValueError:
            pass
        # Session should still be closed even on exception
        assert repo.session is None

    @patch("src.repositories.soda_repository.requests.Session")
    def test_get_datasets_single_page(self, mock_session_class, mock_config, mock_soda_response):
        """Test dataset retrieval for single page."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        
        mock_response = Mock()
        mock_response.json.return_value = mock_soda_response
        mock_response.raise_for_status = Mock()
        mock_session.request.return_value = mock_response
        
        repo = SodaRepository(config=mock_config)
        result = repo.get_datasets(page=0, size=100)
        
        assert "content" in result
        assert len(result["content"]) == 2
        assert result["totalPages"] == 2

    @patch("src.repositories.soda_repository.requests.Session")
    def test_get_all_datasets_success(self, mock_session_class, mock_config, mock_soda_response):
        """Test successful dataset retrieval with pagination."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        
        # First page response
        first_page_response = Mock()
        first_page_response.json.return_value = {
            "totalPages": 2,
            "content": mock_soda_response["content"],
        }
        first_page_response.raise_for_status = Mock()
        
        # Second page response
        second_page_response = Mock()
        second_page_response.json.return_value = {
            "totalPages": 2,
            "content": [{"id": "dataset_3", "name": "DATASET_3", "health": "pass"}],
        }
        second_page_response.raise_for_status = Mock()
        
        mock_session.request.side_effect = [first_page_response, second_page_response]
        
        repo = SodaRepository(config=mock_config)
        datasets = repo.get_all_datasets()
        
        assert len(datasets) == 3
        assert datasets[0]["id"] == "dataset_1"
        assert datasets[2]["id"] == "dataset_3"

    @patch("src.repositories.soda_repository.requests.Session")
    @patch("src.repositories.soda_repository.time.sleep")
    def test_get_all_datasets_respects_api_rate_limit(
        self, mock_sleep, mock_session_class, mock_config, mock_soda_response
    ):
        """Test that pagination respects API rate limits."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        
        first_page_response = Mock()
        first_page_response.json.return_value = {
            "totalPages": 2,
            "content": mock_soda_response["content"],
        }
        first_page_response.raise_for_status = Mock()
        
        second_page_response = Mock()
        second_page_response.json.return_value = {
            "totalPages": 2,
            "content": [],
        }
        second_page_response.raise_for_status = Mock()
        
        mock_session.request.side_effect = [first_page_response, second_page_response]
        
        repo = SodaRepository(config=mock_config)
        repo.get_all_datasets()
        
        # Should sleep between pages
        assert mock_sleep.called

    @patch("src.repositories.soda_repository.requests.Session")
    def test_get_all_datasets_rate_limit(self, mock_session_class, mock_config):
        """Test rate limit handling."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        
        # Mock rate limit response
        mock_response = Mock()
        mock_response.status_code = 429
        mock_response.text = "Rate limit exceeded"
        mock_error = requests.exceptions.HTTPError(response=mock_response)
        mock_response.raise_for_status.side_effect = mock_error
        mock_session.request.return_value = mock_response
        
        repo = SodaRepository(config=mock_config)
        
        with pytest.raises(RetryableError) as exc_info:
            repo.get_all_datasets()
        
        assert exc_info.value.status_code == 429

    @patch("src.repositories.soda_repository.requests.Session")
    def test_get_all_datasets_unauthorized(self, mock_session_class, mock_config):
        """Test unauthorized access handling."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        
        # Mock unauthorized response
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"
        mock_error = requests.exceptions.HTTPError(response=mock_response)
        mock_response.raise_for_status.side_effect = mock_error
        mock_session.request.return_value = mock_response
        
        repo = SodaRepository(config=mock_config)
        
        with pytest.raises(NonRetryableError) as exc_info:
            repo.get_all_datasets()
        
        assert exc_info.value.status_code == 401

    @patch("src.repositories.soda_repository.requests.Session")
    def test_get_all_datasets_server_error(self, mock_session_class, mock_config):
        """Test server error handling (retryable)."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_error = requests.exceptions.HTTPError(response=mock_response)
        mock_response.raise_for_status.side_effect = mock_error
        mock_session.request.return_value = mock_response
        
        repo = SodaRepository(config=mock_config)
        
        with pytest.raises(RetryableError) as exc_info:
            repo.get_all_datasets()
        
        assert exc_info.value.status_code == 500

    @patch("src.repositories.soda_repository.requests.Session")
    def test_get_all_checks_success(self, mock_session_class, mock_config, mock_check_response):
        """Test successful check retrieval."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        
        mock_response = Mock()
        mock_response.json.return_value = mock_check_response
        mock_response.raise_for_status = Mock()
        mock_session.request.return_value = mock_response
        
        repo = SodaRepository(config=mock_config)
        checks = repo.get_all_checks()
        
        assert len(checks) == 2
        assert checks[0]["id"] == "check_1"

    @patch("src.repositories.soda_repository.requests.Session")
    def test_get_check_by_id(self, mock_session_class, mock_config):
        """Test retrieving a specific check by ID."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        
        mock_response = Mock()
        mock_response.json.return_value = {
            "id": "check_123",
            "name": "test_check",
            "evaluationStatus": "pass",
        }
        mock_response.raise_for_status = Mock()
        mock_session.request.return_value = mock_response
        
        repo = SodaRepository(config=mock_config)
        check = repo.get_check("check_123")
        
        assert check["id"] == "check_123"
        assert check["name"] == "test_check"
        mock_session.request.assert_called_once()
        call_args = mock_session.request.call_args
        assert "check_123" in call_args[0][1]  # Check ID in URL

    @patch("src.repositories.soda_repository.requests.Session")
    def test_get_dataset_by_id(self, mock_session_class, mock_config):
        """Test retrieving a specific dataset by ID."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        
        mock_response = Mock()
        mock_response.json.return_value = {
            "id": "dataset_123",
            "name": "TEST_DATASET",
            "health": "pass",
        }
        mock_response.raise_for_status = Mock()
        mock_session.request.return_value = mock_response
        
        repo = SodaRepository(config=mock_config)
        dataset = repo.get_dataset("dataset_123")
        
        assert dataset["id"] == "dataset_123"
        assert dataset["name"] == "TEST_DATASET"
        mock_session.request.assert_called_once()
        call_args = mock_session.request.call_args
        assert "dataset_123" in call_args[0][1]  # Dataset ID in URL

    @patch("src.repositories.soda_repository.requests.Session")
    def test_make_request_connection_error(self, mock_session_class, mock_config):
        """Test connection error handling."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        
        mock_session.request.side_effect = requests.exceptions.ConnectionError("Connection failed")
        
        repo = SodaRepository(config=mock_config)
        
        with pytest.raises(RetryableError):
            repo.get_datasets()

    @patch("src.repositories.soda_repository.requests.Session")
    def test_make_request_timeout(self, mock_session_class, mock_config):
        """Test timeout error handling."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        
        mock_session.request.side_effect = requests.exceptions.Timeout("Request timeout")
        
        repo = SodaRepository(config=mock_config)
        
        with pytest.raises(RetryableError):
            repo.get_datasets()

    @patch("src.repositories.soda_repository.requests.Session")
    def test_make_request_auto_connect(self, mock_session_class, mock_config):
        """Test that _make_request auto-connects if not connected."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        
        mock_response = Mock()
        mock_response.json.return_value = {"content": []}
        mock_response.raise_for_status = Mock()
        mock_session.request.return_value = mock_response
        
        repo = SodaRepository(config=mock_config)
        assert repo.session is None
        
        # Should auto-connect
        repo.get_datasets()
        
        assert repo.session is not None
        mock_session_class.assert_called_once()

    def test_get_datasets_pagination_params(self, mock_config):
        """Test that pagination parameters are correctly passed."""
        with patch("src.repositories.soda_repository.requests.Session") as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value = mock_session
            
            mock_response = Mock()
            mock_response.json.return_value = {"content": [], "totalPages": 1}
            mock_response.raise_for_status = Mock()
            mock_session.request.return_value = mock_response
            
            repo = SodaRepository(config=mock_config)
            repo.get_datasets(page=2, size=50)
            
            call_args = mock_session.request.call_args
            assert "page=2" in call_args[0][1]
            assert "size=50" in call_args[0][1]
