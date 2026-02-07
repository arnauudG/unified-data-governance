"""
Unit tests for CollibraRepository.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import requests

from src.repositories.collibra_repository import CollibraRepository
from src.core.exceptions import RetryableError, NonRetryableError, ConfigurationError


class TestCollibraRepository:
    """Test cases for CollibraRepository."""

    def test_repository_initialization(self, mock_config):
        """Test repository initialization."""
        repo = CollibraRepository(config=mock_config)
        assert repo.base_url == mock_config.collibra.base_url
        assert repo.username == mock_config.collibra.username
        assert repo.password == mock_config.collibra.password

    def test_connect(self, mock_config):
        """Test repository connection."""
        repo = CollibraRepository(config=mock_config)
        repo.connect()
        assert repo.session is not None
        assert repo.session.auth == (
            mock_config.collibra.username,
            mock_config.collibra.password,
        )

    def test_disconnect(self, mock_config):
        """Test repository disconnection."""
        repo = CollibraRepository(config=mock_config)
        repo.connect()
        repo.disconnect()
        assert repo.session is None

    def test_context_manager(self, mock_config):
        """Test repository as context manager."""
        with CollibraRepository(config=mock_config) as repo:
            assert repo.session is not None
        # Session should be closed after context exit
        assert repo.session is None

    @patch("src.repositories.collibra_repository.requests.Session")
    def test_get_database_success(self, mock_session_class, mock_config):
        """Test successful database retrieval."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        
        mock_response = Mock()
        mock_response.json.return_value = {
            "id": "db_123",
            "databaseConnectionId": "conn_456",
        }
        mock_response.raise_for_status = Mock()
        mock_session.request.return_value = mock_response
        
        repo = CollibraRepository(config=mock_config)
        database = repo.get_database("db_123")
        
        assert database["id"] == "db_123"
        assert database["databaseConnectionId"] == "conn_456"

    @patch("src.repositories.collibra_repository.requests.Session")
    def test_get_database_connection_id_success(self, mock_session_class, mock_config):
        """Test successful database connection ID retrieval."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        
        mock_response = Mock()
        mock_response.json.return_value = {
            "id": "db_123",
            "databaseConnectionId": "conn_456",
        }
        mock_response.raise_for_status = Mock()
        mock_session.request.return_value = mock_response
        
        repo = CollibraRepository(config=mock_config)
        connection_id = repo.get_database_connection_id("db_123")
        
        assert connection_id == "conn_456"

    @patch("src.repositories.collibra_repository.requests.Session")
    def test_get_database_connection_id_missing(self, mock_session_class, mock_config):
        """Test error when database connection ID is missing."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        
        mock_response = Mock()
        mock_response.json.return_value = {
            "id": "db_123",
            # Missing databaseConnectionId
        }
        mock_response.raise_for_status = Mock()
        mock_session.request.return_value = mock_response
        
        repo = CollibraRepository(config=mock_config)
        
        with pytest.raises(ConfigurationError):
            repo.get_database_connection_id("db_123")

    @patch("src.repositories.collibra_repository.requests.Session")
    def test_list_schema_connections_success(self, mock_session_class, mock_config):
        """Test successful schema connections listing."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        
        mock_response = Mock()
        mock_response.json.return_value = {
            "results": [
                {"id": "schema_1", "name": "RAW"},
                {"id": "schema_2", "name": "STAGING"},
            ]
        }
        mock_response.raise_for_status = Mock()
        mock_session.request.return_value = mock_response
        
        repo = CollibraRepository(config=mock_config)
        connections = repo.list_schema_connections("conn_456")
        
        assert len(connections) == 2
        assert connections[0]["id"] == "schema_1"

    @patch("src.repositories.collibra_repository.requests.Session")
    def test_resolve_schema_connection_ids_success(self, mock_session_class, mock_config):
        """Test successful schema connection ID resolution."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        
        # Mock get_database response
        db_response = Mock()
        db_response.json.return_value = {
            "id": "db_123",
            "databaseConnectionId": "conn_456",
        }
        db_response.raise_for_status = Mock()
        
        # Mock list_schema_connections response
        schema_response = Mock()
        schema_response.json.return_value = {
            "results": [{"id": "schema_conn_1", "name": "RAW"}]
        }
        schema_response.raise_for_status = Mock()
        
        mock_session.request.side_effect = [db_response, schema_response]
        
        repo = CollibraRepository(config=mock_config)
        connection_ids = repo.resolve_schema_connection_ids("db_123", ["schema_asset_1"])
        
        assert len(connection_ids) == 1
        assert connection_ids[0] == "schema_conn_1"

    @patch("src.repositories.collibra_repository.requests.Session")
    def test_resolve_schema_connection_ids_not_found(self, mock_session_class, mock_config):
        """Test error when schema connection is not found."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        
        # Mock get_database response
        db_response = Mock()
        db_response.json.return_value = {
            "id": "db_123",
            "databaseConnectionId": "conn_456",
        }
        db_response.raise_for_status = Mock()
        
        # Mock empty list_schema_connections response
        schema_response = Mock()
        schema_response.json.return_value = {"results": []}
        schema_response.raise_for_status = Mock()
        
        mock_session.request.side_effect = [db_response, schema_response]
        
        repo = CollibraRepository(config=mock_config)
        
        with pytest.raises(ConfigurationError):
            repo.resolve_schema_connection_ids("db_123", ["schema_asset_1"])

    @patch("src.repositories.collibra_repository.requests.Session")
    def test_trigger_metadata_sync_success(self, mock_session_class, mock_config):
        """Test successful metadata sync trigger."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        
        mock_response = Mock()
        mock_response.json.return_value = {
            "jobId": "job_123",
            "status": "triggered",
        }
        mock_response.raise_for_status = Mock()
        mock_session.request.return_value = mock_response
        
        repo = CollibraRepository(config=mock_config)
        result = repo.trigger_metadata_sync("db_123", ["schema_conn_1"])
        
        assert result["jobId"] == "job_123"
        assert result["status"] == "triggered"
        assert result["databaseId"] == "db_123"

    @patch("src.repositories.collibra_repository.requests.Session")
    def test_trigger_metadata_sync_conflict(self, mock_session_class, mock_config):
        """Test handling of 409 conflict (sync already in progress)."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        
        # Mock 409 response
        mock_error_response = Mock()
        mock_error_response.status_code = 409
        mock_error_response.json.return_value = {
            "errorCode": "assetAlreadyInProcess",
            "userMessage": "Sync already being processed",
        }
        mock_error_response.text = "Conflict"
        
        http_error = requests.exceptions.HTTPError(response=mock_error_response)
        mock_session.request.side_effect = http_error
        
        repo = CollibraRepository(config=mock_config)
        response = repo._make_request("POST", "/test")
        
        # Should return mock response indicating success
        assert response.status_code == 200
        assert response.json()["status"] == "already_running"

    @patch("src.repositories.collibra_repository.requests.Session")
    def test_get_job_status_success(self, mock_session_class, mock_config):
        """Test successful job status retrieval."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        
        mock_response = Mock()
        mock_response.json.return_value = {
            "id": "job_123",
            "status": "COMPLETED",
        }
        mock_response.raise_for_status = Mock()
        mock_session.request.return_value = mock_response
        
        repo = CollibraRepository(config=mock_config)
        status = repo.get_job_status("job_123")
        
        assert status["status"] == "COMPLETED"

    @patch("src.repositories.collibra_repository.requests.Session")
    def test_get_job_status_not_found(self, mock_session_class, mock_config):
        """Test job status when endpoint returns 404."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        
        # Mock 404 responses for all endpoints
        mock_error_response = Mock()
        mock_error_response.status_code = 404
        http_error = requests.exceptions.HTTPError(response=mock_error_response)
        mock_session.request.side_effect = http_error
        
        repo = CollibraRepository(config=mock_config)
        
        with pytest.raises(ConfigurationError):
            repo.get_job_status("job_123")

    @patch("src.repositories.collibra_repository.requests.Session")
    def test_make_request_server_error(self, mock_session_class, mock_config):
        """Test handling of server errors (5xx)."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        
        mock_error_response = Mock()
        mock_error_response.status_code = 500
        mock_error_response.text = "Internal Server Error"
        http_error = requests.exceptions.HTTPError(response=mock_error_response)
        mock_session.request.side_effect = http_error
        
        repo = CollibraRepository(config=mock_config)
        
        with pytest.raises(RetryableError):
            repo._make_request("GET", "/test")

    @patch("src.repositories.collibra_repository.requests.Session")
    def test_make_request_client_error(self, mock_session_class, mock_config):
        """Test handling of client errors (4xx)."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        
        mock_error_response = Mock()
        mock_error_response.status_code = 400
        mock_error_response.text = "Bad Request"
        http_error = requests.exceptions.HTTPError(response=mock_error_response)
        mock_session.request.side_effect = http_error
        
        repo = CollibraRepository(config=mock_config)
        
        with pytest.raises(NonRetryableError):
            repo._make_request("GET", "/test")
