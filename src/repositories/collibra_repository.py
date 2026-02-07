"""
Collibra API Repository.

This repository provides a clean interface for accessing Collibra API,
abstracting away the details of HTTP requests and metadata synchronization.
"""

import time
import requests
from typing import List, Dict, Any, Optional, TYPE_CHECKING

from src.repositories.base import BaseRepository
from src.core.config import get_config
from src.core.exceptions import (
    APIError,
    RetryableError,
    NonRetryableError,
    ConfigurationError,
)

if TYPE_CHECKING:
    from src.core.config import Config


class CollibraRepository(BaseRepository):
    """Repository for accessing Collibra API."""

    def __init__(self, config: Optional["Config"] = None):
        """
        Initialize Collibra repository.

        Args:
            config: Optional Config instance. If None, uses get_config().
        """
        if config is None:
            config = get_config()
        super().__init__(config)

        self.base_url = config.collibra.base_url
        self.username = config.collibra.username
        self.password = config.collibra.password
        self.session: Optional[requests.Session] = None

    def connect(self) -> None:
        """Establish connection to Collibra API."""
        if self.session is None:
            self.session = requests.Session()
            self.session.auth = (self.username, self.password)
            self.session.headers.update({
                "Content-Type": "application/json",
                "Accept": "application/json",
            })
            self.logger.info(f"Connected to Collibra at {self.base_url}")

    def disconnect(self) -> None:
        """Close connection to Collibra API."""
        if self.session:
            self.session.close()
            self.session = None
            self.logger.info("Disconnected from Collibra")

    def _make_request(
        self, method: str, endpoint: str, **kwargs: Any
    ) -> requests.Response:
        """
        Make HTTP request to Collibra API.

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint (relative to base URL)
            **kwargs: Additional arguments for requests

        Returns:
            Response object

        Raises:
            RetryableError: For retryable errors (5xx)
            NonRetryableError: For non-retryable errors (4xx)
        """
        if self.session is None:
            self.connect()

        url = f"{self.base_url}{endpoint}"
        self.logger.debug(f"Making {method} request to {url}")

        try:
            response = self.session.request(method, url, timeout=30, **kwargs)
            response.raise_for_status()
            return response

        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response else None
            response_text = e.response.text if e.response else None

            # Handle 409 Conflict (sync already in progress)
            if status_code == 409:
                try:
                    error_data = e.response.json() if e.response else {}
                    error_code = error_data.get("errorCode", "")
                    user_message = error_data.get("userMessage", "")

                    if (
                        "already being processed" in user_message.lower()
                        or error_code == "assetAlreadyInProcess"
                    ):
                        self.logger.warning(
                            f"Metadata sync already in progress: {user_message}"
                        )
                        # Return a mock response indicating success
                        class MockResponse:
                            def __init__(self):
                                self.status_code = 200
                                self.json_data = {
                                    "status": "already_running",
                                    "message": "Sync already in progress",
                                }

                            def json(self):
                                return self.json_data

                        return MockResponse()

                except Exception:
                    pass

            if status_code and 500 <= status_code < 600:
                raise RetryableError(
                    f"Server error: {status_code}",
                    status_code=status_code,
                    response_body=response_text,
                    details={"endpoint": endpoint, "method": method},
                    cause=e,
                )
            else:
                raise NonRetryableError(
                    f"HTTP error: {status_code}",
                    status_code=status_code,
                    response_body=response_text,
                    details={"endpoint": endpoint, "method": method},
                    cause=e,
                )

        except requests.exceptions.RequestException as e:
            raise RetryableError(
                f"Request failed: {e}",
                details={"endpoint": endpoint, "method": method},
                cause=e,
            )

    def get_database(self, database_id: str) -> Dict[str, Any]:
        """
        Get database information from Collibra.

        Args:
            database_id: Database asset ID

        Returns:
            Database information including connection ID
        """
        endpoint = f"/rest/catalogDatabase/v1/databases/{database_id}"
        response = self._make_request("GET", endpoint)
        return response.json()

    def get_database_connection_id(self, database_id: str) -> str:
        """
        Get database connection ID from database asset ID.

        Args:
            database_id: Database asset ID

        Returns:
            Database connection ID

        Raises:
            ConfigurationError: If database connection not found
        """
        database = self.get_database(database_id)
        connection_id = database.get("databaseConnectionId")

        if not connection_id:
            raise ConfigurationError(
                f"Database asset {database_id} does not have a databaseConnectionId",
                details={"database_id": database_id},
            )

        return connection_id

    def list_schema_connections(
        self,
        database_connection_id: str,
        schema_id: Optional[str] = None,
        limit: int = 500,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """
        List schema connections for a database.

        Args:
            database_connection_id: Database connection ID
            schema_id: Optional schema asset ID to filter by
            limit: Maximum number of results
            offset: Offset for pagination

        Returns:
            List of schema connection dictionaries
        """
        endpoint = "/rest/catalogDatabase/v1/schemaConnections"
        params = {
            "databaseConnectionId": database_connection_id,
            "limit": limit,
            "offset": offset,
        }

        if schema_id:
            params["schemaId"] = schema_id

        response = self._make_request("GET", endpoint, params=params)
        result = response.json()
        return result.get("results", [])

    def resolve_schema_connection_ids(
        self, database_id: str, schema_asset_ids: List[str]
    ) -> List[str]:
        """
        Resolve schema asset IDs to schema connection IDs.

        Args:
            database_id: Database asset ID
            schema_asset_ids: List of schema asset IDs

        Returns:
            List of schema connection IDs

        Raises:
            ConfigurationError: If schema connections cannot be resolved
        """
        database_connection_id = self.get_database_connection_id(database_id)
        connection_ids = []

        for schema_asset_id in schema_asset_ids:
            connections = self.list_schema_connections(
                database_connection_id=database_connection_id,
                schema_id=schema_asset_id,
            )

            if not connections:
                raise ConfigurationError(
                    f"Could not find schema connection for schema asset {schema_asset_id}",
                    details={
                        "schema_asset_id": schema_asset_id,
                        "database_id": database_id,
                        "message": "Make sure the schema has been synchronized at least once.",
                    },
                )

            connection_id = connections[0].get("id")
            if not connection_id:
                raise ConfigurationError(
                    f"Schema connection for {schema_asset_id} has no ID",
                    details={"schema_asset_id": schema_asset_id},
                )

            connection_ids.append(connection_id)
            self.logger.info(
                f"Resolved schema asset {schema_asset_id} to connection {connection_id}"
            )

        return connection_ids

    def trigger_metadata_sync(
        self, database_id: str, schema_connection_ids: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Trigger metadata synchronization for a database.

        Args:
            database_id: Database asset ID
            schema_connection_ids: Optional list of schema connection IDs

        Returns:
            Dictionary containing job ID and sync details
        """
        endpoint = (
            f"/rest/catalogDatabase/v1/databases/{database_id}/synchronizeMetadata"
        )

        body = {}
        if schema_connection_ids:
            body["schemaConnectionIds"] = schema_connection_ids

        self.logger.info(f"Triggering metadata sync for database {database_id}")
        if schema_connection_ids:
            self.logger.info(f"Synchronizing schemas: {', '.join(schema_connection_ids)}")
        else:
            self.logger.info("Synchronizing all schemas with rules defined")

        response = self._make_request("POST", endpoint, json=body)
        result = response.json()

        job_id = result.get("jobId") or result.get("id")

        if job_id:
            self.logger.info(f"Metadata sync triggered successfully. Job ID: {job_id}")
        else:
            self.logger.warning(
                "Metadata sync triggered but no job ID returned. "
                "Sync may be running asynchronously."
            )

        return {
            "jobId": job_id,
            "databaseId": database_id,
            "schemaConnectionIds": schema_connection_ids or [],
            "status": "triggered" if job_id else "triggered_no_job_id",
            "response": result,
        }

    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """
        Get the status of a Collibra job.

        Args:
            job_id: Job ID

        Returns:
            Job status information
        """
        # Try different possible endpoints
        possible_endpoints = [
            f"/rest/jobs/{job_id}",
            f"/rest/job/{job_id}",
            f"/rest/catalogDatabase/v1/jobs/{job_id}",
        ]

        for endpoint in possible_endpoints:
            try:
                response = self._make_request("GET", endpoint)
                return response.json()
            except NonRetryableError as e:
                if e.status_code == 404:
                    continue
                raise

        raise ConfigurationError(
            f"Could not get job status for {job_id} from any known endpoint",
            details={"job_id": job_id},
        )
