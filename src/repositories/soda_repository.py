"""
Soda Cloud API Repository.

This repository provides a clean interface for accessing Soda Cloud API,
abstracting away the details of HTTP requests and error handling.
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
from src.core.retry import retry_with_backoff, RetryConfig

if TYPE_CHECKING:
    from src.core.config import Config


class SodaRepository(BaseRepository):
    """Repository for accessing Soda Cloud API."""

    def __init__(self, config: Optional["Config"] = None):
        """
        Initialize Soda Cloud repository.

        Args:
            config: Optional Config instance. If None, uses get_config().
        """
        if config is None:
            config = get_config()
        super().__init__(config)

        self.base_url = config.soda_cloud.host
        self.api_key_id = config.soda_cloud.api_key_id
        self.api_key_secret = config.soda_cloud.api_key_secret
        self.session: Optional[requests.Session] = None

    def connect(self) -> None:
        """Establish connection to Soda Cloud API."""
        if self.session is None:
            self.session = requests.Session()
            self.session.auth = (self.api_key_id, self.api_key_secret)
            self.session.headers.update({
                "Content-Type": "application/json",
                "Accept": "application/json",
            })
            self.logger.info(f"Connected to Soda Cloud at {self.base_url}")

    def disconnect(self) -> None:
        """Close connection to Soda Cloud API."""
        if self.session:
            self.session.close()
            self.session = None
            self.logger.info("Disconnected from Soda Cloud")

    @retry_with_backoff(config=RetryConfig(max_attempts=3, initial_delay=5.0))
    def _make_request(
        self, method: str, endpoint: str, **kwargs: Any
    ) -> requests.Response:
        """
        Make HTTP request to Soda Cloud API with retry logic.

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint (relative to base URL)
            **kwargs: Additional arguments for requests

        Returns:
            Response object

        Raises:
            RetryableError: For retryable errors (429, 5xx)
            NonRetryableError: For non-retryable errors (401, 403, 4xx)
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

            if status_code == 401:
                raise NonRetryableError(
                    "Unauthorized access to Soda Cloud API",
                    status_code=status_code,
                    response_body=response_text,
                    details={"endpoint": endpoint, "method": method},
                    cause=e,
                )
            elif status_code == 403:
                raise NonRetryableError(
                    "Forbidden access to Soda Cloud API",
                    status_code=status_code,
                    response_body=response_text,
                    details={"endpoint": endpoint, "method": method},
                    cause=e,
                )
            elif status_code == 429:
                raise RetryableError(
                    "Rate limit exceeded",
                    status_code=status_code,
                    response_body=response_text,
                    details={"endpoint": endpoint, "method": method},
                    cause=e,
                )
            elif status_code and 500 <= status_code < 600:
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

    def get_datasets(self, page: int = 0, size: int = 100) -> Dict[str, Any]:
        """
        Get datasets from Soda Cloud API.

        Args:
            page: Page number (0-indexed)
            size: Page size

        Returns:
            Dictionary containing datasets and pagination info
        """
        endpoint = f"/api/v1/datasets?page={page}&size={size}"
        response = self._make_request("GET", endpoint)
        return response.json()

    def get_all_datasets(self) -> List[Dict[str, Any]]:
        """
        Get all datasets from Soda Cloud API (paginated).

        Returns:
            List of all datasets
        """
        all_datasets: List[Dict[str, Any]] = []

        # Get first page to determine total pages
        first_page = self.get_datasets(page=0)
        total_pages = first_page.get("totalPages", 0)
        all_datasets.extend(first_page.get("content", []))

        self.logger.info(f"Found {total_pages} pages of datasets")

        # Fetch remaining pages
        for page in range(1, total_pages):
            page_data = self.get_datasets(page=page)
            all_datasets.extend(page_data.get("content", []))
            self.logger.info(f"Fetched page {page}/{total_pages}")
            time.sleep(0.5)  # Be respectful to the API

        self.logger.info(f"Total datasets fetched: {len(all_datasets)}")
        return all_datasets

    def get_checks(self, page: int = 0, size: int = 100) -> Dict[str, Any]:
        """
        Get checks from Soda Cloud API.

        Args:
            page: Page number (0-indexed)
            size: Page size

        Returns:
            Dictionary containing checks and pagination info
        """
        endpoint = f"/api/v1/checks?page={page}&size={size}"
        response = self._make_request("GET", endpoint)
        return response.json()

    def get_all_checks(self) -> List[Dict[str, Any]]:
        """
        Get all checks from Soda Cloud API (paginated).

        Returns:
            List of all checks
        """
        all_checks: List[Dict[str, Any]] = []

        # Get first page to determine total pages
        first_page = self.get_checks(page=0, size=100)
        total_pages = first_page.get("totalPages", 0)
        all_checks.extend(first_page.get("content", []))

        self.logger.info(f"Found {total_pages} pages of checks")

        # Fetch remaining pages
        for page in range(1, total_pages):
            page_data = self.get_checks(page=page, size=100)
            all_checks.extend(page_data.get("content", []))
            self.logger.info(f"Fetched page {page}/{total_pages}")
            time.sleep(0.5)  # Be respectful to the API

        self.logger.info(f"Total checks fetched: {len(all_checks)}")
        return all_checks

    def get_check(self, check_id: str) -> Dict[str, Any]:
        """
        Get a specific check by ID.

        Args:
            check_id: Check ID

        Returns:
            Check data
        """
        endpoint = f"/api/v1/checks/{check_id}"
        response = self._make_request("GET", endpoint)
        return response.json()

    def get_dataset(self, dataset_id: str) -> Dict[str, Any]:
        """
        Get a specific dataset by ID.

        Args:
            dataset_id: Dataset ID

        Returns:
            Dataset data
        """
        endpoint = f"/api/v1/datasets/{dataset_id}"
        response = self._make_request("GET", endpoint)
        return response.json()
