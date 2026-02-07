"""
Custom exception hierarchy for the unified data governance platform.

This module provides a structured exception hierarchy that allows for
better error handling and categorization.
"""

from typing import Optional


class DataGovernanceError(Exception):
    """Base exception for all data governance platform errors."""

    def __init__(
        self,
        message: str,
        details: Optional[dict] = None,
        cause: Optional[Exception] = None,
    ):
        """
        Initialize the exception.

        Args:
            message: Human-readable error message
            details: Optional dictionary with additional error details
            cause: Optional underlying exception that caused this error
        """
        super().__init__(message)
        self.message = message
        self.details = details or {}
        self.cause = cause

    def __str__(self) -> str:
        """Return a formatted error message."""
        base_msg = self.message
        if self.details:
            details_str = ", ".join(f"{k}={v}" for k, v in self.details.items())
            return f"{base_msg} ({details_str})"
        return base_msg


class ConfigurationError(DataGovernanceError):
    """Raised when there's a configuration error."""

    pass


class APIError(DataGovernanceError):
    """Base exception for API-related errors."""

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        response_body: Optional[str] = None,
        details: Optional[dict] = None,
        cause: Optional[Exception] = None,
    ):
        """
        Initialize the API error.

        Args:
            message: Human-readable error message
            status_code: HTTP status code (if applicable)
            response_body: Response body from the API (if applicable)
            details: Optional dictionary with additional error details
            cause: Optional underlying exception that caused this error
        """
        super().__init__(message, details=details, cause=cause)
        self.status_code = status_code
        self.response_body = response_body

    def __str__(self) -> str:
        """Return a formatted error message."""
        base_msg = super().__str__()
        if self.status_code:
            return f"{base_msg} [Status: {self.status_code}]"
        return base_msg


class RetryableError(APIError):
    """Raised when an API error is retryable (e.g., rate limiting, temporary failures)."""

    pass


class NonRetryableError(APIError):
    """Raised when an API error is not retryable (e.g., authentication, bad request)."""

    pass


class ValidationError(DataGovernanceError):
    """Raised when validation fails."""

    pass


class ConnectionError(DataGovernanceError):
    """Raised when a connection error occurs."""

    pass


class TimeoutError(DataGovernanceError):
    """Raised when an operation times out."""

    pass
