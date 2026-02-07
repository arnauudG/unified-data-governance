"""
Common error handling utilities.

This module provides reusable error handling functions to ensure
consistent error handling across the platform.
"""

import requests
from typing import Dict, Any, Optional

from src.core.exceptions import (
    APIError,
    RetryableError,
    NonRetryableError,
    ConnectionError,
)
from src.core.constants import HTTPStatusCodes


def classify_error(
    status_code: Optional[int], response_text: Optional[str] = None
) -> type[APIError]:
    """
    Classify an HTTP error as retryable or non-retryable.

    Args:
        status_code: HTTP status code
        response_text: Optional response text

    Returns:
        Exception class to raise
    """
    if status_code is None:
        return ConnectionError

    if status_code in HTTPStatusCodes.RETRYABLE_CODES:
        return RetryableError
    elif status_code in HTTPStatusCodes.NON_RETRYABLE_CODES:
        return NonRetryableError
    else:
        return NonRetryableError  # Default to non-retryable


def handle_api_error(
    exception: requests.exceptions.RequestException,
    endpoint: str,
    method: str,
    context: Optional[Dict[str, Any]] = None,
) -> APIError:
    """
    Handle API errors consistently.

    Args:
        exception: The requests exception
        endpoint: API endpoint that failed
        method: HTTP method used
        context: Additional context

    Returns:
        Appropriate APIError exception
    """
    context = context or {}
    context.update({"endpoint": endpoint, "method": method})

    if isinstance(exception, requests.exceptions.HTTPError):
        status_code = exception.response.status_code if exception.response else None
        response_text = exception.response.text if exception.response else None

        error_class = classify_error(status_code, response_text)

        # Special handling for specific status codes
        if status_code == HTTPStatusCodes.UNAUTHORIZED:
            message = "Unauthorized access. Please check your API credentials."
        elif status_code == HTTPStatusCodes.FORBIDDEN:
            message = "Forbidden access. Please check your permissions."
        elif status_code == HTTPStatusCodes.RATE_LIMIT:
            message = "Rate limit exceeded. Please retry after delay."
        elif status_code == HTTPStatusCodes.NOT_FOUND:
            message = f"Resource not found at {endpoint}"
        else:
            message = f"HTTP error {status_code}"

        return error_class(
            message,
            status_code=status_code,
            response_body=response_text,
            details=context,
            cause=exception,
        )

    elif isinstance(exception, requests.exceptions.ConnectionError):
        return ConnectionError(
            f"Connection error: {exception}",
            details=context,
            cause=exception,
        )

    elif isinstance(exception, requests.exceptions.Timeout):
        return RetryableError(
            f"Request timeout: {exception}",
            details=context,
            cause=exception,
        )

    else:
        return RetryableError(
            f"Request failed: {exception}",
            details=context,
            cause=exception,
        )
