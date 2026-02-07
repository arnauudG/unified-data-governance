"""
Retry utilities with exponential backoff for the unified data governance platform.

This module provides retry decorators and utilities for handling transient failures
in API calls and other operations.
"""

import time
import logging
from typing import Callable, TypeVar, ParamSpec, Optional, Type, Union
from functools import wraps
from dataclasses import dataclass

from src.core.exceptions import (
    RetryableError,
    NonRetryableError,
    APIError,
    DataGovernanceError,
)

logger = logging.getLogger(__name__)

P = ParamSpec("P")
T = TypeVar("T")


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""

    max_attempts: int = 3
    initial_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True
    retryable_exceptions: tuple[Type[Exception], ...] = (
        RetryableError,
        ConnectionError,
        TimeoutError,
    )

    def calculate_delay(self, attempt: int) -> float:
        """
        Calculate delay for a given attempt using exponential backoff.

        Args:
            attempt: Current attempt number (0-indexed)

        Returns:
            Delay in seconds
        """
        delay = self.initial_delay * (self.exponential_base ** attempt)
        delay = min(delay, self.max_delay)

        if self.jitter:
            import random

            # Add jitter: random value between 0 and 20% of delay
            jitter_amount = delay * 0.2 * random.random()
            delay += jitter_amount

        return delay


def retry_with_backoff(
    config: Optional[RetryConfig] = None,
    on_retry: Optional[Callable[[Exception, int], None]] = None,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """
    Decorator to retry a function with exponential backoff.

    Args:
        config: Retry configuration. If None, uses default config.
        on_retry: Optional callback function called on each retry.
                  Receives (exception, attempt_number) as arguments.

    Returns:
        Decorated function

    Example::

        @retry_with_backoff(config=RetryConfig(max_attempts=5))
        def api_call():
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
    """

    if config is None:
        config = RetryConfig()

    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            last_exception: Optional[Exception] = None

            for attempt in range(config.max_attempts):
                try:
                    return func(*args, **kwargs)
                except config.retryable_exceptions as e:
                    last_exception = e

                    if attempt < config.max_attempts - 1:
                        delay = config.calculate_delay(attempt)
                        logger.warning(
                            f"Retryable error in {func.__name__} (attempt {attempt + 1}/{config.max_attempts}): {e}. "
                            f"Retrying in {delay:.2f}s..."
                        )

                        if on_retry:
                            try:
                                on_retry(e, attempt + 1)
                            except Exception as callback_error:
                                logger.error(
                                    f"Error in retry callback: {callback_error}"
                                )

                        time.sleep(delay)
                    else:
                        logger.error(
                            f"Max retries ({config.max_attempts}) exceeded for {func.__name__}"
                        )
                        raise
                except NonRetryableError as e:
                    logger.error(
                        f"Non-retryable error in {func.__name__}: {e}. Not retrying."
                    )
                    raise
                except Exception as e:
                    # Check if it's a retryable exception type
                    if isinstance(e, config.retryable_exceptions):
                        last_exception = e
                        if attempt < config.max_attempts - 1:
                            delay = config.calculate_delay(attempt)
                            logger.warning(
                                f"Retryable error in {func.__name__} (attempt {attempt + 1}/{config.max_attempts}): {e}. "
                                f"Retrying in {delay:.2f}s..."
                            )
                            if on_retry:
                                try:
                                    on_retry(e, attempt + 1)
                                except Exception as callback_error:
                                    logger.error(
                                        f"Error in retry callback: {callback_error}"
                                    )
                            time.sleep(delay)
                        else:
                            logger.error(
                                f"Max retries ({config.max_attempts}) exceeded for {func.__name__}"
                            )
                            raise
                    else:
                        # Non-retryable exception
                        logger.error(
                            f"Non-retryable error in {func.__name__}: {e}. Not retrying."
                        )
                        raise

            # Should never reach here, but just in case
            if last_exception:
                raise last_exception
            raise RuntimeError(f"Unexpected error in {func.__name__}")

        return wrapper

    return decorator


def is_retryable_error(exception: Exception) -> bool:
    """
    Check if an exception is retryable.

    Args:
        exception: Exception to check

    Returns:
        True if exception is retryable, False otherwise
    """
    retryable_types = (
        RetryableError,
        ConnectionError,
        TimeoutError,
    )

    # Check exception type
    if isinstance(exception, retryable_types):
        return True

    # Check for specific error messages/codes
    if isinstance(exception, APIError):
        # Rate limiting (429) is retryable
        if exception.status_code == 429:
            return True
        # Server errors (5xx) are retryable
        if exception.status_code and 500 <= exception.status_code < 600:
            return True
        # Client errors (4xx) except 429 are not retryable
        if exception.status_code and 400 <= exception.status_code < 500:
            return False

    return False
