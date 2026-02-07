"""
Centralized logging configuration for the unified data governance platform.

This module provides structured logging setup with support for both
development (human-readable) and production (JSON) formats.
"""

import logging
import sys
from typing import Optional, Dict, Any
from pathlib import Path
import json
from datetime import datetime


class StructuredFormatter(logging.Formatter):
    """JSON formatter for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_data: Dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        # Add extra fields
        if hasattr(record, "extra_fields"):
            log_data.update(record.extra_fields)

        return json.dumps(log_data)


class HumanReadableFormatter(logging.Formatter):
    """Human-readable formatter for development."""

    def __init__(self):
        """Initialize the formatter."""
        super().__init__(
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )


def setup_logging(
    level: str = "INFO",
    format_type: str = "human",
    log_file: Optional[Path] = None,
) -> None:
    """
    Set up logging configuration for the platform.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format_type: Format type - "human" for human-readable, "json" for structured
        log_file: Optional path to log file. If None, logs only to stdout/stderr
    """
    # Convert string level to logging level
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    # Clear any existing handlers
    root_logger = logging.getLogger()
    root_logger.handlers.clear()

    # Set root logger level
    root_logger.setLevel(numeric_level)

    # Choose formatter
    if format_type.lower() == "json":
        formatter = StructuredFormatter()
    else:
        formatter = HumanReadableFormatter()

    # Console handler (always add)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # File handler (if specified)
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(numeric_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    # Set levels for noisy third-party loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("snowflake").setLevel(logging.WARNING)
    logging.getLogger("snowflake.connector").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for a module.

    Args:
        name: Logger name (typically __name__)

    Returns:
        Logger instance
    """
    return logging.getLogger(name)


def add_correlation_id(logger: logging.Logger, correlation_id: str) -> None:
    """
    Add correlation ID to logger context.

    Args:
        logger: Logger instance
        correlation_id: Correlation ID to add
    """
    # Store correlation ID in logger context
    # This can be used by custom formatters
    logger.setLevel(logger.level)  # Trigger handler update
    for handler in logger.handlers:
        if isinstance(handler.formatter, StructuredFormatter):
            # Add correlation ID to extra fields
            if not hasattr(logger, "extra_fields"):
                logger.extra_fields = {}
            logger.extra_fields["correlation_id"] = correlation_id
