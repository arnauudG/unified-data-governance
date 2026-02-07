"""
Constants used throughout the unified data governance platform.

This module centralizes all magic strings, numbers, and configuration values
to improve maintainability and reduce errors.
"""

from typing import List, Dict


class DataLayers:
    """Data layer names used throughout the platform."""

    RAW = "raw"
    STAGING = "staging"
    MART = "mart"
    QUALITY = "quality"

    ALL: List[str] = [RAW, STAGING, MART, QUALITY]

    @classmethod
    def is_valid(cls, layer: str) -> bool:
        """Check if a layer name is valid."""
        return layer.lower() in cls.ALL


class ExpectedDatasets:
    """Expected datasets per layer based on check files."""

    RAW: List[str] = ["CUSTOMERS", "PRODUCTS", "ORDERS", "ORDER_ITEMS"]
    STAGING: List[str] = [
        "STG_CUSTOMERS",
        "STG_PRODUCTS",
        "STG_ORDERS",
        "STG_ORDER_ITEMS",
    ]
    MART: List[str] = ["DIM_CUSTOMERS", "DIM_PRODUCTS", "FACT_ORDERS"]
    QUALITY: List[str] = ["CHECK_RESULTS"]

    LAYER_DATASETS: Dict[str, List[str]] = {
        DataLayers.RAW: RAW,
        DataLayers.STAGING: STAGING,
        DataLayers.MART: MART,
        DataLayers.QUALITY: QUALITY,
    }

    @classmethod
    def get_for_layer(cls, layer: str) -> List[str]:
        """Get expected datasets for a layer."""
        return cls.LAYER_DATASETS.get(layer.lower(), [])


class APIEndpoints:
    """API endpoint paths for external services."""

    # Soda Cloud API endpoints
    SODA_DATASETS = "/api/v1/datasets"
    SODA_CHECKS = "/api/v1/checks"
    SODA_DATASET_BY_ID = "/api/v1/datasets/{dataset_id}"
    SODA_CHECK_BY_ID = "/api/v1/checks/{check_id}"

    # Collibra API endpoints
    COLLIBRA_DATABASES = "/rest/catalogDatabase/v1/databases"
    COLLIBRA_DATABASE_BY_ID = "/rest/catalogDatabase/v1/databases/{database_id}"
    COLLIBRA_SCHEMA_CONNECTIONS = "/rest/catalogDatabase/v1/schemaConnections"
    COLLIBRA_SYNC_METADATA = (
        "/rest/catalogDatabase/v1/databases/{database_id}/synchronizeMetadata"
    )
    COLLIBRA_JOBS = "/rest/jobs/{job_id}"
    COLLIBRA_JOB = "/rest/job/{job_id}"
    COLLIBRA_CATALOG_JOBS = "/rest/catalogDatabase/v1/jobs/{job_id}"


class HTTPStatusCodes:
    """HTTP status code constants."""

    OK = 200
    UNAUTHORIZED = 401
    FORBIDDEN = 403
    NOT_FOUND = 404
    CONFLICT = 409
    RATE_LIMIT = 429
    INTERNAL_SERVER_ERROR = 500
    BAD_GATEWAY = 502
    SERVICE_UNAVAILABLE = 503

    RETRYABLE_CODES: List[int] = [429, 500, 502, 503]
    NON_RETRYABLE_CODES: List[int] = [401, 403, 404]


class RetryConfigDefaults:
    """Default retry configuration values."""

    MAX_ATTEMPTS = 3
    INITIAL_DELAY = 1.0
    MAX_DELAY = 60.0
    EXPONENTIAL_BASE = 2.0
    POLL_INTERVAL = 10  # For job status polling
    MAX_WAIT_TIME = 3600  # 1 hour for job completion


class FilePatterns:
    """File patterns for data organization."""

    DATASETS_LATEST = "datasets_latest.csv"
    CHECKS_LATEST = "checks_latest.csv"
    ANALYSIS_SUMMARY = "analysis_summary.csv"
    DATASETS_PATTERN = "datasets_*.csv"
    CHECKS_PATTERN = "checks_*.csv"
    SUMMARY_REPORT_PATTERN = "summary_report_*.txt"

    FILES_TO_KEEP: List[str] = [DATASETS_LATEST, CHECKS_LATEST, ANALYSIS_SUMMARY]
    PATTERNS_TO_REMOVE: List[str] = [
        DATASETS_PATTERN,
        CHECKS_PATTERN,
        SUMMARY_REPORT_PATTERN,
    ]


class DatabaseDefaults:
    """Default database configuration values."""

    SNOWFLAKE_DATABASE = "DATA PLATFORM XYZ"
    SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
    SNOWFLAKE_SCHEMA_RAW = "RAW"
    SNOWFLAKE_SCHEMA_STAGING = "STAGING"
    SNOWFLAKE_SCHEMA_MART = "MART"
    SNOWFLAKE_SCHEMA_QUALITY = "QUALITY"



class Timeouts:
    """Timeout values in seconds."""

    API_REQUEST = 30
    DATABASE_CONNECTION = 10
    JOB_POLLING = 10
    MAX_JOB_WAIT = 3600  # 1 hour


class PaginationDefaults:
    """Default pagination values."""

    SODA_DATASETS_PAGE_SIZE = 100
    SODA_CHECKS_PAGE_SIZE = 100
    COLLIBRA_SCHEMA_CONNECTIONS_LIMIT = 500
    COLLIBRA_SCHEMA_CONNECTIONS_OFFSET = 0
