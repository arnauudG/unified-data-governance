"""
Pytest configuration and shared fixtures.

This module provides common fixtures and configuration for all tests.
"""

import pytest
from unittest.mock import Mock, MagicMock
from typing import Any, Optional
from pathlib import Path

from src.core.config import Config, reset_config


@pytest.fixture(autouse=True)
def reset_config_singleton():
    """Reset config singleton before and after each test."""
    reset_config()
    yield
    reset_config()


@pytest.fixture
def mock_config() -> Mock:
    """Provide a mock configuration object."""
    config = Mock(spec=Config)
    
    # Soda Cloud config
    config.soda_cloud.host = "https://cloud.soda.io"
    config.soda_cloud.api_key_id = "test_api_key_id"
    config.soda_cloud.api_key_secret = "test_api_key_secret"
    
    # Collibra config
    config.collibra.base_url = "https://test.collibra.com"
    config.collibra.username = "test_user"
    config.collibra.password = "test_password"
    
    # Snowflake config
    config.snowflake.account = "test_account"
    config.snowflake.user = "test_user"
    config.snowflake.password = "test_password"
    config.snowflake.database = "TEST_DB"
    config.snowflake.warehouse = "TEST_WH"
    config.snowflake.schema_name = "TEST_SCHEMA"
    
    # Paths config
    config.paths.collibra_config_path = Path("/tmp/test_collibra_config.yml")
    
    return config


@pytest.fixture
def mock_soda_response() -> dict:
    """Provide mock Soda Cloud API response."""
    return {
        "totalPages": 2,
        "totalElements": 150,
        "content": [
            {
                "id": "dataset_1",
                "name": "TEST_DATASET",
                "health": "pass",
            },
            {
                "id": "dataset_2",
                "name": "ANOTHER_DATASET",
                "health": "fail",
            },
        ],
    }


@pytest.fixture
def mock_check_response() -> dict:
    """Provide mock Soda Cloud check response."""
    return {
        "totalPages": 1,
        "totalElements": 50,
        "content": [
            {
                "id": "check_1",
                "name": "test_check",
                "evaluationStatus": "pass",
                "attributes": {"critical": True},
                "dataset": {"name": "TEST_DATASET"},
            },
            {
                "id": "check_2",
                "name": "another_check",
                "evaluationStatus": "fail",
                "attributes": {"critical": False},
                "dataset": {"name": "ANOTHER_DATASET"},
            },
        ],
    }


@pytest.fixture
def mock_collibra_response() -> dict:
    """Provide mock Collibra API response."""
    return {
        "id": "test_job_id",
        "status": "RUNNING",
        "progress": 50,
    }


@pytest.fixture
def temp_output_dir(tmp_path: Path) -> Path:
    """Provide a temporary output directory for tests."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    return output_dir


@pytest.fixture
def sample_datasets() -> list:
    """Provide sample dataset data."""
    return [
        {"id": "1", "name": "DATASET_1", "health": "pass"},
        {"id": "2", "name": "DATASET_2", "health": "fail"},
    ]


@pytest.fixture
def sample_checks() -> list:
    """Provide sample check data."""
    return [
        {
            "id": "1",
            "name": "check_1",
            "evaluationStatus": "pass",
            "attributes": {"critical": True},
            "dataset": {"name": "DATASET_1"},
        },
        {
            "id": "2",
            "name": "check_2",
            "evaluationStatus": "fail",
            "attributes": {"critical": False},
            "dataset": {"name": "DATASET_2"},
        },
    ]
