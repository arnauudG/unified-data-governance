"""
Unit tests for configuration module.
"""

import pytest
from pathlib import Path
from unittest.mock import patch, Mock

from src.core.config import Config, get_config, reset_config
from src.core.exceptions import ConfigurationError


class TestConfig:
    """Test cases for Config class."""

    def test_config_creation(self, mock_config):
        """Test that config can be created."""
        assert mock_config is not None

    def test_config_singleton(self):
        """Test that get_config returns singleton instance."""
        reset_config()
        config1 = get_config()
        config2 = get_config()
        assert config1 is config2

    def test_reset_config(self):
        """Test that reset_config clears singleton."""
        config1 = get_config()
        reset_config()
        config2 = get_config()
        assert config1 is not config2

    @patch.dict("os.environ", {
        "SODA_CLOUD_HOST": "https://test.soda.io",
        "SODA_CLOUD_API_KEY_ID": "test_key",
        "SODA_CLOUD_API_KEY_SECRET": "test_secret",
    })
    def test_config_loads_from_env(self):
        """Test that config loads from environment variables."""
        reset_config()
        config = get_config()
        assert config.soda_cloud.host == "https://test.soda.io"
        assert config.soda_cloud.api_key_id == "test_key"
        assert config.soda_cloud.api_key_secret == "test_secret"
