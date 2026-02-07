"""
Centralized configuration management for the unified data governance platform.

This module provides a singleton configuration object that loads and validates
environment variables and configuration files.
"""

import os
from pathlib import Path
from typing import Optional, Dict, Any
from functools import lru_cache
from dotenv import load_dotenv
from pydantic import BaseModel, Field, field_validator

# Try to import pydantic-settings, fallback to pydantic BaseSettings
try:
    from pydantic_settings import BaseSettings, SettingsConfigDict
except ImportError:
    try:
        from pydantic import BaseSettings
        SettingsConfigDict = dict
    except ImportError:
        # If neither works, create a minimal BaseSettings
        class BaseSettings(BaseModel):
            pass
        SettingsConfigDict = dict

from typing import TYPE_CHECKING

from src.core.exceptions import ConfigurationError

if TYPE_CHECKING:
    from typing import TypeAlias

    # Type alias for Config to avoid circular imports
    ConfigType: TypeAlias = "Config"


class SnowflakeConfig(BaseModel):
    """Snowflake connection configuration."""

    account: str = Field(..., description="Snowflake account identifier")
    user: str = Field(..., description="Snowflake username")
    password: str = Field(..., description="Snowflake password")
    warehouse: str = Field(default="COMPUTE_WH", description="Snowflake warehouse")
    database: str = Field(
        default="DATA PLATFORM XYZ", description="Snowflake database name"
    )
    schema_name: str = Field(default="RAW", description="Snowflake schema", alias="schema")
    role: Optional[str] = Field(default=None, description="Snowflake role")


class SodaCloudConfig(BaseModel):
    """Soda Cloud API configuration."""

    host: str = Field(
        default="https://cloud.soda.io", description="Soda Cloud host URL"
    )
    api_key_id: str = Field(..., description="Soda Cloud API key ID")
    api_key_secret: str = Field(..., description="Soda Cloud API key secret")
    organization_id: Optional[str] = Field(
        default=None, description="Soda Cloud organization ID"
    )

    @field_validator("host")
    @classmethod
    def validate_host(cls, v: str) -> str:
        """Ensure host URL has proper scheme."""
        if not v.startswith(("http://", "https://")):
            return f"https://{v}"
        return v


class CollibraConfig(BaseModel):
    """Collibra API configuration."""

    base_url: str = Field(..., description="Collibra base URL")
    username: str = Field(..., description="Collibra username")
    password: str = Field(..., description="Collibra password")

    @field_validator("base_url")
    @classmethod
    def validate_base_url(cls, v: str) -> str:
        """Remove trailing slash from base URL."""
        return v.rstrip("/")


class PathsConfig(BaseModel):
    """Path configuration for the platform."""

    project_root: Path = Field(default_factory=lambda: Path.cwd())
    airflow_env_path: Path = Field(
        default=Path("/opt/airflow/.env"), description="Airflow .env file path"
    )
    collibra_config_path: Path = Field(
        default=Path("collibra/config.yml"), description="Collibra config file path"
    )

    def resolve_paths(self) -> "PathsConfig":
        """Resolve all paths relative to project root."""
        resolved = self.model_copy()
        resolved.project_root = resolved.project_root.resolve()
        if not resolved.airflow_env_path.is_absolute():
            resolved.airflow_env_path = (
                resolved.project_root / resolved.airflow_env_path
            )
        if not resolved.collibra_config_path.is_absolute():
            resolved.collibra_config_path = (
                resolved.project_root / resolved.collibra_config_path
            )
        return resolved


class Config(BaseSettings):
    """Main configuration class for the platform."""

    if hasattr(BaseSettings, 'model_config'):
        model_config = SettingsConfigDict(
            env_file=None,  # We'll handle env file loading manually
            env_nested_delimiter="__",
            case_sensitive=False,
            extra="ignore",
        )
    else:
        # Fallback for older pydantic
        class Config:
            env_file = None
            env_nested_delimiter = "__"
            case_sensitive = False
            extra = "ignore"

    # Snowflake configuration
    snowflake: SnowflakeConfig

    # Soda Cloud configuration
    soda_cloud: SodaCloudConfig

    # Collibra configuration
    collibra: CollibraConfig

    # Paths configuration
    paths: PathsConfig = Field(default_factory=PathsConfig)

    @classmethod
    def load(cls, env_file: Optional[Path] = None) -> "Config":
        """
        Load configuration from environment variables and optional .env file.

        Args:
            env_file: Optional path to .env file. If None, will try common locations.

        Returns:
            Config instance

        Raises:
            ConfigurationError: If required configuration is missing or invalid
        """
        # Try to load .env file from common locations
        env_paths = [
            Path("/opt/airflow/.env"),  # Airflow Docker container
            Path(".env"),  # Current directory
            Path(__file__).parent.parent.parent / ".env",  # Project root
        ]

        if env_file:
            env_paths.insert(0, env_file)

        env_loaded = False
        for env_path in env_paths:
            if env_path.exists():
                load_dotenv(env_path, override=True)
                env_loaded = True
                break

        if not env_loaded:
            # Fallback to default dotenv behavior
            load_dotenv(override=True)

        try:
            # Build configuration from environment variables
            config = cls(
                snowflake=SnowflakeConfig(
                    account=os.getenv("SNOWFLAKE_ACCOUNT", ""),
                    user=os.getenv("SNOWFLAKE_USER", ""),
                    password=os.getenv("SNOWFLAKE_PASSWORD", ""),
                    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
                    database=os.getenv(
                        "SNOWFLAKE_DATABASE", "DATA PLATFORM XYZ"
                    ),
                    schema_name=os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
                    role=os.getenv("SNOWFLAKE_ROLE"),
                ),
                soda_cloud=SodaCloudConfig(
                    host=os.getenv("SODA_CLOUD_HOST", "https://cloud.soda.io"),
                    api_key_id=os.getenv("SODA_CLOUD_API_KEY_ID", ""),
                    api_key_secret=os.getenv("SODA_CLOUD_API_KEY_SECRET", ""),
                    organization_id=os.getenv("SODA_CLOUD_ORGANIZATION_ID"),
                ),
                collibra=CollibraConfig(
                    base_url=os.getenv("COLLIBRA_BASE_URL", ""),
                    username=os.getenv("COLLIBRA_USERNAME", ""),
                    password=os.getenv("COLLIBRA_PASSWORD", ""),
                ),
                paths=PathsConfig(),
            )

            # Resolve paths
            config.paths = config.paths.resolve_paths()

            # Validate required fields
            _validate_config(config)

            return config

        except Exception as e:
            raise ConfigurationError(
                f"Failed to load configuration: {e}",
                details={"error_type": type(e).__name__},
                cause=e if isinstance(e, Exception) else None,
            ) from e

    def get_data_source_name(self, layer: str) -> str:
        """
        Get Soda data source name for a given layer.

        Args:
            layer: Layer name (raw, staging, mart, quality)

        Returns:
            Data source name (e.g., "data_platform_xyz_raw")
        """
        database_name = self.snowflake.database.lower().replace(" ", "_").replace("-", "_")
        return f"{database_name}_{layer}"

    def get_all_data_source_names(self) -> Dict[str, str]:
        """
        Get all data source names for all layers.

        Returns:
            Dictionary mapping layer names to data source names
        """
        layers = ["raw", "staging", "mart", "quality"]
        return {layer: self.get_data_source_name(layer) for layer in layers}


def _validate_config(config: Config) -> None:
    """
    Validate that required configuration is present.

    Args:
        config: Config instance to validate

    Raises:
        ConfigurationError: If required configuration is missing
    """
    errors = []

    # Validate Snowflake config
    if not config.snowflake.account:
        errors.append("SNOWFLAKE_ACCOUNT is required")
    if not config.snowflake.user:
        errors.append("SNOWFLAKE_USER is required")
    if not config.snowflake.password:
        errors.append("SNOWFLAKE_PASSWORD is required")

    # Validate Soda Cloud config
    if not config.soda_cloud.api_key_id:
        errors.append("SODA_CLOUD_API_KEY_ID is required")
    if not config.soda_cloud.api_key_secret:
        errors.append("SODA_CLOUD_API_KEY_SECRET is required")

    # Validate Collibra config
    if not config.collibra.base_url:
        errors.append("COLLIBRA_BASE_URL is required")
    if not config.collibra.username:
        errors.append("COLLIBRA_USERNAME is required")
    if not config.collibra.password:
        errors.append("COLLIBRA_PASSWORD is required")

    if errors:
        raise ConfigurationError(
            "Missing required configuration",
            details={"missing_fields": errors},
        )


# Singleton instance
_config: Optional[Config] = None


@lru_cache(maxsize=1)
def get_config(env_file: Optional[Path] = None) -> Config:
    """
    Get the global configuration instance (singleton pattern).

    Args:
        env_file: Optional path to .env file

    Returns:
        Config instance
    """
    global _config
    if _config is None:
        _config = Config.load(env_file=env_file)
    return _config


def reset_config() -> None:
    """Reset the global configuration (useful for testing)."""
    global _config
    _config = None
    get_config.cache_clear()
