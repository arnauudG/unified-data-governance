#!/usr/bin/env python3
"""
Soda Configuration Helpers

Helper functions to derive data source names and other configuration values
from environment variables, particularly the database name.

This module has been refactored to use the centralized configuration system.
"""

from typing import Dict
import sys
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.core.config import get_config
from src.core.logging import get_logger

logger = get_logger(__name__)


def get_database_name() -> str:
    """
    Get the database name from configuration.
    
    Returns:
        str: Database name (default: DATA PLATFORM XYZ)
    """
    try:
        config = get_config()
        return config.snowflake.database
    except Exception as e:
        logger.warning(f"Failed to load config, using default database name: {e}")
        return "DATA PLATFORM XYZ"


def database_to_data_source_name(database_name: str | None = None, layer: str | None = None) -> str:
    """
    Convert database name to data source name format.
    
    Converts database name (e.g., "DATA PLATFORM XYZ") to lowercase
    with underscores and appends the layer name.
    
    Args:
        database_name: Database name (if None, reads from configuration)
        layer: Layer name (raw, staging, mart, quality)
    
    Returns:
        str: Data source name (e.g., "data_platform_xyz_raw")
    
    Examples:
        >>> database_to_data_source_name("DATA PLATFORM XYZ", "raw")
        'data_platform_xyz_raw'
        >>> database_to_data_source_name("MY_DB", "staging")
        'my_db_staging'
    """
    if database_name is None:
        database_name = get_database_name()
    
    # Convert to lowercase, replace spaces and hyphens with underscores
    data_source_base = database_name.lower().replace(' ', '_').replace('-', '_')
    
    if layer:
        return f"{data_source_base}_{layer}"
    else:
        return data_source_base


def get_data_source_name(layer: str) -> str:
    """
    Get the data source name for a given layer.
    
    Uses the centralized configuration system.
    
    Args:
        layer: Layer name (raw, staging, mart, quality)
    
    Returns:
        str: Data source name for the layer
    """
    try:
        config = get_config()
        return config.get_data_source_name(layer)
    except Exception as e:
        logger.warning(f"Failed to load config, falling back to manual calculation: {e}")
        return database_to_data_source_name(layer=layer)


def get_all_data_source_names() -> Dict[str, str]:
    """
    Get all data source names for all layers.
    
    Uses the centralized configuration system.
    
    Returns:
        dict: Dictionary mapping layer names to data source names
    """
    try:
        config = get_config()
        return config.get_all_data_source_names()
    except Exception as e:
        logger.warning(f"Failed to load config, falling back to manual calculation: {e}")
        layers = ['raw', 'staging', 'mart', 'quality']
        return {layer: get_data_source_name(layer) for layer in layers}


if __name__ == "__main__":
    # Test the functions
    print("Database name:", get_database_name())
    print("\nData source names:")
    for layer, name in get_all_data_source_names().items():
        print(f"  {layer}: {name}")
