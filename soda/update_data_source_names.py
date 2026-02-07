#!/usr/bin/env python3
"""
Update Data Source Names in Soda Configuration Files

This script updates the data source names in all Soda configuration files
based on the SNOWFLAKE_DATABASE environment variable.

Refactored to use centralized configuration and logging.

Usage:
    python3 soda/update_data_source_names.py
"""

import re
import sys
from pathlib import Path
from typing import Dict

# Add project root to Python path for imports
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.core.config import get_config
from src.core.logging import setup_logging, get_logger
from src.core.exceptions import ConfigurationError
from soda.helpers import database_to_data_source_name, get_database_name

# Setup logging for standalone script execution
setup_logging(level="INFO", format_type="human")
logger = get_logger(__name__)


def update_config_file(config_path: Path, layer: str) -> bool:
    """
    Update data source name in a configuration file.
    
    Args:
        config_path: Path to configuration file
        layer: Layer name (raw, staging, mart, quality)
    
    Returns:
        True if update was successful, False otherwise
    """
    if not config_path.exists():
        logger.warning(f"Configuration file does not exist: {config_path}")
        return False
    
    try:
        database_name = get_database_name()
        new_data_source_name = database_to_data_source_name(database_name, layer)
        
        # Read the file
        with open(config_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Find the current data source name pattern
        # Pattern: data_source <name>: (handles names with spaces and underscores)
        # Match: data_source followed by whitespace, then capture everything up to the colon
        pattern = r'data_source\s+([^:]+):'
        match = re.search(pattern, content)
        
        if not match:
            logger.warning(f"Could not find data_source definition in {config_path}")
            return False
        
        old_data_source_name = match.group(1).strip()
        
        if old_data_source_name == new_data_source_name:
            logger.info(f"{config_path.name}: Already using '{new_data_source_name}'")
            return True
        
        # Replace the data source name (use more specific pattern to avoid replacing comments)
        # Match: data_source <name>: (not in comments)
        lines = content.split('\n')
        updated_lines = []
        replaced = False
        
        for line in lines:
            # Check if this line contains the data_source definition (not a comment)
            if not line.strip().startswith('#') and 'data_source' in line and ':' in line:
                if not replaced:
                    # Replace the data source name in this line
                    # Match: data_source <anything>:
                    line = re.sub(
                        r'data_source\s+[^:]+:',
                        f'data_source {new_data_source_name}:',
                        line,
                        count=1
                    )
                    replaced = True
            updated_lines.append(line)
        
        if not replaced:
            logger.warning(f"Could not replace data_source in {config_path}")
            return False
        
        # Write back
        content = '\n'.join(updated_lines)
        with open(config_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        logger.info(
            f"{config_path.name}: Updated '{old_data_source_name}' → '{new_data_source_name}'"
        )
        return True
        
    except Exception as e:
        logger.error(f"Error updating {config_path}: {e}", exc_info=True)
        return False


def main() -> None:
    """Update all configuration files."""
    try:
        config_dir = Path(__file__).parent / 'configuration'
        
        if not config_dir.exists():
            raise ConfigurationError(
                f"Configuration directory not found: {config_dir}",
                details={"config_dir": str(config_dir)}
            )
        
        database_name = get_database_name()
        logger.info(f"Database name: {database_name}")
        logger.info("Updating data source names in configuration files...")
        
        layers: Dict[str, Path] = {
            'raw': config_dir / 'configuration_raw.yml',
            'staging': config_dir / 'configuration_staging.yml',
            'mart': config_dir / 'configuration_mart.yml',
            'quality': config_dir / 'configuration_quality.yml',
        }
        
        updated_count = 0
        for layer, config_path in layers.items():
            if update_config_file(config_path, layer):
                updated_count += 1
        
        logger.info(f"Updated {updated_count}/{len(layers)} configuration files")
        logger.info("Data source names are now:")
        for layer in layers.keys():
            data_source_name = database_to_data_source_name(database_name, layer)
            logger.info(f"  {layer}: {data_source_name}")
            
    except ConfigurationError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
