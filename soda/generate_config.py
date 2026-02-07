#!/usr/bin/env python3
"""
Generate Soda Configuration Files

This script generates Soda configuration files with data source names
derived from the SNOWFLAKE_DATABASE environment variable.

Refactored to use centralized configuration and logging.

Usage:
    python3 soda/generate_config.py
"""

import sys
import re
from pathlib import Path
from typing import List

# Add project root to Python path for imports
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.core.config import get_config
from src.core.logging import get_logger
from src.core.exceptions import ConfigurationError
from soda.helpers import database_to_data_source_name, get_database_name

logger = get_logger(__name__)


def generate_config_file(layer: str, template_path: Path, output_path: Path) -> None:
    """
    Generate a Soda configuration file for a given layer.
    
    Args:
        layer: Layer name (raw, staging, mart, quality)
        template_path: Path to template file
        output_path: Path to output file
    
    Raises:
        ConfigurationError: If template file cannot be read or output cannot be written
    """
    try:
        database_name = get_database_name()
        data_source_name = database_to_data_source_name(database_name, layer)
        
        # Read template
        with open(template_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Replace data source name placeholder
        content = content.replace('{{DATA_SOURCE_NAME}}', data_source_name)
        
        # Write output
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        logger.info(f"Generated {output_path} with data source: {data_source_name}")
        
    except FileNotFoundError:
        raise ConfigurationError(
            f"Template file not found: {template_path}",
            details={"template_path": str(template_path), "layer": layer}
        )
    except Exception as e:
        raise ConfigurationError(
            f"Error generating config file for {layer}",
            details={"layer": layer, "output_path": str(output_path)},
            cause=e
        )


def main() -> None:
    """Generate all configuration files."""
    try:
        config_dir = Path(__file__).parent / 'configuration'
        template_dir = Path(__file__).parent / 'configuration_templates'
        
        # Create template directory if it doesn't exist
        template_dir.mkdir(exist_ok=True)
        
        layers: List[str] = ['raw', 'staging', 'mart', 'quality']
        
        for layer in layers:
            template_path = template_dir / f'configuration_{layer}.yml.template'
            output_path = config_dir / f'configuration_{layer}.yml'
            
            # If template exists, use it; otherwise use existing config as template
            if not template_path.exists():
                # Use existing config as base and update data source name
                if output_path.exists():
                    try:
                        with open(output_path, 'r', encoding='utf-8') as f:
                            content = f.read()
                        
                        # Find and replace the data source name
                        pattern = (
                            r'data_source\s+\w+_raw:|'
                            r'data_source\s+\w+_staging:|'
                            r'data_source\s+\w+_mart:|'
                            r'data_source\s+\w+_quality:'
                        )
                        database_name = get_database_name()
                        data_source_name = database_to_data_source_name(database_name, layer)
                        replacement = f'data_source {data_source_name}:'
                        
                        content = re.sub(pattern, replacement, content)
                        
                        with open(output_path, 'w', encoding='utf-8') as f:
                            f.write(content)
                        
                        logger.info(f"Updated {output_path} with data source: {data_source_name}")
                    except Exception as e:
                        logger.error(f"Error updating {output_path}: {e}", exc_info=True)
            else:
                generate_config_file(layer, template_path, output_path)
                
    except ConfigurationError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
