#!/usr/bin/env python3
"""
Initialize Great Expectations project structure.
This script creates the GX project directory structure and base configuration.
"""

import os
import json
from pathlib import Path
from typing import Dict, Any

# Project root (parent of gx directory)
PROJECT_ROOT = Path(__file__).parent.parent.parent
GX_ROOT = Path(__file__).parent.parent
GX_DIR = GX_ROOT / "great_expectations"

def create_directory_structure():
    """Create the GX directory structure."""
    directories = [
        GX_DIR,
        GX_DIR / "datasources",
        GX_DIR / "expectations" / "raw",
        GX_DIR / "expectations" / "staging",
        GX_DIR / "expectations" / "mart",
        GX_DIR / "expectations" / "quality",
        GX_DIR / "checkpoints" / "raw",
        GX_DIR / "checkpoints" / "staging",
        GX_DIR / "checkpoints" / "mart",
        GX_DIR / "checkpoints" / "quality",
        GX_DIR / "validations",
        GX_DIR / "data_docs",
    ]
    
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
        print(f"✅ Created directory: {directory}")

def create_great_expectations_config():
    """Create the main great_expectations.yml configuration file."""
    config = {
        "config_version": 3.0,
        "datasources": {},
        "stores": {
            "expectations_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": "expectations/"
                }
            },
            "validations_store": {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": "validations/"
                }
            },
            "evaluation_parameter_store": {
                "class_name": "EvaluationParameterStore"
            },
            "checkpoint_store": {
                "class_name": "CheckpointStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": "checkpoints/"
                }
            }
        },
        "data_docs_sites": {
            "local_site": {
                "class_name": "SiteBuilder",
                "show_how_to_buttons": True,
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": "data_docs/"
                },
                "site_index_builder": {
                    "class_name": "DefaultSiteIndexBuilder"
                }
            }
        },
        "anonymous_usage_statistics": {
            "enabled": False
        },
        "notebooks": {
            "suite_edit": {
                "class_name": "SuiteEditNotebookRenderer"
            },
            "suite_new": {
                "class_name": "SuiteNewNotebookRenderer"
            },
            "validation_results": {
                "class_name": "ValidationResultsNotebookRenderer"
            }
        }
    }
    
    try:
        import yaml
        config_path = GX_DIR / "great_expectations.yml"
        with open(config_path, 'w') as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)
        print(f"✅ Created: {config_path}")
    except ImportError:
        print("⚠️  PyYAML not installed. Skipping config file creation.")
        print("   Install with: pip install pyyaml")

def create_datasource_config(layer: str, schema: str) -> Dict[str, Any]:
    """Create a datasource configuration for a layer."""
    # Note: This is a template - actual connection strings will be set up via GX CLI
    return {
        "name": f"{layer}_datasource",
        "class_name": "Datasource",
        "execution_engine": {
            "class_name": "SqlAlchemyExecutionEngine",
            "connection_string": "snowflake://${SNOWFLAKE_USER}:${SNOWFLAKE_PASSWORD}@${SNOWFLAKE_ACCOUNT}/${SNOWFLAKE_DATABASE}/{schema}?warehouse=${SNOWFLAKE_WAREHOUSE}&role=${SNOWFLAKE_ROLE}"
        },
        "data_connectors": {
            "default_runtime_data_connector": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["default_identifier_name"]
            },
            "default_inferred_data_connector": {
                "class_name": "InferredAssetSqlDataConnector",
                "include_schema_name": True,
                "schema_name": schema
            }
        }
    }

def create_datasource_files():
    """Create datasource configuration files for each layer."""
    layers = {
        "raw": "RAW",
        "staging": "STAGING",
        "mart": "MART",
        "quality": "QUALITY"
    }
    
    try:
        import yaml
        for layer, schema in layers.items():
            datasource_config = create_datasource_config(layer, schema)
            datasource_path = GX_DIR / "datasources" / f"{layer}_datasource.yml"
            
            with open(datasource_path, 'w') as f:
                yaml.dump({layer: datasource_config}, f, default_flow_style=False, sort_keys=False)
            
            print(f"✅ Created datasource template: {datasource_path}")
            print(f"   ⚠️  Note: Update connection string with actual Snowflake credentials")
    except ImportError:
        print("⚠️  PyYAML not installed. Skipping datasource file creation.")

def main():
    """Main setup function."""
    print("🚀 Setting up Great Expectations project...")
    print(f"Project root: {PROJECT_ROOT}")
    print(f"GX root: {GX_ROOT}")
    print()
    
    create_directory_structure()
    create_great_expectations_config()
    create_datasource_files()
    
    print("\n✅ Great Expectations project setup complete!")
    print("\nNext steps:")
    print("1. Install Great Expectations: pip install great-expectations")
    print("2. Create example expectations: python gx/scripts/create_example_expectations.py")
    print("3. Set up datasources using GX CLI: great_expectations datasource new")
    print("4. Run: python gx/scripts/run_gx_check.py --layer mart --table fact_orders")

if __name__ == "__main__":
    main()
