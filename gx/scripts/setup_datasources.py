#!/usr/bin/env python3
"""
Set up Great Expectations datasources with Snowflake connections.
This script configures datasources using environment variables, similar to Soda configuration.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
PROJECT_ROOT = Path(__file__).parent.parent.parent
env_file = PROJECT_ROOT / ".env"
if env_file.exists():
    load_dotenv(env_file)

GX_ROOT = Path(__file__).parent.parent
GX_DIR = GX_ROOT / "great_expectations"

def get_snowflake_connection_string(schema: str) -> str:
    """Build Snowflake connection string from environment variables."""
    account = os.getenv('SNOWFLAKE_ACCOUNT', '').replace('.snowflakecomputing.com', '')
    user = os.getenv('SNOWFLAKE_USER', '')
    password = os.getenv('SNOWFLAKE_PASSWORD', '')
    database = os.getenv('SNOWFLAKE_DATABASE', 'DATA_GOVERNANCE_PLATFORM')
    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
    role = os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN')
    
    # URL encode password
    import urllib.parse
    password_encoded = urllib.parse.quote_plus(password)
    
    connection_string = (
        f"snowflake://{user}:{password_encoded}@{account}/"
        f"{database}/{schema}?warehouse={warehouse}&role={role}"
    )
    
    return connection_string

def setup_datasource_via_gx_cli(layer: str, schema: str):
    """Set up datasource using GX CLI (interactive)."""
    try:
        import great_expectations as gx
        
        print(f"\n🔧 Setting up datasource for {layer} layer (schema: {schema})...")
        
        # Initialize context
        context = gx.get_context(context_root_dir=str(GX_DIR))
        
        # Create datasource configuration
        connection_string = get_snowflake_connection_string(schema)
        
        datasource_config = {
            "name": f"{layer}_datasource",
            "class_name": "Datasource",
            "execution_engine": {
                "class_name": "SqlAlchemyExecutionEngine",
                "connection_string": connection_string
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
        
        # Add datasource to context
        context.add_datasource(**datasource_config)
        
        print(f"✅ Datasource '{layer}_datasource' configured successfully")
        return True
        
    except ImportError:
        print("❌ Great Expectations not installed. Install with: pip install great-expectations")
        return False
    except Exception as e:
        print(f"❌ Error setting up datasource: {e}")
        return False

def main():
    """Set up all datasources."""
    layers = {
        "raw": "RAW",
        "staging": "STAGING",
        "mart": "MART",
        "quality": "QUALITY"
    }
    
    print("🚀 Setting up Great Expectations datasources...")
    print(f"GX directory: {GX_DIR}")
    
    # Check environment variables
    required_vars = ['SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD', 'SNOWFLAKE_DATABASE']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"⚠️  Missing environment variables: {', '.join(missing_vars)}")
        print("   Please set them in your .env file or environment")
        return 1
    
    results = []
    for layer, schema in layers.items():
        success = setup_datasource_via_gx_cli(layer, schema)
        results.append(success)
    
    if all(results):
        print("\n✅ All datasources configured successfully!")
        return 0
    else:
        print("\n⚠️  Some datasources failed to configure")
        return 1

if __name__ == "__main__":
    sys.exit(main())
