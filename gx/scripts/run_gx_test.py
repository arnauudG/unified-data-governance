#!/usr/bin/env python3
"""
Run GX checkpoint test - handles installation check and provides helpful errors.
"""

import os
import sys
import argparse
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
PROJECT_ROOT = Path(__file__).parent.parent.parent
env_file = PROJECT_ROOT / ".env"
if env_file.exists():
    load_dotenv(env_file)

GX_ROOT = Path(__file__).parent.parent
GX_DIR = GX_ROOT / "great_expectations"

def check_gx_installation():
    """Check if GX is installed."""
    try:
        # Try importing in a way that avoids segfaults
        import importlib
        gx_module = importlib.import_module("great_expectations")
        version = getattr(gx_module, "__version__", "unknown")
        print(f"✅ Great Expectations installed (version: {version})")
        return True, gx_module
    except ImportError:
        print("❌ Great Expectations not installed")
        print("\nTo install:")
        print("   pip install great-expectations")
        print("\nOr with Snowflake support:")
        print("   pip install great-expectations[snowflake]")
        return False, None
    except Exception as e:
        print(f"⚠️  Error importing Great Expectations: {e}")
        print("   This might be a Python version compatibility issue")
        print("   Try: python3 -m pip install --upgrade great-expectations")
        return False, None

def check_datasource_configured(layer: str):
    """Check if datasource is configured in GX context."""
    try:
        import great_expectations as gx
        context = gx.get_context(context_root_dir=str(GX_DIR))
        
        datasource_name = f"{layer}_datasource"
        try:
            datasource = context.get_datasource(datasource_name)
            print(f"✅ Datasource '{datasource_name}' is configured")
            return True
        except Exception:
            print(f"⚠️  Datasource '{datasource_name}' not configured")
            print(f"   Run: python scripts/setup_datasources.py")
            return False
    except Exception as e:
        print(f"⚠️  Could not check datasource: {e}")
        return False

def run_checkpoint(layer: str, table: str):
    """Run a GX checkpoint."""
    gx_installed, gx = check_gx_installation()
    if not gx_installed:
        return 1
    
    print()
    
    # Check datasource
    datasource_ok = check_datasource_configured(layer)
    if not datasource_ok:
        print("\n⚠️  Datasource not configured. Setting up...")
        # Try to set up datasource
        try:
            from setup_datasources import setup_datasource_via_gx_cli, get_snowflake_connection_string
            import great_expectations as gx
            
            context = gx.get_context(context_root_dir=str(GX_DIR))
            schema = {"raw": "RAW", "staging": "STAGING", "mart": "MART", "quality": "QUALITY"}[layer]
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
            
            context.add_datasource(**datasource_config)
            print(f"✅ Datasource '{layer}_datasource' configured")
            datasource_ok = True
        except Exception as e:
            print(f"❌ Failed to configure datasource: {e}")
            print("   Please run: python scripts/setup_datasources.py")
            return 1
    
    if not datasource_ok:
        return 1
    
    print()
    
    # Run checkpoint
    try:
        context = gx.get_context(context_root_dir=str(GX_DIR))
        checkpoint_name = f"{layer}.{table}"
        
        print(f"🔍 Running checkpoint: {checkpoint_name}")
        print(f"   Table: {table.upper()}")
        print(f"   Schema: {layer.upper()}")
        print()
        
        # Create batch request
        batch_request = {
            "datasource_name": f"{layer}_datasource",
            "data_connector_name": "default_runtime_data_connector",
            "data_asset_name": table.upper(),
            "batch_identifiers": {
                "default_identifier_name": "default_identifier"
            }
        }
        
        # Run checkpoint
        result = context.run_checkpoint(
            checkpoint_name=checkpoint_name,
            batch_request=batch_request
        )
        
        print()
        if result.success:
            print(f"✅ Checkpoint '{checkpoint_name}' PASSED!")
            print(f"   Run ID: {result.run_id.run_name}")
            return 0
        else:
            print(f"❌ Checkpoint '{checkpoint_name}' FAILED!")
            print(f"   Run ID: {result.run_id.run_name}")
            
            # Show failed expectations
            failed_count = 0
            if hasattr(result, 'list_validation_results'):
                for validation_result in result.list_validation_results():
                    for result_item in validation_result.results:
                        if not result_item.success:
                            failed_count += 1
                            exp_type = result_item.expectation_config.expectation_type
                            exp_kwargs = result_item.expectation_config.kwargs
                            print(f"\n   ❌ Failed expectation #{failed_count}:")
                            print(f"      Type: {exp_type}")
                            print(f"      Column: {exp_kwargs.get('column', 'N/A')}")
                            if 'observed_value' in result_item.result:
                                print(f"      Observed: {result_item.result['observed_value']}")
            
            if failed_count == 0:
                print("   (No individual expectation failures found)")
            
            return 1
            
    except Exception as e:
        print(f"❌ Error running checkpoint: {e}")
        import traceback
        traceback.print_exc()
        return 1

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Run a GX checkpoint test")
    parser.add_argument("--layer", required=True, choices=["raw", "staging", "mart", "quality"],
                       help="Data layer")
    parser.add_argument("--table", required=True,
                       help="Table name (e.g., customers, fact_orders)")
    
    args = parser.parse_args()
    
    exit_code = run_checkpoint(args.layer, args.table)
    sys.exit(exit_code)

if __name__ == "__main__":
    main()
