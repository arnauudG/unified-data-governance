#!/usr/bin/env python3
"""
Run a single Great Expectations checkpoint (one table).
Mimics: soda scan -d datasource -c config.yml checks/layer/table.yml
Usage: python run_gx_check.py --layer mart --table fact_orders
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

# Add GX to path
GX_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(GX_ROOT))

def run_checkpoint(layer: str, table: str):
    """Run a GX checkpoint for a specific table."""
    try:
        import great_expectations as gx
        
        # Initialize GX context
        context = gx.get_context(context_root_dir=str(GX_ROOT / "great_expectations"))
        
        # Run checkpoint
        checkpoint_name = f"{layer}.{table}"
        print(f"🔍 Running checkpoint: {checkpoint_name}")
        print(f"   Expectation suite: {checkpoint_name}")
        print(f"   Datasource: {layer}_datasource")
        print()
        
        # Get batch request
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
            print(f"✅ Checkpoint '{checkpoint_name}' passed!")
            print(f"   Validation ID: {result.run_id.run_name}")
            return 0
        else:
            print(f"❌ Checkpoint '{checkpoint_name}' failed!")
            print(f"   Validation ID: {result.run_id.run_name}")
            # Print failed expectations
            if hasattr(result, 'list_validation_results'):
                for validation_result in result.list_validation_results():
                    for result_item in validation_result.results:
                        if not result_item.success:
                            print(f"   ❌ Failed: {result_item.expectation_config.expectation_type}")
            return 1
            
    except ImportError:
        print("❌ Great Expectations not installed.")
        print("   Install with: pip install great-expectations")
        return 1
    except Exception as e:
        print(f"❌ Error running checkpoint: {e}")
        import traceback
        traceback.print_exc()
        return 1

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Run a single GX checkpoint")
    parser.add_argument("--layer", required=True, choices=["raw", "staging", "mart", "quality"],
                       help="Data layer")
    parser.add_argument("--table", required=True,
                       help="Table name (e.g., customers, fact_orders)")
    
    args = parser.parse_args()
    
    exit_code = run_checkpoint(args.layer, args.table)
    sys.exit(exit_code)

if __name__ == "__main__":
    main()
