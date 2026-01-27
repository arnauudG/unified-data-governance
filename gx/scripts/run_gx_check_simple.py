#!/usr/bin/env python3
"""
Simple script to run GX checkpoint without full GX installation.
This creates a minimal test to verify the checkpoint configuration.
"""

import os
import sys
import json
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

def validate_checkpoint_config(layer: str, table: str):
    """Validate that checkpoint and expectation suite files exist and are valid."""
    print(f"🔍 Validating checkpoint configuration: {layer}.{table}")
    
    # Check expectation suite
    suite_path = GX_DIR / "expectations" / layer / f"{table}.json"
    if not suite_path.exists():
        print(f"❌ Expectation suite not found: {suite_path}")
        return False
    
    try:
        with open(suite_path, 'r') as f:
            suite = json.load(f)
        
        if suite.get("expectation_suite_name") != f"{layer}.{table}":
            print(f"⚠️  Suite name mismatch: expected '{layer}.{table}', got '{suite.get('expectation_suite_name')}'")
        
        expectations = suite.get("expectations", [])
        print(f"   ✅ Expectation suite found with {len(expectations)} expectations")
        
        for i, exp in enumerate(expectations, 1):
            exp_type = exp.get("expectation_type", "unknown")
            notes = exp.get("meta", {}).get("notes", {}).get("content", "")
            print(f"      {i}. {exp_type}")
            if notes:
                print(f"         └─ {notes}")
        
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON in expectation suite: {e}")
        return False
    
    # Check checkpoint
    checkpoint_path = GX_DIR / "checkpoints" / layer / f"{table}.yml"
    if not checkpoint_path.exists():
        print(f"❌ Checkpoint not found: {checkpoint_path}")
        return False
    
    print(f"   ✅ Checkpoint file found: {checkpoint_path}")
    
    # Check datasource
    datasource_path = GX_DIR / "datasources" / f"{layer}_datasource.yml"
    if datasource_path.exists():
        print(f"   ✅ Datasource template found: {datasource_path}")
    else:
        print(f"   ⚠️  Datasource template not found: {datasource_path}")
    
    print(f"\n✅ Configuration valid for {layer}.{table}")
    print(f"\nTo run this checkpoint:")
    print(f"   1. Install GX: pip install great-expectations")
    print(f"   2. Set up datasource: python scripts/setup_datasources.py")
    print(f"   3. Run checkpoint: python scripts/run_gx_check.py --layer {layer} --table {table}")
    
    return True

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Validate GX checkpoint configuration")
    parser.add_argument("--layer", required=True, choices=["raw", "staging", "mart", "quality"],
                       help="Data layer")
    parser.add_argument("--table", required=True,
                       help="Table name (e.g., customers, fact_orders)")
    
    args = parser.parse_args()
    
    success = validate_checkpoint_config(args.layer, args.table)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
