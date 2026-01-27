#!/usr/bin/env python3
"""
Run all Great Expectations checkpoints for a layer.
Mimics: soda scan -d datasource -c config.yml checks/layer/
Usage: python run_gx_layer.py --layer mart
"""

import os
import sys
import argparse
from pathlib import Path

# Add GX to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
GX_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(GX_ROOT))

def get_tables_for_layer(layer: str) -> list:
    """Get list of tables for a layer based on Soda check files."""
    soda_checks_dir = PROJECT_ROOT / "soda" / "checks" / layer
    if not soda_checks_dir.exists():
        return []
    
    tables = []
    for check_file in soda_checks_dir.glob("*.yml"):
        # Extract table name from file name
        table_name = check_file.stem
        tables.append(table_name)
    
    return tables

def run_layer_checkpoints(layer: str):
    """Run all checkpoints for a layer."""
    try:
        import great_expectations as gx
        
        # Initialize GX context
        context = gx.get_context(context_root_dir=str(GX_ROOT / "great_expectations"))
        
        # Get tables for this layer
        tables = get_tables_for_layer(layer)
        
        if not tables:
            print(f"⚠️  No tables found for layer: {layer}")
            return 1
        
        print(f"🔍 Running checkpoints for layer: {layer}")
        print(f"   Tables: {', '.join(tables)}")
        
        results = []
        for table in tables:
            checkpoint_name = f"{layer}.{table}"
            print(f"\n  Running: {checkpoint_name}")
            try:
                result = context.run_checkpoint(checkpoint_name=checkpoint_name)
                if result.success:
                    print(f"  ✅ {checkpoint_name} passed")
                    results.append(True)
                else:
                    print(f"  ❌ {checkpoint_name} failed")
                    results.append(False)
            except Exception as e:
                print(f"  ❌ {checkpoint_name} error: {e}")
                results.append(False)
        
        passed = sum(results)
        total = len(results)
        print(f"\n📊 Results: {passed}/{total} checkpoints passed")
        
        return 0 if all(results) else 1
        
    except ImportError:
        print("❌ Great Expectations not installed. Install with: pip install great-expectations")
        return 1
    except Exception as e:
        print(f"❌ Error running checkpoints: {e}")
        return 1

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Run all GX checkpoints for a layer")
    parser.add_argument("--layer", required=True, 
                       choices=["raw", "staging", "mart", "quality", "all"],
                       help="Data layer (or 'all' for all layers)")
    
    args = parser.parse_args()
    
    if args.layer == "all":
        layers = ["raw", "staging", "mart", "quality"]
        exit_codes = []
        for layer in layers:
            exit_code = run_layer_checkpoints(layer)
            exit_codes.append(exit_code)
        sys.exit(max(exit_codes))
    else:
        exit_code = run_layer_checkpoints(args.layer)
        sys.exit(exit_code)

if __name__ == "__main__":
    main()
