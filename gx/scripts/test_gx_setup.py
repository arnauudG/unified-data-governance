#!/usr/bin/env python3
"""
Test GX setup and configuration without running full checkpoints.
This validates the project structure and configuration files.
"""

import os
import sys
import json
import yaml
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
PROJECT_ROOT = Path(__file__).parent.parent.parent
env_file = PROJECT_ROOT / ".env"
if env_file.exists():
    load_dotenv(env_file)

GX_ROOT = Path(__file__).parent.parent
GX_DIR = GX_ROOT / "great_expectations"

def test_project_structure():
    """Test that all required directories exist."""
    print("📁 Testing project structure...")
    
    required_dirs = [
        GX_DIR,
        GX_DIR / "expectations" / "raw",
        GX_DIR / "expectations" / "staging",
        GX_DIR / "expectations" / "mart",
        GX_DIR / "expectations" / "quality",
        GX_DIR / "checkpoints" / "raw",
        GX_DIR / "checkpoints" / "staging",
        GX_DIR / "checkpoints" / "mart",
        GX_DIR / "checkpoints" / "quality",
    ]
    
    all_exist = True
    for directory in required_dirs:
        if directory.exists():
            print(f"   ✅ {directory.relative_to(GX_ROOT)}")
        else:
            print(f"   ❌ {directory.relative_to(GX_ROOT)} - MISSING")
            all_exist = False
    
    return all_exist

def test_expectation_suites():
    """Test that expectation suites are valid JSON."""
    print("\n📋 Testing expectation suites...")
    
    layers = ["raw", "staging", "mart", "quality"]
    total_suites = 0
    valid_suites = 0
    
    for layer in layers:
        layer_dir = GX_DIR / "expectations" / layer
        if not layer_dir.exists():
            continue
        
        for suite_file in layer_dir.glob("*.json"):
            total_suites += 1
            try:
                with open(suite_file, 'r') as f:
                    suite = json.load(f)
                
                suite_name = suite.get("expectation_suite_name", "unknown")
                expectations = suite.get("expectations", [])
                
                print(f"   ✅ {suite_file.name}")
                print(f"      Suite: {suite_name}")
                print(f"      Expectations: {len(expectations)}")
                valid_suites += 1
            except json.JSONDecodeError as e:
                print(f"   ❌ {suite_file.name} - Invalid JSON: {e}")
            except Exception as e:
                print(f"   ❌ {suite_file.name} - Error: {e}")
    
    print(f"\n   Summary: {valid_suites}/{total_suites} valid suites")
    return valid_suites == total_suites and total_suites > 0

def test_checkpoints():
    """Test that checkpoints are valid YAML."""
    print("\n🎯 Testing checkpoints...")
    
    layers = ["raw", "staging", "mart", "quality"]
    total_checkpoints = 0
    valid_checkpoints = 0
    
    for layer in layers:
        layer_dir = GX_DIR / "checkpoints" / layer
        if not layer_dir.exists():
            continue
        
        for checkpoint_file in layer_dir.glob("*.yml"):
            total_checkpoints += 1
            try:
                with open(checkpoint_file, 'r') as f:
                    checkpoint_data = yaml.safe_load(f)
                
                checkpoint_name = list(checkpoint_data.keys())[0] if checkpoint_data else "unknown"
                checkpoint = checkpoint_data.get(checkpoint_name, {})
                
                print(f"   ✅ {checkpoint_file.name}")
                print(f"      Checkpoint: {checkpoint_name}")
                print(f"      Suite: {checkpoint.get('expectation_suite_name', 'N/A')}")
                print(f"      Datasource: {checkpoint.get('batch_request', {}).get('datasource_name', 'N/A')}")
                valid_checkpoints += 1
            except yaml.YAMLError as e:
                print(f"   ❌ {checkpoint_file.name} - Invalid YAML: {e}")
            except Exception as e:
                print(f"   ❌ {checkpoint_file.name} - Error: {e}")
    
    print(f"\n   Summary: {valid_checkpoints}/{total_checkpoints} valid checkpoints")
    return valid_checkpoints == total_checkpoints and total_checkpoints > 0

def test_datasources():
    """Test that datasource templates exist."""
    print("\n🔌 Testing datasource templates...")
    
    layers = ["raw", "staging", "mart", "quality"]
    all_exist = True
    
    for layer in layers:
        datasource_file = GX_DIR / "datasources" / f"{layer}_datasource.yml"
        if datasource_file.exists():
            print(f"   ✅ {layer}_datasource.yml")
        else:
            print(f"   ⚠️  {layer}_datasource.yml - Template missing")
            all_exist = False
    
    return all_exist

def test_environment_variables():
    """Test that required environment variables are set."""
    print("\n🔐 Testing environment variables...")
    
    required_vars = [
        'SNOWFLAKE_ACCOUNT',
        'SNOWFLAKE_USER',
        'SNOWFLAKE_PASSWORD',
        'SNOWFLAKE_DATABASE'
    ]
    
    optional_vars = [
        'SNOWFLAKE_WAREHOUSE',
        'SNOWFLAKE_ROLE'
    ]
    
    all_set = True
    for var in required_vars:
        value = os.getenv(var)
        if value:
            print(f"   ✅ {var}: {'*' * min(len(value), 10)}")
        else:
            print(f"   ❌ {var}: NOT SET")
            all_set = False
    
    for var in optional_vars:
        value = os.getenv(var)
        if value:
            print(f"   ✅ {var}: {value}")
        else:
            print(f"   ⚠️  {var}: Using default")
    
    return all_set

def main():
    """Run all tests."""
    print("🧪 Testing Great Expectations Project Setup\n")
    print(f"GX directory: {GX_DIR}\n")
    
    results = []
    
    results.append(("Project Structure", test_project_structure()))
    results.append(("Expectation Suites", test_expectation_suites()))
    results.append(("Checkpoints", test_checkpoints()))
    results.append(("Datasource Templates", test_datasources()))
    results.append(("Environment Variables", test_environment_variables()))
    
    print("\n" + "="*60)
    print("📊 Test Summary")
    print("="*60)
    
    for test_name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status} - {test_name}")
    
    all_passed = all(result[1] for result in results)
    
    print("\n" + "="*60)
    if all_passed:
        print("✅ All tests passed! Project is ready to use.")
        print("\nNext steps:")
        print("1. Set up datasources: python scripts/setup_datasources.py")
        print("2. Run a checkpoint: python scripts/run_gx_check.py --layer mart --table fact_orders")
    else:
        print("⚠️  Some tests failed. Please fix the issues above.")
    print("="*60)
    
    return 0 if all_passed else 1

if __name__ == "__main__":
    sys.exit(main())
