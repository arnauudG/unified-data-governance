#!/usr/bin/env python3
"""
Validation script for Kubernetes deployment setup
"""

import os
import sys
import yaml
from pathlib import Path

# Add parent directory to Python path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

def validate_docker_setup():
    """Validate Docker setup"""
    print("🐳 Validating Docker setup...")
    
    # Check Dockerfile exists
    if not Path("Dockerfile").exists():
        print("❌ Dockerfile not found")
        return False
    
    # Check requirements.txt exists
    if not Path("requirements.txt").exists():
        print("❌ requirements.txt not found")
        return False
    
    print("✅ Docker files present")
    return True

def validate_k8s_files():
    """Validate Kubernetes files"""
    print("\n☸️  Validating Kubernetes files...")
    
    k8s_files = ["k8s/cronjob.yaml", "k8s/debug.yaml", "k8s/README.md"]
    
    for file_path in k8s_files:
        if not Path(file_path).exists():
            print(f"❌ {file_path} not found")
            return False
        print(f"✅ {file_path} exists")
    
    return True

def validate_config_structure():
    """Validate configuration structure"""
    print("\n⚙️  Validating configuration structure...")
    
    try:
        from config import load_config
        config = load_config()
        
        # Check required fields
        required_fields = [
            ('collibra.base_url', config.collibra.base_url),
            ('soda.base_url', config.soda.base_url),
            ('collibra.asset_types.table_asset_type', config.collibra.asset_types.table_asset_type),
            ('collibra.asset_types.soda_check_asset_type', config.collibra.asset_types.soda_check_asset_type),
        ]
        
        for field_name, field_value in required_fields:
            if not field_value:
                print(f"❌ Missing required field: {field_name}")
                return False
        
        print("✅ Configuration structure valid")
        return True
        
    except Exception as e:
        print(f"❌ Configuration validation failed: {e}")
        return False

def validate_environment_overrides():
    """Validate environment variable override functionality"""
    print("\n🔧 Validating environment variable overrides...")
    
    try:
        # Set test environment variable
        os.environ['SODA_API_KEY_ID'] = 'test_override_value'
        
        from config import load_config
        config = load_config()
        
        if config.soda.api_key_id == 'test_override_value':
            print("✅ Environment variable override working")
            return True
        else:
            print(f"❌ Environment override failed. Expected 'test_override_value', got '{config.soda.api_key_id}'")
            return False
            
    except Exception as e:
        print(f"❌ Environment override validation failed: {e}")
        return False
    finally:
        # Clean up test environment variable
        if 'SODA_API_KEY_ID' in os.environ:
            del os.environ['SODA_API_KEY_ID']

def validate_cli_functionality():
    """Validate CLI functionality"""
    print("\n🖥️  Validating CLI functionality...")
    
    try:
        # Test that main module can be imported
        import main
        print("✅ Main module imports successfully")
        
        # Test that integration class can be imported
        from integration import SodaCollibraIntegration
        print("✅ Integration class imports successfully")
        
        return True
        
    except Exception as e:
        print(f"❌ CLI validation failed: {e}")
        return False

def main():
    """Main validation function"""
    print("🚀 Kubernetes Deployment Validation")
    print("=" * 50)
    
    # Change to parent directory for correct relative paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)
    os.chdir(parent_dir)
    
    validations = [
        validate_docker_setup,
        validate_k8s_files,
        validate_config_structure,
        validate_environment_overrides,
        validate_cli_functionality,
    ]
    
    results = []
    for validation in validations:
        try:
            result = validation()
            results.append(result)
        except Exception as e:
            print(f"❌ Validation error: {e}")
            results.append(False)
    
    print("\n" + "=" * 50)
    print("📊 Validation Summary")
    print("=" * 50)
    
    if all(results):
        print("🎉 All validations passed! Kubernetes deployment is ready.")
        print("\n📋 Next steps:")
        print("1. Build and push Docker image to your registry")
        print("2. Update image URLs in k8s/cronjob.yaml and k8s/debug.yaml")
        print("3. Replace <customer-name> placeholders in YAML files")
        print("4. Follow the deployment instructions in k8s/README.md")
        return 0
    else:
        print("❌ Some validations failed. Please fix the issues above.")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 