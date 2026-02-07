#!/usr/bin/env python3
"""
Local Kubernetes Environment Testing Script

This script simulates the Kubernetes environment locally by:
1. Setting environment variables like K8s secrets
2. Testing the integration with different configurations
3. Validating the Docker build process
4. Simulating different deployment scenarios
"""

import os
import sys
import subprocess
import tempfile
import yaml
from pathlib import Path
import time

# Add parent directory to Python path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

class K8sLocalTester:
    """Local Kubernetes environment tester"""
    
    def __init__(self):
        self.original_env = dict(os.environ)
        self.test_results = []
        
        # Change to parent directory for correct relative paths
        self.original_cwd = os.getcwd()
        script_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(script_dir)
        os.chdir(parent_dir)
    
    def restore_environment(self):
        """Restore original environment variables"""
        os.environ.clear()
        os.environ.update(self.original_env)
        os.chdir(self.original_cwd)
    
    def set_k8s_environment(self, test_credentials=True):
        """Set environment variables as they would be in Kubernetes"""
        print("🔧 Setting up Kubernetes-like environment...")
        
        if test_credentials:
            # Use test credentials (safe for local testing)
            os.environ['SODA_API_KEY_ID'] = 'test-api-key-id'
            os.environ['SODA_API_KEY_SECRET'] = 'test-api-key-secret'
            os.environ['COLLIBRA_USERNAME'] = 'test-username'
            os.environ['COLLIBRA_PASSWORD'] = 'test-password'
            os.environ['COLLIBRA_BASE_URL'] = 'https://test-collibra.com/rest/2.0'
        else:
            # Use real credentials from config file (will be overridden)
            print("  Using real credentials from config.yaml")
        
        print("✅ Environment variables set")
    
    def test_config_loading(self):
        """Test configuration loading with environment overrides"""
        print("\n📋 Testing configuration loading...")
        
        try:
            from config import load_config
            config = load_config()
            
            # Check if environment variables override config values
            if os.environ.get('SODA_API_KEY_ID'):
                expected = os.environ['SODA_API_KEY_ID']
                actual = config.soda.api_key_id
                if actual == expected:
                    print(f"✅ SODA_API_KEY_ID override working: {actual}")
                else:
                    print(f"❌ SODA_API_KEY_ID override failed: expected {expected}, got {actual}")
                    return False
            
            if os.environ.get('COLLIBRA_USERNAME'):
                expected = os.environ['COLLIBRA_USERNAME']
                actual = config.collibra.username
                if actual == expected:
                    print(f"✅ COLLIBRA_USERNAME override working: {actual}")
                else:
                    print(f"❌ COLLIBRA_USERNAME override failed: expected {expected}, got {actual}")
                    return False
            
            print("✅ Configuration loading test passed")
            return True
            
        except Exception as e:
            print(f"❌ Configuration loading failed: {e}")
            return False
    
    def test_cli_functionality(self):
        """Test CLI functionality as it would work in Kubernetes"""
        print("\n🖥️  Testing CLI functionality...")
        
        test_commands = [
            ["python", "main.py", "--help"],
            ["python", "-c", "from integration import SodaCollibraIntegration; print('Import successful')"],
            ["python", "-c", "from config import load_config; config = load_config(); print('Config loaded')"],
        ]
        
        for cmd in test_commands:
            try:
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                
                if result.returncode == 0:
                    print(f"✅ Command succeeded: {' '.join(cmd)}")
                else:
                    print(f"❌ Command failed: {' '.join(cmd)}")
                    print(f"   Error: {result.stderr}")
                    return False
                    
            except subprocess.TimeoutExpired:
                print(f"⏰ Command timed out: {' '.join(cmd)}")
                return False
            except Exception as e:
                print(f"❌ Command error: {' '.join(cmd)} - {e}")
                return False
        
        print("✅ CLI functionality test passed")
        return True
    
    def test_docker_build(self):
        """Test Docker build process"""
        print("\n🐳 Testing Docker build...")
        
        try:
            # Check if Docker is available
            result = subprocess.run(
                ["docker", "--version"],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode != 0:
                print("⚠️  Docker not available - skipping Docker tests")
                return True
            
            print(f"✅ Docker available: {result.stdout.strip()}")
            
            # Test Docker build
            print("🔨 Building Docker image...")
            build_result = subprocess.run(
                ["docker", "build", "-t", "soda-collibra-integration:test", "."],
                capture_output=True,
                text=True,
                timeout=300  # 5 minutes timeout
            )
            
            if build_result.returncode == 0:
                print("✅ Docker build successful")
                
                # Test running the container
                print("🏃 Testing container run...")
                run_result = subprocess.run(
                    ["docker", "run", "--rm", "soda-collibra-integration:test", "python", "main.py", "--help"],
                    capture_output=True,
                    text=True,
                    timeout=60
                )
                
                if run_result.returncode == 0:
                    print("✅ Container run test successful")
                    return True
                else:
                    print(f"❌ Container run failed: {run_result.stderr}")
                    return False
            else:
                print(f"❌ Docker build failed: {build_result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            print("⏰ Docker operation timed out")
            return False
        except FileNotFoundError:
            print("⚠️  Docker not installed - skipping Docker tests")
            return True
        except Exception as e:
            print(f"❌ Docker test error: {e}")
            return False
    
    def test_k8s_yaml_validation(self):
        """Validate Kubernetes YAML files"""
        print("\n☸️  Validating Kubernetes YAML files...")
        
        yaml_files = [
            "k8s/cronjob.yaml",
            "k8s/debug.yaml"
        ]
        
        for yaml_file in yaml_files:
            try:
                with open(yaml_file, 'r') as f:
                    yaml_content = yaml.safe_load(f)
                
                # Basic validation
                if 'apiVersion' not in yaml_content:
                    print(f"❌ {yaml_file}: Missing apiVersion")
                    return False
                
                if 'kind' not in yaml_content:
                    print(f"❌ {yaml_file}: Missing kind")
                    return False
                
                print(f"✅ {yaml_file}: Valid YAML structure")
                
            except yaml.YAMLError as e:
                print(f"❌ {yaml_file}: YAML parsing error - {e}")
                return False
            except FileNotFoundError:
                print(f"❌ {yaml_file}: File not found")
                return False
        
        print("✅ Kubernetes YAML validation passed")
        return True
    
    def simulate_k8s_deployment(self):
        """Simulate a Kubernetes deployment scenario"""
        print("\n🚀 Simulating Kubernetes deployment scenario...")
        
        scenarios = [
            {
                "name": "Production (minimal logging)",
                "args": [],
                "env": {"LOG_LEVEL": "WARNING"}
            },
            {
                "name": "Verbose logging",
                "args": ["--verbose"],
                "env": {}
            },
            {
                "name": "Debug logging",
                "args": ["--debug"],
                "env": {}
            }
        ]
        
        for scenario in scenarios:
            print(f"\n  📋 Testing scenario: {scenario['name']}")
            
            # Set scenario environment
            for key, value in scenario['env'].items():
                os.environ[key] = value
            
            try:
                # Test that the command would work (without actually running integration)
                cmd = ["python", "main.py", "--help"] + scenario['args']
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                
                if result.returncode == 0:
                    print(f"    ✅ Scenario '{scenario['name']}' would work")
                else:
                    print(f"    ❌ Scenario '{scenario['name']}' failed")
                    return False
                    
            except Exception as e:
                print(f"    ❌ Scenario '{scenario['name']}' error: {e}")
                return False
            finally:
                # Clean up scenario environment
                for key in scenario['env'].keys():
                    if key in os.environ:
                        del os.environ[key]
        
        print("✅ Kubernetes deployment simulation passed")
        return True
    
    def run_all_tests(self, use_real_credentials=False):
        """Run all tests"""
        print("🧪 Starting Kubernetes Local Testing")
        print("=" * 60)
        
        try:
            # Setup environment
            self.set_k8s_environment(test_credentials=not use_real_credentials)
            
            # Run tests
            tests = [
                ("Configuration Loading", self.test_config_loading),
                ("CLI Functionality", self.test_cli_functionality),
                ("Docker Build", self.test_docker_build),
                ("K8s YAML Validation", self.test_k8s_yaml_validation),
                ("K8s Deployment Simulation", self.simulate_k8s_deployment),
            ]
            
            results = []
            for test_name, test_func in tests:
                try:
                    result = test_func()
                    results.append((test_name, result))
                    self.test_results.append({"name": test_name, "passed": result})
                except Exception as e:
                    print(f"❌ {test_name} crashed: {e}")
                    results.append((test_name, False))
                    self.test_results.append({"name": test_name, "passed": False, "error": str(e)})
            
            # Summary
            print("\n" + "=" * 60)
            print("📊 Test Results Summary")
            print("=" * 60)
            
            passed = sum(1 for _, result in results if result)
            total = len(results)
            
            for test_name, result in results:
                status = "✅ PASS" if result else "❌ FAIL"
                print(f"{status} {test_name}")
            
            print(f"\n📈 Overall: {passed}/{total} tests passed")
            
            if passed == total:
                print("\n🎉 All tests passed! Kubernetes deployment is ready.")
                print("\n📋 Next steps:")
                print("1. Start Docker if you want to test containerization")
                print("2. Update image URLs in k8s/*.yaml files")
                print("3. Replace <customer-name> placeholders")
                print("4. Deploy to your Kubernetes cluster")
                return True
            else:
                print(f"\n⚠️  {total - passed} test(s) failed. Please fix issues before deploying.")
                return False
                
        finally:
            self.restore_environment()

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Test Kubernetes setup locally")
    parser.add_argument(
        "--real-credentials",
        action="store_true",
        help="Use real credentials from config.yaml instead of test values"
    )
    
    args = parser.parse_args()
    
    tester = K8sLocalTester()
    success = tester.run_all_tests(use_real_credentials=args.real_credentials)
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main()) 