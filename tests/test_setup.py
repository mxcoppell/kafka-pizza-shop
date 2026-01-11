#!/usr/bin/env python3
"""
Test script to verify environment setup for the pizza shop demo.

This script checks:
- Python version compatibility
- Required dependencies installed
- Configuration files present
- Python modules can be imported
- Configuration loads correctly
"""

import sys
import os
from pathlib import Path


def test_python_version():
    """Test Python version is compatible (3.8+)."""
    print("Testing Python version...")
    version = sys.version_info
    if version.major >= 3 and version.minor >= 8:
        print(f"  ✓ Python {version.major}.{version.minor}.{version.micro} (compatible)")
        return True
    else:
        print(f"  ✗ Python {version.major}.{version.minor}.{version.micro} (requires 3.8+)")
        return False


def test_dependencies():
    """Test required dependencies are installed."""
    print("\nTesting dependencies...")
    required_packages = {
        "confluent_kafka": "Confluent Kafka Python client",
        "dotenv": "Python-dotenv for environment variables",
    }
    
    all_installed = True
    for package, description in required_packages.items():
        try:
            __import__(package)
            print(f"  ✓ {description} installed")
        except ImportError:
            print(f"  ✗ {description} NOT installed")
            all_installed = False
    
    return all_installed


def test_config_files():
    """Test configuration files exist."""
    print("\nTesting configuration files...")
    required_files = [
        ".env",
        "requirements.txt",
        "docker-compose.yml",
        "scripts/init-kafka.sh",
    ]
    
    all_present = True
    for filename in required_files:
        filepath = Path(filename)
        if filepath.exists():
            print(f"  ✓ {filename} exists")
        else:
            print(f"  ✗ {filename} NOT found")
            all_present = False
    
    return all_present


def test_source_structure():
    """Test source code directory structure."""
    print("\nTesting source directory structure...")
    required_dirs = [
        "src",
        "src/producers",
        "src/consumers",
        "src/utils",
    ]
    
    all_present = True
    for dirname in required_dirs:
        dirpath = Path(dirname)
        if dirpath.is_dir():
            print(f"  ✓ {dirname}/ exists")
        else:
            print(f"  ✗ {dirname}/ NOT found")
            all_present = False
    
    return all_present


def test_python_imports():
    """Test Python modules can be imported."""
    print("\nTesting Python module imports...")
    
    # Add project root to path if needed
    project_root = Path(__file__).parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    
    modules_to_test = [
        "src.config",
        "src.models",
        "src.producers.order_producer",
        "src.producers.web_producer",
        "src.producers.frontdesk_producer",
        "src.producers.simulate_orders",
        "src.consumers.pizza_maker",
        "src.consumers.delivery_worker",
        "src.consumers.bookkeeper",
    ]
    
    all_imported = True
    for module_name in modules_to_test:
        try:
            __import__(module_name)
            print(f"  ✓ {module_name} imports successfully")
        except Exception as e:
            print(f"  ✗ {module_name} import failed: {e}")
            all_imported = False
    
    return all_imported


def test_config_loading():
    """Test configuration loads properly."""
    print("\nTesting configuration loading...")
    
    try:
        # Add project root to path
        project_root = Path(__file__).parent.parent
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))
        
        from src import config
        
        # Test key configuration values
        tests = [
            ("KAFKA_BOOTSTRAP_SERVERS", config.KAFKA_BOOTSTRAP_SERVERS),
            ("TOPIC_PIZZA_ORDERS", config.TOPIC_PIZZA_ORDERS),
            ("TOPIC_COOKED_PIZZAS", config.TOPIC_COOKED_PIZZAS),
            ("TOPIC_ORDER_EVENTS", config.TOPIC_ORDER_EVENTS),
            ("CONSUMER_GROUP_PIZZA_MAKERS", config.CONSUMER_GROUP_PIZZA_MAKERS),
            ("TOTAL_ORDERS", config.TOTAL_ORDERS),
        ]
        
        all_valid = True
        for key, value in tests:
            if value:
                print(f"  ✓ {key} = {value}")
            else:
                print(f"  ✗ {key} is empty or None")
                all_valid = False
        
        return all_valid
        
    except Exception as e:
        print(f"  ✗ Configuration loading failed: {e}")
        return False


def test_models():
    """Test models module and data structures."""
    print("\nTesting models and data structures...")
    
    try:
        # Add project root to path
        project_root = Path(__file__).parent.parent
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))
        
        from src import models
        
        # Test pizza menu
        if len(models.PIZZA_MENU) > 0:
            print(f"  ✓ Pizza menu loaded ({len(models.PIZZA_MENU)} types)")
        else:
            print(f"  ✗ Pizza menu is empty")
            return False
        
        # Test helper functions
        test_order_id = models.generate_order_id()
        if test_order_id:
            print(f"  ✓ Order ID generation works")
        else:
            print(f"  ✗ Order ID generation failed")
            return False
        
        # Test customer name generation
        test_name = models.generate_customer_name()
        if test_name and len(test_name) > 0:
            print(f"  ✓ Customer name generation works")
        else:
            print(f"  ✗ Customer name generation failed")
            return False
        
        return True
        
    except Exception as e:
        print(f"  ✗ Models testing failed: {e}")
        return False


def main():
    """Run all tests and report results."""
    print("=" * 70)
    print("Pizza Shop Demo - Environment Setup Test")
    print("=" * 70)
    
    results = []
    
    # Run all tests
    results.append(("Python Version", test_python_version()))
    results.append(("Dependencies", test_dependencies()))
    results.append(("Config Files", test_config_files()))
    results.append(("Source Structure", test_source_structure()))
    results.append(("Python Imports", test_python_imports()))
    results.append(("Config Loading", test_config_loading()))
    results.append(("Models", test_models()))
    
    # Summary
    print("\n" + "=" * 70)
    print("Test Summary")
    print("=" * 70)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{status:8} {test_name}")
    
    print("-" * 70)
    print(f"Results: {passed}/{total} tests passed")
    print("=" * 70)
    
    if passed == total:
        print("\n✓ All tests passed! Environment is ready.")
        return 0
    else:
        print(f"\n✗ {total - passed} test(s) failed. Please fix the issues above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
