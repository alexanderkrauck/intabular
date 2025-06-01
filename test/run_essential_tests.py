#!/usr/bin/env python3
"""
Essential test runner for fast development feedback
Runs only the core functionality tests that must always pass
"""

import subprocess
import sys
import os
from pathlib import Path


def run_command(cmd, description, exit_on_failure=True):
    """Run a command and return success status"""
    print(f"\n{'='*50}")
    print(f"🚀 {description}")
    print(f"{'='*50}")
    print(f"Command: {cmd}")
    print()
    
    try:
        result = subprocess.run(cmd, shell=True, cwd=Path(__file__).parent.parent)
        success = result.returncode == 0
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"\n{status}: {description}")
        
        if not success and exit_on_failure:
            print(f"\n💥 Essential test failed: {description}")
            print("Fix this before proceeding with development.")
            sys.exit(1)
            
        return success
    except Exception as e:
        print(f"Error running command: {e}")
        if exit_on_failure:
            sys.exit(1)
        return False


def main():
    """Run essential tests for fast feedback"""
    
    print("⚡ InTabular Essential Test Runner")
    print("Running only the most critical fast tests...")
    print("For full testing, use: pytest test/ or python test/run_tests.py")
    
    results = []
    
    # 1. Core functionality tests (super fast)
    success = run_command(
        "python -m pytest test/test_fast_core.py -v --tb=short",
        "Core Functionality Tests (Essential)"
    )
    results.append(("Core Tests", success))
    
    # 2. Basic transformation tests
    success = run_command(
        "python -m pytest test/test_format_transformations.py::TestFormatTransformations::test_email_normalization -v",
        "Email Normalization Test"
    )
    results.append(("Email Normalization", success))
    
    # 3. Data loading verification  
    success = run_command(
        "python -m pytest test/test_perfect_matching.py::TestPerfectMatching::test_all_columns_present -v",
        "Test Data Integrity Check"
    )
    results.append(("Data Integrity", success))
    
    # 4. Configuration loading
    success = run_command(
        "python -m pytest test/test_edge_cases.py::TestEdgeCases::test_configuration_validation -v",
        "Configuration Loading Test"
    )
    results.append(("Config Loading", success))
    
    # 5. Cross-domain compatibility (sample)
    success = run_command(
        "python -m pytest test/test_diverse_domains.py::TestCrossDomainCapabilities::test_numeric_data_handling -v",
        "Cross-Domain Compatibility"
    )
    results.append(("Cross-Domain", success))
    
    # Summary
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    print(f"\n{'='*50}")
    print("📊 ESSENTIAL TESTS SUMMARY")
    print(f"{'='*50}")
    
    for test_name, success in results:
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"{test_name:<25} {status}")
    
    if passed == total:
        print(f"\n🎉 All {total} essential tests passed!")
        print("✅ Core functionality is working correctly")
        print("✅ Ready for development or full test suite")
        
        print(f"\n{'='*50}")
        print("🏃‍♂️ NEXT STEPS")
        print(f"{'='*50}")
        print("# Run full fast test suite:")
        print("pytest test/ -m 'no_llm' -v")
        print()
        print("# Run domain-specific tests:")
        print("pytest test/ -m 'industrial and no_llm' -v")
        print("pytest test/ -m 'financial and no_llm' -v")
        print()
        print("# Run full test suite with LLM (requires API key):")
        print("pytest test/ -v")
        print()
        print("# Generate coverage report:")
        print("pytest test/ --cov=intabular --cov-report=html")
        
    else:
        print(f"\n💥 {total - passed}/{total} essential tests failed!")
        print("❌ Core functionality issues detected")
        print("❌ Fix these before proceeding")
        
        print(f"\n{'='*50}")
        print("🔧 DEBUGGING TIPS")
        print(f"{'='*50}")
        print("# Run failed tests with more detail:")
        print("pytest test/test_fast_core.py -v -s --tb=long")
        print()
        print("# Check data loading:")
        print("python -c 'import pandas as pd; print(pd.read_csv(\"test/data/csv/perfect_match.csv\").head())'")
        print()
        print("# Verify configuration:")
        print("python -c 'from intabular.core.config import GatekeeperConfig; print(GatekeeperConfig.from_yaml(\"test/data/configs/customer_crm.yaml\").purpose)'")
    
    return passed == total


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 