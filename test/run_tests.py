#!/usr/bin/env python3
"""
Test runner script for InTabular test suite
Demonstrates how to run different types of tests including diverse data domains
"""

import subprocess
import sys
import os
from pathlib import Path


def run_command(cmd, description):
    """Run a command and display results"""
    print(f"\n{'='*70}")
    print(f"🧪 {description}")
    print(f"{'='*70}")
    print(f"Command: {cmd}")
    print()
    
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd=Path(__file__).parent.parent)
        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)
        return result.returncode == 0
    except Exception as e:
        print(f"Error running command: {e}")
        return False


def main():
    """Run various test scenarios to demonstrate the test suite"""
    
    print("🚀 InTabular Test Suite Demo - Diverse Data Domains")
    print("=" * 70)
    
    # Check if OpenAI API key is available
    api_key_available = bool(os.getenv('OPENAI_API_KEY'))
    print(f"OpenAI API Key Available: {'✅' if api_key_available else '❌'}")
    if not api_key_available:
        print("Note: LLM tests will be skipped without OPENAI_API_KEY")
    
    results = []
    
    # Test 1: NO LLM - Format transformations (fast, no API needed)
    print(f"\n{'🟢 FAST TESTS (NO LLM REQUIRED)':<50}")
    print("=" * 70)
    
    success = run_command(
        "python -m pytest test/test_format_transformations.py::TestFormatTransformations::test_email_normalization -v -m 'no_llm'",
        "Email Normalization (No LLM Required)"
    )
    results.append(("Email Normalization", success))
    
    # Test 2: Industrial data without LLM
    success = run_command(
        "python -m pytest test/test_diverse_domains.py::TestIndustrialData::test_sensor_data_structure -v",
        "Industrial Sensor Data Structure (No LLM)"
    )
    results.append(("Industrial Data Structure", success))
    
    # Test 3: Financial data transformations
    success = run_command(
        "python -m pytest test/test_diverse_domains.py::TestFinancialData::test_transaction_id_normalization -v",
        "Financial Transaction ID Normalization (No LLM)"
    )
    results.append(("Financial Data Normalization", success))
    
    # Test 4: Scientific data transformations
    success = run_command(
        "python -m pytest test/test_diverse_domains.py::TestScientificData::test_sample_id_standardization -v",
        "Scientific Sample ID Standardization (No LLM)"
    )
    results.append(("Scientific Data Standardization", success))
    
    # Test 5: Cross-domain numeric handling
    success = run_command(
        "python -m pytest test/test_diverse_domains.py::TestCrossDomainCapabilities::test_numeric_data_handling -v",
        "Cross-Domain Numeric Data Handling (No LLM)"
    )
    results.append(("Cross-Domain Numeric Handling", success))
    
    # Test 6: All no-LLM tests across different domains
    success = run_command(
        "python -m pytest test/ -m 'no_llm' --tb=line -q",
        "All Fast Tests (No LLM Required)"
    )
    results.append(("All Fast Tests", success))
    
    # LLM TESTS (if API key available)
    if api_key_available:
        print(f"\n{'🔵 LLM TESTS (REQUIRE API KEY)':<50}")
        print("=" * 70)
        
        success = run_command(
            "python -m pytest test/test_llm_parsing.py::TestLLMParsing::test_llm_direct_parsing -v",
            "LLM Direct Parsing Test"
        )
        results.append(("LLM Direct Parsing", success))
        
        success = run_command(
            "python -m pytest test/test_diverse_domains.py::TestIndustrialData::test_industrial_pipeline_integration -v",
            "Industrial Data with LLM Pipeline"
        )
        results.append(("Industrial LLM Pipeline", success))
        
        success = run_command(
            "python -m pytest test/test_integration.py::TestIntegration::test_full_pipeline_perfect_match -v",
            "Full Pipeline Integration Test"
        )
        results.append(("Full Pipeline Integration", success))
        
        # Test different data domains with LLM
        success = run_command(
            "python -m pytest test/ -m 'llm and industrial' -v --tb=line",
            "Industrial Data LLM Tests"
        )
        results.append(("Industrial LLM Tests", success))
        
    else:
        print(f"\n{'⚠️  SKIPPING LLM TESTS (NO API KEY)':<50}")
        print("=" * 70)
        print("Set OPENAI_API_KEY to run LLM-dependent tests")
    
    # DOMAIN-SPECIFIC TEST EXAMPLES
    print(f"\n{'📊 DOMAIN-SPECIFIC TEST EXAMPLES':<50}")
    print("=" * 70)
    
    # Show how to run tests by domain
    domain_examples = [
        ("industrial", "Industrial/Sensor Data Tests"),
        ("financial", "Financial Transaction Tests"),
        ("scientific", "Laboratory/Scientific Data Tests"),
        ("customer", "Customer/CRM Data Tests")
    ]
    
    for domain, description in domain_examples:
        success = run_command(
            f"python -m pytest test/ -m '{domain} and no_llm' --tb=line -q",
            f"{description} (Fast)"
        )
        results.append((f"{description} Fast", success))
    
    # Summary
    print(f"\n{'='*70}")
    print("📊 TEST SUMMARY")
    print(f"{'='*70}")
    
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    for test_name, success in results:
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"{test_name:<40} {status}")
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! The test suite works correctly.")
    else:
        print("⚠️  Some tests failed. Check the output above for details.")
    
    # Show comprehensive usage examples
    print(f"\n{'='*70}")
    print("🏃‍♂️ COMPREHENSIVE USAGE EXAMPLES")
    print(f"{'='*70}")
    print("""
# BASIC USAGE
pytest test/                                    # Run all tests
pytest test/ -v                                 # Verbose output

# FAST TESTS (NO LLM/API REQUIRED)
pytest test/ -m "no_llm"                        # Only fast tests
pytest test/ -m "no_llm" -n auto                # Fast tests in parallel

# LLM TESTS (REQUIRE OPENAI_API_KEY)
pytest test/ -m "llm"                           # Only LLM tests
pytest test/ -m "not llm"                       # Skip LLM tests

# DOMAIN-SPECIFIC TESTS
pytest test/ -m "industrial"                    # Industrial sensor data
pytest test/ -m "financial"                     # Financial transactions
pytest test/ -m "scientific"                    # Laboratory data
pytest test/ -m "customer"                      # Customer/CRM data

# COMBINED FILTERS
pytest test/ -m "industrial and no_llm"         # Fast industrial tests
pytest test/ -m "customer and llm"              # Customer tests with LLM
pytest test/ -m "not customer"                  # All non-customer domains

# INTEGRATION VS UNIT TESTS
pytest test/ -m "unit"                          # Unit tests only
pytest test/ -m "integration"                   # Integration tests only

# COVERAGE AND REPORTING
pytest test/ --cov=intabular --cov-report=html  # Generate coverage report
pytest test/ --tb=short                         # Shorter error traces
pytest test/ -x                                 # Stop on first failure

# PERFORMANCE AND DEBUGGING
pytest test/ -n auto                            # Parallel execution
pytest test/ --durations=10                     # Show 10 slowest tests
pytest test/ -s                                 # Show print statements
""")
    
    print("\n💡 KEY FEATURES OF THIS TEST SUITE:")
    print("✅ Works with OR without OpenAI API key")
    print("✅ Tests multiple data domains (industrial, financial, scientific, customer)")
    print("✅ Clear separation between fast unit tests and LLM integration tests")
    print("✅ Comprehensive coverage with 50+ test cases")
    print("✅ CI/CD friendly with automatic test marking")
    print("✅ Demonstrates InTabular works beyond just customer data")
    
    return passed == total


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 