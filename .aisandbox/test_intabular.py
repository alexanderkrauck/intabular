#!/usr/bin/env python3
"""
Test script for InTabular package functionality.
"""

import sys
import os
from pathlib import Path

# Add parent directory to path so we can import intabular
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    print("🧪 Testing InTabular package imports...")
    
    # Test basic imports
    from intabular import AdaptiveMerger, TableConfig
    from intabular.core import CSVAnalyzer, IngestionStrategy, DataProcessor
    print("✅ All imports successful!")
    
    # Test TableConfig creation
    print("\n📋 Testing TableConfig...")
    config = TableConfig(
        purpose="Test customer database",
        enrichment_columns={"email": "Primary email", "name": "Full name"},
        column_policy="Keep all relevant data"
    )
    print(f"✅ TableConfig created: {len(config.get_enrichment_column_names())} columns")
    
    # Test YAML save/load
    print("\n💾 Testing YAML operations...")
    test_yaml = Path("test_config.yaml")
    config.to_yaml(str(test_yaml))
    loaded_config = TableConfig.from_yaml(str(test_yaml))
    print(f"✅ YAML operations successful: {loaded_config.purpose[:30]}...")
    
    # Clean up
    if test_yaml.exists():
        test_yaml.unlink()
    
    print("\n🎉 All tests passed! InTabular package is working correctly.")
    
except ImportError as e:
    print(f"❌ Import error: {e}")
    sys.exit(1)
except Exception as e:
    print(f"❌ Test error: {e}")
    sys.exit(1)
