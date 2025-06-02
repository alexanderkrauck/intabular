#!/usr/bin/env python3
"""
Test script for simplified LLM parsing implementation
"""

import os
import sys
import pandas as pd
import yaml
from pathlib import Path

# Add the parent directory to sys.path to import intabular
sys.path.insert(0, str(Path(__file__).parent.parent))

from intabular.main import setup_llm_client
from intabular.core.config import GatekeeperConfig
from intabular.core.analyzer import DataframeAnalyzer
from intabular.core.strategy import DataframeIngestionStrategy
from intabular.core.processor import DataframeIngestionProcessor


def test_simplified_llm_strategy():
    """Test the simplified LLM strategy creation"""
    
    print("🧪 Testing simplified LLM parsing strategy...")
    
    # Create test data
    source_data = {
        'first_name': ['John', 'Jane'],
        'last_name': ['Doe', 'Smith'],
        'email_addr': ['john.doe@company.com', 'jane.smith@company.com'],
        'notes': ['Very interested', 'Follow up next week']
    }
    source_df = pd.DataFrame(source_data)
    
    # Create test target config using proper constructor
    enrichment_columns = {
        'email': {
            'description': 'Primary email address of the lead',
            'is_entity_identifier': True,
            'identity_indication': 1.0,
            'validation_rules': ['valid_email']
        },
        'full_name': {
            'description': 'Full name of the lead (first last)',
            'is_entity_identifier': True,
            'identity_indication': 0.5,
            'validation_rules': ['non_empty']
        }
    }
    
    additional_columns = {
        'notes': {
            'description': 'Notes about the lead and interactions',
            'is_entity_identifier': False,
            'identity_indication': 0.0,
            'validation_rules': []
        }
    }
    
    try:
        # Check if OpenAI API key is available
        if not os.getenv('OPENAI_API_KEY'):
            print("⚠️  OPENAI_API_KEY not set, skipping LLM test")
            return True
            
        # Setup components
        client = setup_llm_client()
        target_config = GatekeeperConfig(
            purpose='Lead management database for tracking potential customers',
            enrichment_columns=enrichment_columns,
            additional_columns=additional_columns
        )
        analyzer = DataframeAnalyzer(client, target_config)
        strategy_creator = DataframeIngestionStrategy(client)
        processor = DataframeIngestionProcessor(client)
        
        print("📊 Analyzing source dataframe...")
        analysis = analyzer.analyze_dataframe_structure(source_df, "Test CSV with leads")
        
        print("🎯 Creating ingestion strategy...")
        strategy = strategy_creator.create_ingestion_strategy(target_config, analysis)
        
        print("📋 Strategy results:")
        for col, mapping in strategy.no_merge_column_mappings.items():
            print(f"  Entity column '{col}': {mapping['transformation_type']}")
            if mapping.get('llm_source_columns'):
                print(f"    LLM source columns: {mapping['llm_source_columns']}")
        
        for col, mapping in strategy.merge_column_mappings.items():
            print(f"  Merge column '{col}': {mapping['transformation_type']}")
            if mapping.get('llm_source_columns'):
                print(f"    LLM source columns: {mapping['llm_source_columns']}")
        
        # Test processor with a single row to verify LLM format works
        if any(mapping['transformation_type'] == 'llm_format' 
               for mapping in {**strategy.no_merge_column_mappings, **strategy.merge_column_mappings}.values()):
            print("🤖 Testing LLM format processing...")
            
            test_row = source_df.iloc[0].to_dict()
            
            for col, mapping in {**strategy.no_merge_column_mappings, **strategy.merge_column_mappings}.items():
                if mapping['transformation_type'] == 'llm_format':
                    print(f"  Testing LLM format for column '{col}'...")
                    result = processor.apply_column_mapping(
                        mapping, test_row, col, target_config, 
                        analysis.general_ingestion_analysis
                    )
                    print(f"    Result: {result}")
        
        print("✅ Simplified LLM parsing test completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_format_strategy():
    """Test that format strategy still works correctly"""
    
    print("🧪 Testing format strategy (should be unchanged)...")
    
    # Create simple test data where format should work
    source_data = {
        'email_address': ['JOHN.DOE@COMPANY.COM', 'jane.smith@COMPANY.COM']
    }
    source_df = pd.DataFrame(source_data)
    
    enrichment_columns = {
        'email': {
            'description': 'Normalized email address',
            'is_entity_identifier': True,
            'identity_indication': 1.0,
            'validation_rules': ['valid_email', 'lowercase']
        }
    }
    
    try:
        if not os.getenv('OPENAI_API_KEY'):
            print("⚠️  OPENAI_API_KEY not set, skipping format test")
            return True
            
        client = setup_llm_client()
        target_config = GatekeeperConfig(
            purpose='Simple email database',
            enrichment_columns=enrichment_columns
        )
        analyzer = DataframeAnalyzer(client, target_config)
        strategy_creator = DataframeIngestionStrategy(client)
        processor = DataframeIngestionProcessor(client)
        
        analysis = analyzer.analyze_dataframe_structure(source_df, "Simple email CSV")
        strategy = strategy_creator.create_ingestion_strategy(target_config, analysis)
        
        # Test format processing
        email_mapping = strategy.no_merge_column_mappings.get('email', {})
        if email_mapping.get('transformation_type') == 'format':
            print("  Testing format transformation...")
            test_row = source_df.iloc[0].to_dict()
            result = processor.apply_column_mapping(
                email_mapping, test_row, 'email', target_config,
                analysis.general_ingestion_analysis
            )
            print(f"    Input: {test_row['email_address']}")
            print(f"    Output: {result}")
            print(f"    Rule: {email_mapping.get('transformation_rule', 'None')}")
        
        print("✅ Format strategy test completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Format test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    print("🚀 Testing simplified LLM parsing implementation\n")
    
    success = True
    success &= test_format_strategy()
    print()
    success &= test_simplified_llm_strategy()
    
    if success:
        print("\n🎉 All tests passed!")
        sys.exit(0)
    else:
        print("\n💥 Some tests failed!")
        sys.exit(1) 