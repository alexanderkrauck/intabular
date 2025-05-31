#!/usr/bin/env python3
"""
Test script to validate the phased entity-aware strategy algorithm.
"""

import sys
sys.path.append('..')

from intabular.core.strategy import EntityAwareIngestionStrategy
from intabular.core.config import GatekeeperConfig

def test_phased_strategy():
    """Test the phased processing: entity matching columns first, then remaining"""
    
    print("🧪 Testing Phased Strategy Processing...")
    
    # Mock OpenAI client for testing
    class MockOpenAI:
        pass
    
    # Create strategy instance
    strategy = EntityAwareIngestionStrategy(MockOpenAI())
    print("✅ EntityAwareIngestionStrategy initialization works")
    
    # Test enhanced config format matching yaml_example.yaml
    enrichment_columns = {
        "email": {
            "description": "Primary email address for customer contact",
            "is_entity_identifier": True,
            "identity_indication": 1.0  # Strong entity identifier
        },
        "full_name": {
            "description": "Full customer name",
            "is_entity_identifier": True, 
            "identity_indication": 0.5  # Moderate entity identifier
        },
        "company_name": {
            "description": "Company where customer works",
            "is_entity_identifier": True,
            "identity_indication": 0.0  # Entity identifier but no matching strength
        },
        "deal_stage": {
            "description": "Current deal stage",
            "is_entity_identifier": False,
            "identity_indication": 0.0  # Descriptive column
        },
        "notes": {
            "description": "Notes about customer",
            "is_entity_identifier": False,
            "identity_indication": 0.0  # Descriptive column
        }
    }
    
    config = GatekeeperConfig(
        purpose="Customer relationship database for testing",
        enrichment_columns=enrichment_columns,
        target_file_path="test_customers.csv"
    )
    
    # Test Phase 1: Get entity matching columns (identity_indication > 0)
    entity_matching_columns = strategy._get_entity_matching_columns(config)
    
    # Should only include columns with identity_indication > 0
    assert "email" in entity_matching_columns  # identity_indication = 1.0
    assert "full_name" in entity_matching_columns  # identity_indication = 0.5
    assert "company_name" not in entity_matching_columns  # identity_indication = 0.0
    assert "deal_stage" not in entity_matching_columns  # is_entity_identifier = False
    assert "notes" not in entity_matching_columns  # is_entity_identifier = False
    
    assert len(entity_matching_columns) == 2
    print(f"✅ Phase 1 - Entity matching columns: {list(entity_matching_columns.keys())}")
    
    # Test Phase 2: Get remaining columns
    remaining_columns = strategy._get_remaining_columns(config, entity_matching_columns)
    
    # Should include everything NOT in entity matching
    assert "email" not in remaining_columns
    assert "full_name" not in remaining_columns
    assert "company_name" in remaining_columns  # entity identifier but identity_indication = 0
    assert "deal_stage" in remaining_columns  # descriptive column
    assert "notes" in remaining_columns  # descriptive column
    
    assert len(remaining_columns) == 3
    print(f"✅ Phase 2 - Remaining columns: {list(remaining_columns.keys())}")
    
    # Verify total coverage
    total_columns = len(entity_matching_columns) + len(remaining_columns)
    assert total_columns == len(enrichment_columns)
    print(f"✅ Complete coverage: {total_columns} total columns")
    
    # Test entity matching configuration (should include ALL entity identifiers)
    entity_matching_config = strategy._create_entity_matching_config(config)
    identity_columns = entity_matching_config["identity_columns"]
    
    # Should include ALL entity identifiers, not just those with identity_indication > 0
    assert "email" in identity_columns
    assert "full_name" in identity_columns  
    assert "company_name" in identity_columns  # This should be included even with identity_indication = 0
    assert "deal_stage" not in identity_columns  # Not an entity identifier
    assert "notes" not in identity_columns  # Not an entity identifier
    
    # Check identity strengths
    assert identity_columns["email"]["identity_indication"] == 1.0
    assert identity_columns["full_name"]["identity_indication"] == 0.5
    assert identity_columns["company_name"]["identity_indication"] == 0.0
    
    max_score = entity_matching_config["max_possible_score"]
    expected_max = 1.0 + 0.5 + 0.0  # All entity identifiers contribute to max score
    assert abs(max_score - expected_max) < 0.01
    
    print(f"✅ Entity matching config: {len(identity_columns)} identity columns, max score: {max_score}")
    
    # Test merging strategies
    merging_strategies = strategy._create_merging_strategies(config)
    
    # All entity identifiers use simple merging
    assert merging_strategies["email"]["strategy"] == "prefer_existing_fill_empty"
    assert merging_strategies["full_name"]["strategy"] == "prefer_existing_fill_empty"
    assert merging_strategies["company_name"]["strategy"] == "prefer_existing_fill_empty"
    
    # Descriptive columns use LLM merging
    assert merging_strategies["deal_stage"]["strategy"] == "llm_intelligent_merge"
    assert merging_strategies["notes"]["strategy"] == "llm_intelligent_merge"
    
    print(f"✅ Merging strategies: 3 entity identifiers (simple), 2 descriptive (LLM)")
    
    print("🎉 All phased strategy tests passed!")
    print()
    print("📋 Summary:")
    print(f"   🎯 Phase 1 (Entity Matching): {list(entity_matching_columns.keys())}")
    print(f"   📝 Phase 2 (Remaining): {list(remaining_columns.keys())}")
    print(f"   🔍 Entity Identifiers for Matching: {len(identity_columns)} total")
    print(f"   ⚖️  Merging: 3 simple + 2 LLM strategies")

if __name__ == "__main__":
    test_phased_strategy() 