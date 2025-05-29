#!/usr/bin/env python3
"""
Example usage of InTabular package for intelligent CSV data ingestion.
"""

import os
from pathlib import Path
from intabular import AdaptiveMerger, TableConfig

def main():
    print("🎯 InTabular Package Example")
    print("=" * 50)
    
    # Example 1: Create a table configuration
    print("\n1️⃣ Creating table configuration...")
    
    config = TableConfig(
        purpose="Customer relationship database for sales and marketing",
        enrichment_columns={
            "email": "Primary email address for customer contact",
            "full_name": "Complete customer name for personalization",
            "company_name": "Customer's organization name", 
            "phone": "Primary phone number for contact",
            "location": "Customer location for geographic analysis"
        },
        column_policy="Maintain high data quality. Prefer complete, accurate information.",
        target="example_customers.csv"
    )
    
    # Save configuration
    config_file = "example_config.yaml"
    config.to_yaml(config_file)
    print(f"✅ Configuration saved to: {config_file}")
    
    # Example 2: Programmatic usage (without actual API call)
    print("\n2️⃣ Programmatic API usage...")
    print("Note: This would require an OpenAI API key for full functionality")
    
    try:
        # This will fail without API key, which is expected
        merger = AdaptiveMerger()
        print("✅ AdaptiveMerger initialized")
    except ValueError as e:
        print(f"⚠️  Expected error (no API key): {e}")
        print("   💡 Set OPENAI_API_KEY environment variable for full functionality")
    
    # Example 3: Show CLI usage
    print("\n3️⃣ CLI Usage Examples:")
    print("intabular config customers.csv 'Customer master database'")
    print("intabular customer_config.yaml unknown-leads.csv")
    print("intabular analyze mystery-data.csv")
    
    # Example 4: Show configuration content
    print(f"\n4️⃣ Configuration content ({config_file}):")
    if Path(config_file).exists():
        with open(config_file, 'r') as f:
            print(f.read())
    
    print("\n🎉 Example complete!")
    print("Ready for PyPI packaging! 🚀")

if __name__ == "__main__":
    main() 