#!/usr/bin/env python3
"""
Test script for the enhanced YAML configuration format.
Demonstrates detailed column descriptions and free-form policies.
"""

import sys
import os
import pandas as pd
sys.path.append('..')

from adaptive_merger import AdaptiveMerger, TableConfig, MergePolicy

def create_enhanced_yaml_config():
    """Create the enhanced YAML configuration from the user's example"""
    
    enhanced_config = TableConfig(
        purpose="""Customer relationship data. Contains qualified customers, their contact information,
        and active deal pipeline. This is the authoritative source for customer data and should
        be prioritized in merges. The goal is to have all information required in order to contact
        the customer per email or per phone. All information relevant for this can be stored here.""",
        
        enrichment_columns={
            "email": "Primary email address used for contacting the customer. We only want one email address per customer. Keep the more important one.",
            "full_name": "Full name of the customer",
            "company_name": "Full name of the company the customer works for",
            "deal_stage": "Stage of the deal described in a single sentence",
            "deal_value": "Value of the deal in a single sentence",
            "phone": "Primary phone number used for contacting the customer. There must only be one phone number per customer.",
            "location": "Location of the customer in a sentence",
            "notes": "Notes about the customer. Keep it short and concise.",
            "enrichment_platforms": "Platforms used to enrich the customer data. It should be a dictionary with the platform name, date of enrichment, URL of the enrichment, and the full data that was obtained there in raw. This should not be parsed by the AI, but rather stored as is."
        },
        
        column_policy="I want to keep custom fields, but only if they really support the purpose.",
        merge_priority=3
    )
    
    # Save the enhanced configuration
    enhanced_config.to_yaml('enhanced_customer_config.yaml')
    print("✅ Created enhanced YAML configuration: enhanced_customer_config.yaml")
    
    # Show what was created
    print("\n📝 Configuration Details:")
    print(f"   Purpose: {enhanced_config.purpose[:80]}...")
    print(f"   Enrichment Columns: {len(enhanced_config.enrichment_columns)} detailed specifications")
    print(f"   Column Policy: {enhanced_config.column_policy}")
    print(f"   Merge Priority: {enhanced_config.merge_priority}")
    
    return enhanced_config

def create_sample_customer_data():
    """Create sample customer data matching the enhanced config"""
    
    customer_data = {
        'customer_id': ['CUST001', 'CUST002', 'CUST003', 'CUST004'],
        'email': ['john@acme.com', 'sarah@techco.io', 'mike@startup.com', 'lisa@bigcorp.net'],
        'full_name': ['John Smith', 'Sarah Johnson', 'Mike Brown', 'Lisa Davis'],
        'company_name': ['ACME Corp', 'TechCo', 'Startup Inc', 'BigCorp'],
        'deal_stage': ['Proposal sent', 'Negotiating contract', 'Ready to close', 'Initial contact'],
        'deal_value': ['$50,000 annually', '$125,000 one-time', '$300,000 over 2 years', '$75,000 pilot'],
        'phone': ['+1-555-0101', '+1-555-0202', '+1-555-0303', '+1-555-0404'],
        'location': ['San Francisco, CA', 'New York, NY', 'Austin, TX', 'Boston, MA'],
        'notes': ['Very responsive, technical team', 'Budget approved, waiting on legal', 'Founder decision maker', 'Large enterprise, slow process'],
        'enrichment_platforms': ['Apollo: 2024-01-15', 'LinkedIn Sales Nav: 2024-01-20', 'ZoomInfo: 2024-01-25', 'Apollo: 2024-02-01']
    }
    
    df = pd.DataFrame(customer_data)
    df.to_csv('enhanced_customers.csv', index=False)
    print("✅ Created sample customer data: enhanced_customers.csv")
    return 'enhanced_customers.csv'

def create_marketing_data():
    """Create marketing data with different schema for merging"""
    
    marketing_data = {
        'lead_id': ['LEAD001', 'LEAD002', 'LEAD003', 'LEAD005', 'LEAD006'],
        'contact_email': ['john@acme.com', 'sarah@techco.io', 'new@prospect.com', 'alex@creative.co', 'marketing@bigcorp.net'],
        'first_name': ['John', 'Sarah', 'New', 'Alex', 'Marketing'],
        'last_name': ['Smith', 'Johnson', 'Prospect', 'Wilson', 'Team'],
        'organization': ['ACME Corp', 'TechCo', 'Prospect Co', 'Creative Agency', 'BigCorp'],
        'campaign_source': ['webinar', 'linkedin_ad', 'content_download', 'conference', 'cold_email'],
        'interest_score': [85, 92, 45, 67, 78],
        'last_engagement': ['2024-01-20', '2024-01-25', '2024-02-01', '2024-02-05', '2024-02-10'],
        'marketing_notes': ['Downloaded pricing guide', 'Attended product demo', 'Visited pricing page', 'Met at conference', 'Opened multiple emails']
    }
    
    df = pd.DataFrame(marketing_data)
    df.to_csv('marketing_prospects.csv', index=False)
    
    # Create marketing config
    marketing_config = TableConfig(
        purpose="Marketing lead generation and qualification database. Contains prospects from various marketing channels with engagement tracking and lead scoring.",
        enrichment_columns={
            "contact_email": "Email address from marketing campaigns",
            "campaign_source": "Marketing channel that generated this lead",
            "interest_score": "Numerical score indicating engagement level",
            "marketing_notes": "Campaign interaction and engagement details"
        },
        column_policy="Keep marketing context and engagement data to inform sales approach.",
        merge_priority=1
    )
    
    marketing_config.to_yaml('marketing_prospects_config.yaml')
    print("✅ Created marketing data and config: marketing_prospects.csv")
    return 'marketing_prospects.csv'

def demo_enhanced_yaml_merging():
    """Demonstrate merging with enhanced YAML configurations"""
    
    print("🧠 Enhanced YAML Configuration Demo")
    print("=" * 50)
    
    # Create configurations and data
    customer_config = create_enhanced_yaml_config()
    customer_file = create_sample_customer_data()
    marketing_file = create_marketing_data()
    
    # Initialize merger
    print("\n🔧 Initializing adaptive merger...")
    merger = AdaptiveMerger()
    
    # Load the enhanced configurations
    customer_config_loaded = TableConfig.from_yaml('enhanced_customer_config.yaml')
    marketing_config_loaded = TableConfig.from_yaml('marketing_prospects_config.yaml')
    
    print("\n🔍 Configuration Analysis:")
    print(f"   Customer Config Format: {'Enhanced' if isinstance(customer_config_loaded.enrichment_columns, dict) else 'Basic'}")
    print(f"   Marketing Config Format: {'Enhanced' if isinstance(marketing_config_loaded.enrichment_columns, dict) else 'Basic'}")
    
    # Show enrichment column details
    print(f"\n📋 Customer Enrichment Specifications:")
    for col, desc in customer_config_loaded.enrichment_columns.items():
        print(f"   • {col}: {desc[:60]}...")
    
    print(f"\n📈 Marketing Enrichment Specifications:")
    for col, desc in marketing_config_loaded.enrichment_columns.items():
        print(f"   • {col}: {desc[:60]}...")
    
    # Perform semantic analysis
    print(f"\n🔍 Analyzing table semantics with enhanced configs...")
    customer_analysis = merger.analyze_table_semantics(customer_file, customer_config_loaded)
    marketing_analysis = merger.analyze_table_semantics(marketing_file, marketing_config_loaded)
    
    print(f"   Customer analysis format: {customer_analysis.get('config_format', 'unknown')}")
    print(f"   Marketing analysis format: {marketing_analysis.get('config_format', 'unknown')}")
    
    # Create enhanced merge strategy
    print(f"\n🧠 Creating strategy with detailed column semantics...")
    strategy = merger.create_merge_strategy(
        customer_analysis, marketing_analysis,
        customer_config_loaded, marketing_config_loaded
    )
    
    print(f"   Strategy confidence: {strategy.confidence_score:.2f}")
    print(f"   Join fields identified: {strategy.join_fields}")
    print(f"   Field mappings: {len(strategy.field_mappings)} intelligent mappings")
    
    # Execute merge
    print(f"\n🔀 Executing enhanced merge...")
    result = merger.execute_merge(strategy)
    
    # Save and analyze results
    result.to_csv('enhanced_merged_result.csv', index=False)
    
    print(f"\n📊 Enhanced Merge Results:")
    print(f"   Input records: {len(pd.read_csv(customer_file))} + {len(pd.read_csv(marketing_file))}")
    print(f"   Output records: {len(result)}")
    print(f"   Output columns: {len(result.columns)}")
    print(f"   Columns: {list(result.columns)}")
    
    print(f"\n🎯 Key Benefits of Enhanced YAML:")
    print(f"   ✅ Detailed column descriptions guide intelligent merging")
    print(f"   ✅ Free-form policies allow business-specific rules")
    print(f"   ✅ Rich semantic context improves field mapping")
    print(f"   ✅ Purpose-driven merge strategies")
    print(f"   ✅ Backwards compatible with simple format")
    
    print(f"\n" + "=" * 50)
    print("✅ Enhanced YAML demo complete!")
    print("📁 Check generated files: enhanced_*.csv and *_config.yaml")

if __name__ == "__main__":
    demo_enhanced_yaml_merging() 