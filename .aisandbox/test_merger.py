#!/usr/bin/env python3
"""
Test script for the Adaptive Table Merger.
Creates sample tables and demonstrates intelligent merging.
"""

import sys
import os
import pandas as pd
sys.path.append('..')

from adaptive_merger import AdaptiveMerger, TableConfig, MergePolicy

def create_sample_tables():
    """Create sample tables for testing"""
    
    # Create a leads table
    leads_data = {
        'lead_id': ['L001', 'L002', 'L003', 'L004', 'L005'],
        'email': ['john@acme.com', 'sarah@techco.com', 'mike@startup.io', 'lisa@corp.com', 'alex@agency.net'],
        'first_name': ['John', 'Sarah', 'Mike', 'Lisa', 'Alex'],
        'last_name': ['Smith', 'Johnson', 'Brown', 'Davis', 'Wilson'],
        'company': ['ACME Corp', 'TechCo', 'Startup Inc', 'Big Corp', 'Creative Agency'],
        'status': ['new', 'contacted', 'qualified', 'new', 'qualified'],
        'source': ['website', 'linkedin', 'referral', 'cold_email', 'conference']
    }
    
    leads_df = pd.DataFrame(leads_data)
    leads_df.to_csv('sample_leads.csv', index=False)
    
    # Create a contacts table with overlapping but different schema
    contacts_data = {
        'contact_id': ['C001', 'C002', 'C003', 'C006', 'C007'],
        'email_address': ['john@acme.com', 'sarah@techco.com', 'mike@startup.io', 'new@company.com', 'another@firm.co'],
        'full_name': ['John Smith', 'Sarah Johnson', 'Mike Brown', 'New Contact', 'Another Person'],
        'organization': ['ACME Corp', 'TechCo', 'Startup Inc', 'New Company', 'Another Firm'],
        'job_title': ['Engineer', 'Manager', 'Founder', 'Director', 'Consultant'],
        'phone': ['+1-555-0101', '+1-555-0102', '+1-555-0103', '+1-555-0104', '+1-555-0105'],
        'notes': ['Interested in product demo', 'Wants pricing info', 'Ready to buy', 'Follow up needed', 'Not interested'],
        'last_contact_date': ['2024-01-15', '2024-01-20', '2024-01-25', '2024-01-30', '2024-02-01']
    }
    
    contacts_df = pd.DataFrame(contacts_data)
    contacts_df.to_csv('sample_contacts.csv', index=False)
    
    print("✅ Created sample tables:")
    print(f"   📋 Leads: {len(leads_df)} rows, {len(leads_df.columns)} columns")
    print(f"   📋 Contacts: {len(contacts_df)} rows, {len(contacts_df.columns)} columns")
    
    return 'sample_leads.csv', 'sample_contacts.csv'

def create_sample_configs():
    """Create sample YAML configurations"""
    
    # Leads table configuration
    leads_config = TableConfig(
        purpose="Lead generation and qualification database. Contains prospects from various sources that need to be contacted and converted to customers.",
        enrichment_columns=['email', 'company', 'first_name', 'last_name'],
        column_policy=MergePolicy.BALANCED,
        merge_priority=2
    )
    leads_config.to_yaml('sample_leads_config.yaml')
    
    # Contacts table configuration  
    contacts_config = TableConfig(
        purpose="Customer relationship management contacts. Contains detailed interaction history and contact information for active prospects and customers.",
        enrichment_columns=['email_address', 'full_name', 'organization', 'notes'],
        column_policy=MergePolicy.COMPREHENSIVE,
        merge_priority=1
    )
    contacts_config.to_yaml('sample_contacts_config.yaml')
    
    print("✅ Created configuration files:")
    print("   📝 sample_leads_config.yaml")
    print("   📝 sample_contacts_config.yaml")

def demo_adaptive_merger():
    """Demonstrate the adaptive merger functionality"""
    
    print("🚀 Adaptive Table Merger Demo")
    print("=" * 50)
    
    # Create sample data
    leads_file, contacts_file = create_sample_tables()
    create_sample_configs()
    
    # Initialize merger
    print("\n🔧 Initializing Adaptive Merger...")
    merger = AdaptiveMerger()
    
    # Load configurations
    leads_config = TableConfig.from_yaml('sample_leads_config.yaml')
    contacts_config = TableConfig.from_yaml('sample_contacts_config.yaml')
    
    print(f"\n📋 Table Configurations:")
    print(f"   Leads Purpose: {leads_config.purpose[:60]}...")
    print(f"   Leads Policy: {leads_config.column_policy.value}")
    print(f"   Contacts Purpose: {contacts_config.purpose[:60]}...")
    print(f"   Contacts Policy: {contacts_config.column_policy.value}")
    
    # Analyze tables individually
    print(f"\n🔍 Analyzing table semantics...")
    leads_analysis = merger.analyze_table_semantics(leads_file, leads_config)
    contacts_analysis = merger.analyze_table_semantics(contacts_file, contacts_config)
    
    print(f"   📊 Leads analysis confidence: {leads_analysis.get('confidence', 'N/A')}")
    print(f"   📊 Contacts analysis confidence: {contacts_analysis.get('confidence', 'N/A')}")
    
    # Create merge strategy
    print(f"\n🧠 Creating intelligent merge strategy...")
    strategy = merger.create_merge_strategy(leads_analysis, contacts_analysis, 
                                          leads_config, contacts_config)
    
    print(f"   🎯 Strategy confidence: {strategy.confidence_score:.2f}")
    print(f"   🔗 Join fields: {strategy.join_fields}")
    print(f"   🗺️  Field mappings: {len(strategy.field_mappings)} mappings")
    print(f"   ⚖️  Fusion rules: {len(strategy.fusion_rules)} rules")
    
    # Execute merge
    print(f"\n🔀 Executing merge...")
    merged_df = merger.execute_merge(strategy)
    
    # Save and display results
    output_file = 'merged_demo.csv'
    merged_df.to_csv(output_file, index=False)
    
    print(f"\n📊 Merge Results:")
    print(f"   📁 Output file: {output_file}")
    print(f"   📈 Final table: {len(merged_df)} rows, {len(merged_df.columns)} columns")
    print(f"   📋 Columns: {list(merged_df.columns)}")
    
    # Show sample of merged data
    print(f"\n🔍 Sample merged data:")
    print(merged_df.head(3).to_string(index=False))
    
    print(f"\n" + "=" * 50)
    print("✅ Demo complete! Check the output files in .aisandbox/")

if __name__ == "__main__":
    demo_adaptive_merger() 