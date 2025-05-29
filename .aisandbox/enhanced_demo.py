#!/usr/bin/env python3
"""
Enhanced demo showing the power of semantic table merging.
Creates more realistic tables with varying schemas and naming conventions.
"""

import sys
import os
import pandas as pd
sys.path.append('..')

from adaptive_merger import AdaptiveMerger, TableConfig, MergePolicy

def create_realistic_tables():
    """Create realistic tables with different schemas"""
    
    # CRM Export - Customer data
    crm_data = {
        'customer_id': ['CUST001', 'CUST002', 'CUST003', 'CUST004', 'CUST005'],
        'email_addr': ['john.smith@acme.com', 'sarah.j@techcorp.io', 'm.brown@innovate.co', 'lisa.davis@bigco.net', 'alex@creativeagency.com'],
        'full_name': ['John Smith', 'Sarah Johnson', 'Michael Brown', 'Lisa Davis', 'Alex Wilson'],
        'company_name': ['ACME Corporation', 'TechCorp', 'Innovate Solutions', 'BigCo Industries', 'Creative Agency'],
        'job_position': ['Senior Engineer', 'Product Manager', 'CTO & Founder', 'VP Operations', 'Creative Director'],
        'phone_number': ['+1-555-0101', '+1-555-0202', '+1-555-0303', '+1-555-0404', '+1-555-0505'],
        'last_interaction': ['2024-01-15', '2024-01-20', '2024-01-25', '2024-02-01', '2024-02-05'],
        'deal_stage': ['qualified', 'proposal', 'negotiation', 'qualified', 'proposal'],
        'deal_value': [50000, 125000, 300000, 75000, 90000]
    }
    
    crm_df = pd.DataFrame(crm_data)
    crm_df.to_csv('crm_customers.csv', index=False)
    
    # Marketing Leads - Different naming but similar data
    marketing_data = {
        'lead_uuid': ['LEAD_A1', 'LEAD_B2', 'LEAD_C3', 'LEAD_D4', 'LEAD_E5', 'LEAD_F6'],
        'contact_email': ['john.smith@acme.com', 'sarah.j@techcorp.io', 'unknown@startup.tech', 'new.prospect@enterprise.com', 'alex@creativeagency.com', 'marketing@newcompany.org'],
        'first': ['John', 'Sarah', 'Unknown', 'New', 'Alex', 'Marketing'],
        'last': ['Smith', 'Johnson', 'Contact', 'Prospect', 'Wilson', 'Team'],
        'org': ['ACME Corporation', 'TechCorp', 'Startup Tech', 'Enterprise Corp', 'Creative Agency', 'New Company'],
        'title': ['Senior Engineer', 'Product Manager', 'Developer', 'Director', 'Creative Director', 'Marketing Manager'],
        'lead_source': ['website', 'linkedin', 'referral', 'cold_outreach', 'conference', 'content_download'],
        'interest_level': ['high', 'medium', 'low', 'high', 'medium', 'low'],
        'marketing_notes': ['Attended webinar, very engaged', 'Downloaded whitepaper', 'Minimal engagement', 'Requested demo', 'Met at conference', 'Subscribed to newsletter'],
        'created_date': ['2024-01-10', '2024-01-15', '2024-01-28', '2024-01-30', '2024-02-02', '2024-02-10']
    }
    
    marketing_df = pd.DataFrame(marketing_data)
    marketing_df.to_csv('marketing_leads.csv', index=False)
    
    print("✅ Created realistic test tables:")
    print(f"   🏢 CRM Customers: {len(crm_df)} rows, {len(crm_df.columns)} columns")
    print(f"   📈 Marketing Leads: {len(marketing_df)} rows, {len(marketing_df.columns)} columns")
    print(f"   🔗 Expected overlaps: 3 matching email addresses")
    
    return 'crm_customers.csv', 'marketing_leads.csv'

def create_semantic_configs():
    """Create semantic configurations for realistic tables"""
    
    # CRM configuration - focus on customer lifecycle and deals
    crm_config = TableConfig(
        purpose="Customer relationship management system. Contains qualified customers, their contact information, and active deal pipeline. This is the authoritative source for customer data and should be prioritized in merges.",
        enrichment_columns=['email_addr', 'full_name', 'company_name', 'deal_stage', 'deal_value'],
        column_policy=MergePolicy.COMPREHENSIVE,  # Never lose customer data
        merge_priority=3  # High priority
    )
    crm_config.to_yaml('crm_customers_config.yaml')
    
    # Marketing configuration - focus on lead generation and qualification
    marketing_config = TableConfig(
        purpose="Marketing lead generation database. Contains prospects from various marketing channels that need qualification and nurturing. Should enrich customer records with marketing context and lead sources.",
        enrichment_columns=['contact_email', 'lead_source', 'interest_level', 'marketing_notes'],
        column_policy=MergePolicy.BALANCED,  # Keep important marketing context
        merge_priority=1  # Lower priority than CRM
    )
    marketing_config.to_yaml('marketing_leads_config.yaml')
    
    print("✅ Created semantic configurations:")
    print("   📝 CRM: Comprehensive policy, Priority 3")
    print("   📝 Marketing: Balanced policy, Priority 1")

def demo_semantic_intelligence():
    """Demonstrate semantic intelligence in merging"""
    
    print("🧠 Adaptive Table Merger - Semantic Intelligence Demo")
    print("=" * 60)
    
    # Create test data
    crm_file, marketing_file = create_realistic_tables()
    create_semantic_configs()
    
    # Initialize merger
    print("\n🔧 Initializing merger...")
    merger = AdaptiveMerger()
    
    # Show what we're merging
    print(f"\n📊 Tables to merge:")
    print(f"   Primary (CRM): Customer data with deal pipeline")
    print(f"   Secondary (Marketing): Lead data with marketing context")
    print(f"   Challenge: Different column names, overlapping records")
    
    # Perform merge
    print(f"\n🔀 Executing intelligent merge...")
    result = merger.merge_tables(
        crm_file, 
        marketing_file,
        output_file='intelligent_merge.csv'
    )
    
    # Analyze results
    print(f"\n📈 Merge Analysis:")
    print(f"   📋 Input CRM records: 5")
    print(f"   📋 Input Marketing records: 6") 
    print(f"   📋 Output records: {len(result)}")
    print(f"   📋 Output columns: {len(result.columns)}")
    
    # Show sample merged data
    print(f"\n🔍 Sample merged records:")
    for i, row in result.head(3).iterrows():
        print(f"   Record {i+1}:")
        if pd.notna(row.get('email_addr', row.get('contact_email', ''))):
            email = row.get('email_addr', row.get('contact_email', ''))
            name = row.get('full_name', f"{row.get('first', '')} {row.get('last', '')}")
            company = row.get('company_name', row.get('org', ''))
            print(f"      📧 {email}")
            print(f"      👤 {name}")
            print(f"      🏢 {company}")
            if pd.notna(row.get('deal_value')):
                print(f"      💰 Deal: ${row.get('deal_value', 0):,}")
            if pd.notna(row.get('lead_source')):
                print(f"      📈 Source: {row.get('lead_source', '')}")
    
    print(f"\n" + "=" * 60)
    print("✅ Semantic merge complete! Check 'intelligent_merge.csv'")
    
    # Show what semantic understanding enables
    print(f"\n🎯 Key Benefits of Semantic Understanding:")
    print(f"   ✅ Auto-detected email field variations (email_addr vs contact_email)")
    print(f"   ✅ Mapped name fields (full_name vs first+last)")
    print(f"   ✅ Preserved both deal data and marketing context")
    print(f"   ✅ Respected column policies (comprehensive vs balanced)")
    print(f"   ✅ Applied merge priorities (CRM data preferred)")

if __name__ == "__main__":
    demo_semantic_intelligence() 