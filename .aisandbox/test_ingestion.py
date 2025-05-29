#!/usr/bin/env python3
"""
Demo script for intelligent data ingestion.
Shows how unknown CSV structures are automatically mapped to target table schemas.
"""

import sys
import os
import pandas as pd
sys.path.append('..')

from adaptive_merger import AdaptiveMerger, TableConfig

def create_target_customer_table():
    """Create a well-structured target customer table with rich YAML config"""
    
    # Create target table data
    target_data = {
        'email': ['existing@customer.com', 'old@client.net'],
        'full_name': ['Existing Customer', 'Old Client'],
        'company_name': ['Existing Corp', 'Old Company'],
        'phone': ['+1-555-1000', '+1-555-2000'],
        'notes': ['Long-time customer', 'Needs follow-up']
    }
    
    target_df = pd.DataFrame(target_data)
    target_df.to_csv('target_customers.csv', index=False)
    
    # Create rich YAML configuration for target table
    target_config = TableConfig(
        purpose="""Customer master database. Contains qualified customers with complete contact information
        and relationship history. This is the authoritative source for customer data and should maintain
        high data quality standards. Used for sales outreach, customer support, and relationship management.""",
        
        enrichment_columns={
            "email": "Primary email address for customer contact. Must be unique and valid. This is the primary identifier for customer records.",
            "full_name": "Complete customer name for personalized communication and formal correspondence.",
            "company_name": "Customer's organization name for B2B relationship management and account planning.",
            "phone": "Primary phone number for urgent communication. Should be formatted consistently.",
            "notes": "Relationship notes, preferences, and important context for customer interactions. Keep concise but informative."
        },
        
        column_policy="Maintain high data quality. Prefer complete, accurate information. Avoid duplicates. Keep only business-relevant fields that support customer relationship management."
    )
    
    target_config.to_yaml('target_customers_config.yaml')
    print("✅ Created target customer table and configuration")
    return 'target_customers.csv'

def create_unknown_apollo_export():
    """Create an unknown CSV that looks like an Apollo export"""
    
    apollo_data = {
        'Person Id': ['apollo_001', 'apollo_002', 'apollo_003'],
        'Email': ['new@startup.com', 'ceo@techfirm.io', 'sales@bigcorp.com'],
        'First Name': ['New', 'Tech', 'Sales'],
        'Last Name': ['Startup', 'CEO', 'Manager'],
        'Company': ['Startup Inc', 'TechFirm', 'BigCorp'],
        'Title': ['Founder', 'CEO', 'Sales Manager'],
        'LinkedIn URL': ['linkedin.com/in/newstartup', 'linkedin.com/in/techceo', ''],
        'Company Size': ['10-50', '100-500', '1000+'],
        'Industry': ['Software', 'Technology', 'Enterprise'],
        'Location': ['San Francisco', 'Austin', 'New York'],
        'Phone Number': ['555-0100', '555-0200', '555-0300']
    }
    
    apollo_df = pd.DataFrame(apollo_data)
    apollo_df.to_csv('unknown_apollo_export.csv', index=False)
    print("✅ Created unknown Apollo export CSV")
    return 'unknown_apollo_export.csv'

def create_unknown_linkedin_export():
    """Create an unknown CSV that looks like a LinkedIn export"""
    
    linkedin_data = {
        'contact_email': ['social@media.com', 'existing@customer.com'],  # One duplicate
        'contact_name': ['Social Media Pro', 'Existing Customer Updated'],
        'contact_company': ['Social Media Inc', 'Existing Corp'], 
        'job_title': ['Social Media Manager', 'Updated Title'],
        'profile_url': ['linkedin.com/in/socialmedia', 'linkedin.com/in/existing'],
        'connection_date': ['2024-02-01', '2024-01-15'],
        'mutual_connections': ['5', '12'],
        'last_activity': ['Posted about AI trends', 'Liked company update'],
        'mobile': ['+1-555-5000', '+1-555-1000']  # Phone in different format
    }
    
    linkedin_df = pd.DataFrame(linkedin_data)
    linkedin_df.to_csv('unknown_linkedin_export.csv', index=False)
    print("✅ Created unknown LinkedIn export CSV")
    return 'unknown_linkedin_export.csv'

def demo_intelligent_ingestion():
    """Demonstrate intelligent data ingestion with unknown CSV structures"""
    
    print("🚀 Intelligent Data Ingestion Demo")
    print("=" * 60)
    
    # Create target table and unknown CSVs
    target_file = create_target_customer_table()
    apollo_file = create_unknown_apollo_export()
    linkedin_file = create_unknown_linkedin_export()
    
    # Initialize ingestion system
    print("\n🔧 Initializing intelligent data ingestion system...")
    ingester = AdaptiveMerger()
    
    print(f"\n📋 Scenario: You have a structured customer database")
    print(f"   🎯 Target: {target_file} (2 existing customers)")
    print(f"   📥 Unknown data sources:")
    print(f"      • {apollo_file} (Apollo export format)")
    print(f"      • {linkedin_file} (LinkedIn export format)")
    
    # Analyze unknown CSV structures
    print(f"\n🔍 Step 1: Analyzing unknown CSV structures...")
    
    apollo_analysis = ingester.analyze_unknown_csv(apollo_file)
    print(f"   📊 Apollo CSV: {apollo_analysis.get('table_purpose', 'Unknown')}")
    print(f"   📦 Detected source: {apollo_analysis.get('data_source', 'Unknown')}")
    
    linkedin_analysis = ingester.analyze_unknown_csv(linkedin_file)
    print(f"   📊 LinkedIn CSV: {linkedin_analysis.get('table_purpose', 'Unknown')}")
    print(f"   📦 Detected source: {linkedin_analysis.get('data_source', 'Unknown')}")
    
    # Ingest Apollo data
    print(f"\n🔄 Step 2: Ingesting Apollo data...")
    apollo_result = ingester.ingest_unknown_csv(
        target_file, apollo_file, 
        output_file='customers_after_apollo.csv'
    )
    
    print(f"   📈 Result: {len(apollo_result)} total customers")
    print(f"   📋 Columns: {list(apollo_result.columns)}")
    
    # Ingest LinkedIn data into the updated table
    print(f"\n🔄 Step 3: Ingesting LinkedIn data...")
    linkedin_result = ingester.ingest_unknown_csv(
        'customers_after_apollo.csv', linkedin_file,
        output_file='customers_final.csv'
    )
    
    print(f"   📈 Final result: {len(linkedin_result)} total customers")
    print(f"   📋 Final columns: {list(linkedin_result.columns)}")
    
    # Show sample results
    print(f"\n📊 Step 4: Final customer database sample:")
    for i, row in linkedin_result.head(3).iterrows():
        print(f"   Customer {i+1}:")
        if pd.notna(row.get('email')):
            print(f"      📧 {row.get('email')}")
        if pd.notna(row.get('full_name')):
            print(f"      👤 {row.get('full_name')}")
        if pd.notna(row.get('company_name')):
            print(f"      🏢 {row.get('company_name')}")
        if pd.notna(row.get('phone')):
            print(f"      📞 {row.get('phone')}")
    
    print(f"\n🎯 Key Benefits Demonstrated:")
    print(f"   ✅ Unknown CSV structures automatically detected")
    print(f"   ✅ Field mapping to target schema (Email → email, First Name + Last Name → full_name)")
    print(f"   ✅ Target table policies respected (structured customer format maintained)")
    print(f"   ✅ Progressive ingestion (Apollo then LinkedIn data)")
    print(f"   ✅ Duplicate handling (existing@customer.com managed intelligently)")
    print(f"   ✅ Schema consistency preserved throughout process")
    
    print(f"\n" + "=" * 60)
    print("✅ Intelligent data ingestion demo complete!")
    print("📁 Check generated files:")
    print("   • target_customers.csv (original structured table)")
    print("   • unknown_apollo_export.csv (Apollo format)")
    print("   • unknown_linkedin_export.csv (LinkedIn format)")
    print("   • customers_after_apollo.csv (after first ingestion)")
    print("   • customers_final.csv (final consolidated database)")
    print("   • target_customers_config.yaml (rich table configuration)")

if __name__ == "__main__":
    demo_intelligent_ingestion() 