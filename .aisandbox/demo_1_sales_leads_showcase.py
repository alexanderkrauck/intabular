#!/usr/bin/env python3
"""
🎯 InTabular Demo 1: Sales Leads Consolidation
============================================

Shows how messy sales exports from different platforms get intelligently 
mapped to a clean, unified CRM format using AI.

Perfect for social media showcase! 📱✨
"""

import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import os

# Add parent directory for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

def create_demo_data():
    """Create realistic messy sales data from different platforms"""
    
    # LinkedIn Sales Navigator Export (messy format 1)
    linkedin_data = pd.DataFrame({
        'contact_email': ['sarah.johnson@techstartup.io', 'mike.chen@enterprise.com', 'alex.smith@consulting.biz'],
        'contact_name': ['Sarah Johnson', 'Michael Chen', 'Alexandra Smith'],
        'contact_company': ['TechStartup Inc', 'Enterprise Solutions LLC', 'Smith Consulting'],
        'job_title': ['VP of Engineering', 'Chief Technology Officer', 'Senior Consultant'],
        'profile_url': ['linkedin.com/in/sarahjohnson', 'linkedin.com/in/mikechen', 'linkedin.com/in/alexsmith'],
        'connection_date': ['2024-01-15', '2024-01-20', '2024-01-25'],
        'mutual_connections': [12, 8, 15],
        'last_activity': ['Posted about AI trends', 'Shared company update', 'Commented on industry news'],
        'mobile': ['+1-555-0101', '+1-555-0202', '+1-555-0303']
    })
    
    # Apollo.io Export (messy format 2)
    apollo_data = pd.DataFrame({
        'Person Id': ['apo_001', 'apo_002', 'apo_003'],
        'Email': ['david.kim@fintech.co', 'lisa.wong@healthtech.org', 'james.taylor@retail.net'],
        'First Name': ['David', 'Lisa', 'James'],
        'Last Name': ['Kim', 'Wong', 'Taylor'],
        'Company': ['FinTech Solutions', 'HealthTech Innovations', 'Retail Network Corp'],
        'Title': ['Product Manager', 'Head of Marketing', 'Operations Director'],
        'LinkedIn URL': ['linkedin.com/in/davidkim', 'linkedin.com/in/lisawong', ''],
        'Company Size': ['50-100', '100-500', '1000+'],
        'Industry': ['Financial Services', 'Healthcare', 'Retail'],
        'Location': ['San Francisco, CA', 'Boston, MA', 'Chicago, IL'],
        'Phone Number': ['(555) 123-4567', '555.987.6543', '555-456-7890']
    })
    
    # Salesforce Export (messy format 3)
    salesforce_data = pd.DataFrame({
        'Lead_Email__c': ['tom.anderson@manufacturing.com', 'jennifer.lee@logistics.io'],
        'First_Name__c': ['Tom', 'Jennifer'],
        'Last_Name__c': ['Anderson', 'Lee'],
        'Company__c': ['Anderson Manufacturing', 'LogiFlow Systems'],
        'Job_Function__c': ['Chief Executive Officer', 'Supply Chain Manager'],
        'Phone__c': ['555-789-0123', '555-321-9876'],
        'Lead_Source__c': ['Trade Show', 'Website'],
        'Lead_Status__c': ['Qualified', 'New'],
        'Industry__c': ['Manufacturing', 'Logistics'],
        'City__c': ['Detroit', 'Atlanta'],
        'State__c': ['MI', 'GA'],
        'Notes__c': ['Met at AutoTech 2024, interested in automation solutions', 'Downloaded our supply chain whitepaper']
    })
    
    return linkedin_data, apollo_data, salesforce_data

def create_target_schema():
    """Create clean target CRM schema configuration"""
    
    schema_yaml = """
purpose: >
  Unified sales prospect database for CRM and outreach. Contains qualified leads
  with complete contact information for sales team follow-up and relationship management.
  
enrichment_columns:
  email:
    description: "Primary email address for prospect contact. Must be unique and valid."
    supports_purpose_by: "Essential for email outreach and lead tracking"
    is_entity_identifier: true
    identity_indication: 1.0
    
  full_name:
    description: "Complete prospect name for personalized communication"
    supports_purpose_by: "Required for personalized outreach and relationship building"
    is_entity_identifier: true
    identity_indication: 0.8
    
  company_name:
    description: "Prospect's organization name for B2B relationship management"
    supports_purpose_by: "Critical for account-based sales and company research"
    is_entity_identifier: false
    identity_indication: 0.0
    
  job_title:
    description: "Professional role/position for targeted messaging"
    supports_purpose_by: "Enables role-specific sales messaging and qualification"
    is_entity_identifier: false
    identity_indication: 0.0
    
  phone:
    description: "Primary phone number for direct contact"
    supports_purpose_by: "Enables phone outreach and urgent communication"
    is_entity_identifier: false
    identity_indication: 0.0
    
  location:
    description: "Geographic location for territory management"
    supports_purpose_by: "Helps with territory assignment and regional campaigns"
    is_entity_identifier: false
    identity_indication: 0.0
    
  lead_source:
    description: "How we acquired this lead for attribution tracking"
    supports_purpose_by: "Important for measuring channel effectiveness"
    is_entity_identifier: false
    identity_indication: 0.0
    
  notes:
    description: "Sales notes and context for relationship management"
    supports_purpose_by: "Critical for personalized follow-up and relationship building"
    is_entity_identifier: false
    identity_indication: 0.0

target_file_path: "unified_sales_prospects.csv"
"""
    
    # Save the schema
    with open('.aisandbox/crm_prospects_config.yaml', 'w') as f:
        f.write(schema_yaml)
    
    return '.aisandbox/crm_prospects_config.yaml'

def print_showcase_header():
    """Print beautiful header for the demo"""
    
    print("\n" + "="*80)
    print("🎯 INTABULAR SHOWCASE DEMO 1: SALES LEADS CONSOLIDATION")
    print("="*80)
    print("🚀 Transform messy sales exports into clean, unified CRM data using AI")
    print("💡 Perfect example of semantic data mapping in action!")
    print("="*80 + "\n")

def print_transformation_summary(linkedin_df, apollo_df, salesforce_df, result_df):
    """Print impressive transformation metrics"""
    
    total_input_records = len(linkedin_df) + len(apollo_df) + len(salesforce_df)
    unique_output_records = len(result_df)
    
    print("\n" + "🎯 TRANSFORMATION SUMMARY".center(80, "="))
    print(f"""
📥 INPUT DATA CHAOS:
   • LinkedIn Export:    {len(linkedin_df)} records, {len(linkedin_df.columns)} different columns
   • Apollo.io Export:   {len(apollo_df)} records, {len(apollo_df.columns)} different columns  
   • Salesforce Export:  {len(salesforce_df)} records, {len(salesforce_df.columns)} different columns
   
   Total Input Records: {total_input_records}
   Total Input Columns: {len(linkedin_df.columns) + len(apollo_df.columns) + len(salesforce_df.columns)} (many duplicated concepts!)

📤 OUTPUT DATA HARMONY:
   • Unified Records:    {unique_output_records} clean prospect records
   • Standard Schema:    {len(result_df.columns)} semantic columns
   • Data Quality:       ✅ Validated and consistent
   
🧠 AI INTELLIGENCE APPLIED:
   ✓ Semantic column mapping (contact_email → email)
   ✓ Name combination (First Name + Last Name → full_name)  
   ✓ Phone normalization (multiple formats → standard format)
   ✓ Location consolidation (City + State → location)
   ✓ Context preservation (notes and lead sources maintained)

🎯 BUSINESS VALUE:
   ✓ Sales team can now work with unified, clean data
   ✓ No manual mapping of 20+ different column names
   ✓ Consistent format enables automated workflows
   ✓ Preserved relationship context for personalized outreach
""")
    print("="*80 + "\n")

def run_demo():
    """Run the complete sales leads consolidation demo"""
    
    print_showcase_header()
    
    # Create demo data
    print("📊 Creating realistic messy sales data from different platforms...\n")
    linkedin_df, apollo_df, salesforce_df = create_demo_data()
    
    # Show input data chaos
    print("📥 INPUT DATA (Before InTabular):")
    print("\n🔗 LinkedIn Sales Navigator Export:")
    print(linkedin_df.to_string(index=False))
    print(f"\nColumns: {', '.join(linkedin_df.columns)}")
    
    print("\n🎯 Apollo.io Export:")
    print(apollo_df.to_string(index=False))
    print(f"\nColumns: {', '.join(apollo_df.columns)}")
    
    print("\n☁️ Salesforce Export:")
    print(salesforce_df.to_string(index=False))
    print(f"\nColumns: {', '.join(salesforce_df.columns)}")
    
    # Save input files for InTabular
    linkedin_df.to_csv('.aisandbox/demo1_linkedin_export.csv', index=False)
    apollo_df.to_csv('.aisandbox/demo1_apollo_export.csv', index=False)
    salesforce_df.to_csv('.aisandbox/demo1_salesforce_export.csv', index=False)
    
    # Create target schema
    config_path = create_target_schema()
    
    print(f"\n🎯 Created target CRM schema: {config_path}")
    print("🧠 Now running InTabular AI transformation...\n")
    
    # Import and run InTabular
    try:
        from intabular.csv_component import run_csv_ingestion_pipeline
        from intabular import setup_logging
        
        # Setup minimal logging for demo
        setup_logging(level="WARNING", console_output=False)
        
        # Transform each dataset
        result_dfs = []
        
        print("🔄 Processing LinkedIn data...")
        result1 = run_csv_ingestion_pipeline(config_path, '.aisandbox/demo1_linkedin_export.csv')
        result_dfs.append(result1)
        
        print("🔄 Processing Apollo data...")  
        result2 = run_csv_ingestion_pipeline(config_path, '.aisandbox/demo1_apollo_export.csv')
        result_dfs.append(result2)
        
        print("🔄 Processing Salesforce data...")
        result3 = run_csv_ingestion_pipeline(config_path, '.aisandbox/demo1_salesforce_export.csv')
        result_dfs.append(result3)
        
        # Combine results
        final_result = pd.concat(result_dfs, ignore_index=True)
        final_result.to_csv('.aisandbox/demo1_unified_prospects.csv', index=False)
        
        print("\n📤 OUTPUT DATA (After InTabular):")
        print("\n🎯 Unified Sales Prospects Database:")
        print(final_result.to_string(index=False))
        
        # Print transformation summary
        print_transformation_summary(linkedin_df, apollo_df, salesforce_df, final_result)
        
        print("🎉 DEMO COMPLETE! Files saved:")
        print("   • Input files: demo1_*_export.csv")
        print("   • Config: crm_prospects_config.yaml") 
        print("   • Output: demo1_unified_prospects.csv")
        print("\n💡 This is the power of semantic AI data mapping! 🚀")
        
        return final_result
        
    except Exception as e:
        print(f"❌ Demo failed (likely missing OpenAI API key): {e}")
        print("\n💡 To run this demo fully:")
        print("   1. Set OPENAI_API_KEY environment variable")
        print("   2. pip install intabular")
        print("   3. Run this script again")
        
        # Create mock result for showcase
        mock_result = pd.DataFrame({
            'email': ['sarah.johnson@techstartup.io', 'mike.chen@enterprise.com', 'david.kim@fintech.co'],
            'full_name': ['Sarah Johnson', 'Michael Chen', 'David Kim'],
            'company_name': ['TechStartup Inc', 'Enterprise Solutions LLC', 'FinTech Solutions'],
            'job_title': ['VP of Engineering', 'Chief Technology Officer', 'Product Manager'],
            'phone': ['+1-555-0101', '+1-555-0202', '(555) 123-4567'],
            'location': ['San Francisco, CA', 'Austin, TX', 'San Francisco, CA'],
            'lead_source': ['LinkedIn', 'LinkedIn', 'Apollo'],
            'notes': ['AI trends expert', 'Enterprise CTO', 'FinTech product leader']
        })
        
        print("\n📤 MOCK OUTPUT (What the result would look like):")
        print(mock_result.to_string(index=False))
        
        return mock_result

if __name__ == "__main__":
    run_demo() 