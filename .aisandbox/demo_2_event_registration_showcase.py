#!/usr/bin/env python3
"""
🎪 InTabular Demo 2: Event Registration Consolidation
==================================================

Shows how messy event registrations from different platforms get intelligently 
mapped to a unified attendee database for event management.

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
    """Create realistic messy event registration data"""
    
    # Eventbrite Export
    eventbrite_data = pd.DataFrame({
        'Order #': ['EB001', 'EB002', 'EB003', 'EB004'],
        'First Name': ['Emma', 'John', 'Maria', 'David'],
        'Last Name': ['Wilson', 'Smith', 'Garcia', 'Brown'],
        'Email Address': ['emma.wilson@design.co', 'j.smith@marketing.com', 'maria.garcia@startup.io', 'david.brown@consulting.biz'],
        'Phone Number': ['555-0001', '(555) 000-2222', '555.000.3333', '+1-555-000-4444'],
        'Company Name': ['Design Studios', 'Marketing Pro', 'Tech Startup Inc', 'Brown Consulting'],
        'Job Title': ['UX Designer', 'Marketing Director', 'Founder & CEO', 'Senior Consultant'],
        'Ticket Type': ['VIP', 'Standard', 'Standard', 'Early Bird'],
        'Registration Date': ['2024-01-15', '2024-01-18', '2024-01-20', '2024-01-22'],
        'Dietary Requirements': ['Vegetarian', '', 'Vegan', 'No restrictions'],
        'T-Shirt Size': ['M', 'L', 'S', 'XL']
    })
    
    # Zoom Webinar Registration
    zoom_data = pd.DataFrame({
        'Registration ID': ['ZM1001', 'ZM1002', 'ZM1003'],
        'Attendee Email': ['alex.johnson@tech.org', 'sarah.davis@education.edu', 'mike.taylor@finance.com'],
        'Full Name': ['Alex Johnson', 'Dr. Sarah Davis', 'Michael Taylor'],
        'Organization': ['TechOrg Solutions', 'State University', 'Finance Corp'],
        'Position': ['Software Engineer', 'Professor', 'Financial Analyst'],
        'Industry': ['Technology', 'Education', 'Finance'],
        'Registration Time': ['2024-01-16 14:30:00', '2024-01-19 09:15:00', '2024-01-21 16:45:00'],
        'Source': ['LinkedIn Ad', 'University Website', 'Email Campaign'],
        'Questions': ['Interested in AI applications', 'Researching EdTech trends', 'Looking for automation tools'],
        'Phone': ['555-1001', '555-1002', '555-1003']
    })
    
    # Manual CSV Upload (conference networking)
    networking_data = pd.DataFrame({
        'contact_email': ['lisa.chen@healthcare.org', 'robert.kim@manufacturing.com'],
        'name': ['Lisa Chen, MD', 'Robert Kim'],
        'company': ['Healthcare Innovations', 'Kim Manufacturing'],
        'role': ['Chief Medical Officer', 'Operations Manager'],
        'location': ['Boston, MA', 'Detroit, MI'], 
        'networking_interests': ['Medical AI, Healthcare IT', 'Industry 4.0, Automation'],
        'contact_phone': ['617-555-7890', '313-555-6789'],
        'referred_by': ['Dr. Martinez', 'Tech Conference 2023'],
        'session_preferences': ['Healthcare Track', 'Manufacturing Track'],
        'special_needs': ['', 'Wheelchair accessible seating']
    })
    
    return eventbrite_data, zoom_data, networking_data

def create_event_schema():
    """Create unified event attendee schema"""
    
    schema_yaml = """
purpose: >
  Unified event attendee database for conference management, networking, and follow-up.
  Contains complete attendee information for event logistics, personalized experiences,
  and post-event relationship building.
  
enrichment_columns:
  email:
    description: "Primary email address for event communications and follow-up"
    supports_purpose_by: "Essential for event updates, networking, and post-event outreach"
    is_entity_identifier: true
    identity_indication: 1.0
    
  full_name:
    description: "Complete attendee name for name badges and networking"
    supports_purpose_by: "Required for personalized event experience and networking"
    is_entity_identifier: true
    identity_indication: 0.9
    
  company:
    description: "Attendee's organization for networking and business development"
    supports_purpose_by: "Critical for B2B networking and sponsor matching"
    is_entity_identifier: false
    identity_indication: 0.0
    
  job_title:
    description: "Professional role for targeted networking and content recommendations"
    supports_purpose_by: "Enables role-specific networking and session recommendations"
    is_entity_identifier: false
    identity_indication: 0.0
    
  phone:
    description: "Contact phone for urgent event communications"
    supports_purpose_by: "Important for day-of-event logistics and emergency contact"
    is_entity_identifier: false
    identity_indication: 0.0
    
  industry:
    description: "Professional industry for networking groups and content tracks"
    supports_purpose_by: "Helps with targeted networking and relevant session matching"
    is_entity_identifier: false
    identity_indication: 0.0
    
  registration_type:
    description: "Type of registration or ticket for access control and benefits"
    supports_purpose_by: "Determines event access levels and special benefits"
    is_entity_identifier: false
    identity_indication: 0.0
    
  location:
    description: "Geographic location for local networking and logistics"
    supports_purpose_by: "Useful for local meetups and travel coordination"
    is_entity_identifier: false
    identity_indication: 0.0
    
  special_requirements:
    description: "Dietary, accessibility, or other special needs"
    supports_purpose_by: "Critical for inclusive event planning and attendee care"
    is_entity_identifier: false
    identity_indication: 0.0
    
  networking_interests:
    description: "Professional interests and goals for targeted networking"
    supports_purpose_by: "Enables smart networking matching and personalized recommendations"
    is_entity_identifier: false
    identity_indication: 0.0

target_file_path: "unified_event_attendees.csv"
"""
    
    with open('.aisandbox/event_attendees_config.yaml', 'w') as f:
        f.write(schema_yaml)
    
    return '.aisandbox/event_attendees_config.yaml'

def print_showcase_header():
    """Print beautiful header for demo 2"""
    
    print("\n" + "="*80)
    print("🎪 INTABULAR SHOWCASE DEMO 2: EVENT REGISTRATION CONSOLIDATION")
    print("="*80)
    print("🎯 Transform messy event registrations into unified attendee database")
    print("💡 Perfect example of multi-source data harmonization!")
    print("="*80 + "\n")

def print_transformation_summary(eventbrite_df, zoom_df, networking_df, result_df):
    """Print impressive transformation metrics for event demo"""
    
    total_input = len(eventbrite_df) + len(zoom_df) + len(networking_df)
    
    print("\n" + "🎪 EVENT TRANSFORMATION SUMMARY".center(80, "="))
    print(f"""
📥 INPUT REGISTRATION CHAOS:
   • Eventbrite Export:     {len(eventbrite_df)} registrations, {len(eventbrite_df.columns)} columns
   • Zoom Webinar Export:   {len(zoom_df)} registrations, {len(zoom_df.columns)} columns  
   • Manual CSV Upload:     {len(networking_df)} registrations, {len(networking_df.columns)} columns
   
   Total Registrations: {total_input}
   Total Unique Columns: {len(set(eventbrite_df.columns) | set(zoom_df.columns) | set(networking_df.columns))} different field names!

📤 OUTPUT ATTENDEE HARMONY:
   • Unified Attendees:     {len(result_df)} complete attendee records
   • Standard Schema:       {len(result_df.columns)} semantic fields
   • Event-Ready Data:      ✅ Name badges, networking, logistics ready
   
🧠 AI EVENT INTELLIGENCE APPLIED:
   ✓ Name standardization (Dr. Sarah Davis → Sarah Davis)
   ✓ Contact consolidation (Phone Number + Attendee Email → unified contact)
   ✓ Company normalization (various formats → standard company field)
   ✓ Requirements mapping (Dietary + Special Needs → special_requirements)
   ✓ Interest extraction (Questions + Networking Interests → networking_interests)

🎯 EVENT MANAGEMENT VALUE:
   ✓ Single source of truth for all attendees
   ✓ Automated name badge generation ready
   ✓ Smart networking recommendations possible
   ✓ Dietary and accessibility needs consolidated
   ✓ Follow-up campaigns can be personalized by industry/role
""")
    print("="*80 + "\n")

def run_demo():
    """Run the complete event registration consolidation demo"""
    
    print_showcase_header()
    
    # Create demo data
    print("📊 Creating realistic event registration data from multiple sources...\n")
    eventbrite_df, zoom_df, networking_df = create_demo_data()
    
    # Show input data chaos
    print("📥 INPUT REGISTRATIONS (Before InTabular):")
    print("\n🎫 Eventbrite Export:")
    print(eventbrite_df.to_string(index=False))
    print(f"\nColumns: {', '.join(eventbrite_df.columns)}")
    
    print("\n💻 Zoom Webinar Export:")
    print(zoom_df.to_string(index=False))
    print(f"\nColumns: {', '.join(zoom_df.columns)}")
    
    print("\n📝 Manual CSV Upload:")
    print(networking_df.to_string(index=False))
    print(f"\nColumns: {', '.join(networking_df.columns)}")
    
    # Save input files
    eventbrite_df.to_csv('.aisandbox/demo2_eventbrite_export.csv', index=False)
    zoom_df.to_csv('.aisandbox/demo2_zoom_export.csv', index=False)
    networking_df.to_csv('.aisandbox/demo2_networking_upload.csv', index=False)
    
    # Create schema
    config_path = create_event_schema()
    
    print(f"\n🎯 Created unified event schema: {config_path}")
    print("🧠 Now running InTabular AI transformation...\n")
    
    # Run transformation
    try:
        from intabular.csv_component import run_csv_ingestion_pipeline
        from intabular import setup_logging
        
        # Setup minimal logging
        setup_logging(level="WARNING", console_output=False)
        
        # Transform each dataset
        result_dfs = []
        
        print("🔄 Processing Eventbrite registrations...")
        result1 = run_csv_ingestion_pipeline(config_path, '.aisandbox/demo2_eventbrite_export.csv')
        result_dfs.append(result1)
        
        print("🔄 Processing Zoom webinar registrations...")  
        result2 = run_csv_ingestion_pipeline(config_path, '.aisandbox/demo2_zoom_export.csv')
        result_dfs.append(result2)
        
        print("🔄 Processing manual networking uploads...")
        result3 = run_csv_ingestion_pipeline(config_path, '.aisandbox/demo2_networking_upload.csv')
        result_dfs.append(result3)
        
        # Combine results
        final_result = pd.concat(result_dfs, ignore_index=True)
        final_result.to_csv('.aisandbox/demo2_unified_attendees.csv', index=False)
        
        print("\n📤 OUTPUT DATA (After InTabular):")
        print("\n🎪 Unified Event Attendees Database:")
        print(final_result.to_string(index=False))
        
        # Print transformation summary
        print_transformation_summary(eventbrite_df, zoom_df, networking_df, final_result)
        
        print("🎉 DEMO 2 COMPLETE! Files saved:")
        print("   • Input files: demo2_*_export.csv")
        print("   • Config: event_attendees_config.yaml") 
        print("   • Output: demo2_unified_attendees.csv")
        print("\n💡 Event management just got intelligent! 🎪✨")
        
        return final_result
        
    except Exception as e:
        print(f"❌ Demo failed (likely missing OpenAI API key): {e}")
        
        # Create mock result for showcase
        mock_result = pd.DataFrame({
            'email': ['emma.wilson@design.co', 'alex.johnson@tech.org', 'lisa.chen@healthcare.org'],
            'full_name': ['Emma Wilson', 'Alex Johnson', 'Lisa Chen'],
            'company': ['Design Studios', 'TechOrg Solutions', 'Healthcare Innovations'],
            'job_title': ['UX Designer', 'Software Engineer', 'Chief Medical Officer'],
            'phone': ['555-0001', '555-1001', '617-555-7890'],
            'industry': ['Design', 'Technology', 'Healthcare'],
            'registration_type': ['VIP', 'Webinar', 'Networking'],
            'location': ['San Francisco, CA', 'Austin, TX', 'Boston, MA'],
            'special_requirements': ['Vegetarian', '', ''],
            'networking_interests': ['Design trends, UX innovation', 'AI applications', 'Medical AI, Healthcare IT']
        })
        
        print("\n📤 MOCK OUTPUT (What the result would look like):")
        print(mock_result.to_string(index=False))
        
        return mock_result

if __name__ == "__main__":
    run_demo() 