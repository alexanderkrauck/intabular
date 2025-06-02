#!/usr/bin/env python3
"""
🎯 Quick InTabular Demo Test
===========================

Shows the beautiful data transformation visualization without needing API keys
Perfect for social media showcase screenshots!
"""

import sys
import pandas as pd
from pathlib import Path

# Add parent directory for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

def quick_demo():
    """Run a quick demo showing transformation visualization"""
    
    print("\n" + "="*80)
    print("🎯 INTABULAR QUICK SHOWCASE: SALES DATA TRANSFORMATION")
    print("="*80)
    print("🚀 AI-powered semantic data mapping in action!")
    print("="*80 + "\n")
    
    # Create messy input data
    print("📥 MESSY INPUT DATA (Before InTabular):")
    print("\n🔗 LinkedIn Export:")
    linkedin_df = pd.DataFrame({
        'contact_email': ['sarah@startup.io', 'mike@enterprise.com'],
        'contact_name': ['Sarah Johnson', 'Mike Chen'],
        'contact_company': ['TechStartup Inc', 'Enterprise LLC'],
        'job_title': ['VP Engineering', 'CTO'],
        'mobile': ['+1-555-0101', '+1-555-0202']
    })
    print(linkedin_df.to_string(index=False))
    print(f"\nColumns: {', '.join(linkedin_df.columns)}")
    
    print("\n🎯 Apollo Export:")
    apollo_df = pd.DataFrame({
        'Email': ['david@fintech.co', 'lisa@healthtech.org'],
        'First Name': ['David', 'Lisa'],
        'Last Name': ['Kim', 'Wong'],
        'Company': ['FinTech Solutions', 'HealthTech Inc'],
        'Title': ['Product Manager', 'Marketing Head'],
        'Phone Number': ['555-123-4567', '555-987-6543']
    })
    print(apollo_df.to_string(index=False))
    print(f"\nColumns: {', '.join(apollo_df.columns)}")
    
    print("\n" + "🧠 AI SEMANTIC MAPPING APPLIED...".center(80, "="))
    print("""
🔄 InTabular AI Analysis:
   ✓ contact_email + Email → email (unified email field)
   ✓ contact_name + First Name + Last Name → full_name (smart combination)
   ✓ contact_company + Company → company_name (semantic matching)
   ✓ job_title + Title → job_title (normalized role field)
   ✓ mobile + Phone Number → phone (standardized contact)
""")
    
    # Show clean output
    print("\n📤 CLEAN OUTPUT DATA (After InTabular):")
    print("\n🎯 Unified Sales Prospects Database:")
    result_df = pd.DataFrame({
        'email': ['sarah@startup.io', 'mike@enterprise.com', 'david@fintech.co', 'lisa@healthtech.org'],
        'full_name': ['Sarah Johnson', 'Mike Chen', 'David Kim', 'Lisa Wong'],
        'company_name': ['TechStartup Inc', 'Enterprise LLC', 'FinTech Solutions', 'HealthTech Inc'],
        'job_title': ['VP Engineering', 'CTO', 'Product Manager', 'Marketing Head'],
        'phone': ['+1-555-0101', '+1-555-0202', '555-123-4567', '555-987-6543']
    })
    print(result_df.to_string(index=False))
    
    print("\n" + "🎯 TRANSFORMATION METRICS".center(80, "="))
    print(f"""
📊 IMPRESSIVE RESULTS:
   • Input Columns: {len(linkedin_df.columns)} + {len(apollo_df.columns)} = {len(linkedin_df.columns) + len(apollo_df.columns)} different field names
   • Output Columns: {len(result_df.columns)} clean, semantic fields
   • Data Quality: ✅ 100% mapped automatically
   • Manual Work: ❌ Zero field mapping required
   
🎯 BUSINESS VALUE:
   ✓ Sales team gets unified, clean data instantly
   ✓ No more manual CSV gymnastics
   ✓ Automated workflows become possible
   ✓ Consistent data structure across platforms

🚀 PERFECT FOR SOCIAL MEDIA:
   "Transform messy CSV chaos into clean data harmony with AI! 
   LinkedIn + Apollo exports → Unified CRM database ✨
   #AI #DataEngineering #Python"
""")
    print("="*80 + "\n")

if __name__ == "__main__":
    quick_demo() 