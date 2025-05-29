#!/usr/bin/env python3
"""
Demo script to test the lead parser with the Apollo CSV file.
This script is for testing/debugging purposes only.
"""

import sys
import os
sys.path.append('..')

from lead_parser import LeadParser

def main():
    # Test with the Apollo CSV file
    csv_file = "../apollo-contacts-export(1).csv"
    
    print("=" * 60)
    print("LEAD PARSER DEMO")
    print("=" * 60)
    
    try:
        # Initialize parser (will use fallback if no OpenAI key)
        print("Initializing parser...")
        try:
            parser = LeadParser()
            print("✓ Using OpenAI LLM for analysis")
        except ValueError:
            print("⚠ No OpenAI API key found - using fallback analysis")
            print("Set OPENAI_API_KEY environment variable for full functionality")
            # Create a mock parser for demo
            class MockParser:
                def analyze_csv_structure(self, file_path):
                    return {
                        "has_headers": True,
                        "detected_columns": {
                            "email": ["Email"],
                            "first_name": ["First Name"],
                            "last_name": ["Last Name"],
                            "company": ["Company"],
                            "title": ["Title"],
                            "phone": ["Work Direct Phone"],
                            "location": ["City", "State", "Country"],
                            "linkedin": ["Person Linkedin Url"],
                            "website": ["Website"],
                            "industry": ["Industry"]
                        },
                        "enrichment_indicators": {
                            "apollo": ["Apollo Contact Id", "Primary Email Source"],
                            "linkedin": ["Person Linkedin Url"]
                        },
                        "personalization_columns": ["CustomAIResearchBasic"],
                        "confidence_score": 0.95
                    }
                
                def parse_csv(self, file_path):
                    from lead_parser import LeadData
                    import pandas as pd
                    
                    analysis = self.analyze_csv_structure(file_path)
                    
                    # Read CSV
                    df = pd.read_csv(file_path)
                    leads = []
                    
                    # Process first 5 rows for demo
                    for i, (_, row) in enumerate(df.head(5).iterrows()):
                        lead = LeadData()
                        lead.email = row.get('Email', '')
                        lead.first_name = row.get('First Name', '')
                        lead.last_name = row.get('Last Name', '')
                        lead.company = row.get('Company', '')
                        lead.title = row.get('Title', '')
                        lead.linkedin_url = row.get('Person Linkedin Url', '')
                        lead.enrichment_platforms = ['apollo']
                        lead.has_personalization_data = len(str(row.get('CustomAIResearchBasic', ''))) > 50
                        lead.needs_enrichment = not lead.has_personalization_data
                        
                        if lead.email:
                            leads.append(lead)
                    
                    return leads, analysis
                
                def export_parsed_leads(self, leads, output_file):
                    print(f"Demo: Would export {len(leads)} leads to {output_file}")
                    
                def _print_summary(self, leads):
                    print(f"Demo processed {len(leads)} leads")
                    
            parser = MockParser()
        
        # Parse the CSV
        print(f"Parsing CSV file: {csv_file}")
        leads, analysis = parser.parse_csv(csv_file)
        
        # Show results
        print(f"\nFound {len(leads)} valid leads")
        
        if leads:
            print("\nSample leads:")
            for i, lead in enumerate(leads[:3]):
                print(f"\nLead {i+1}:")
                print(f"  Email: {lead.email}")
                print(f"  Name: {lead.first_name} {lead.last_name}")
                print(f"  Company: {lead.company}")
                print(f"  Title: {lead.title}")
                print(f"  Enrichment: {lead.enrichment_platforms}")
                print(f"  Has personalization: {lead.has_personalization_data}")
                print(f"  Needs enrichment: {lead.needs_enrichment}")
        
        # Export demo
        if hasattr(parser, 'export_parsed_leads') and callable(parser.export_parsed_leads):
            parser.export_parsed_leads(leads, ".aisandbox/demo_output.csv")
            if hasattr(parser, '_print_summary'):
                parser._print_summary(leads)
        
        print("\n" + "=" * 60)
        print("DEMO COMPLETE")
        print("=" * 60)
        
    except Exception as e:
        print(f"Demo error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 