#!/usr/bin/env python3
"""
Omni-Lead Parser: AI-powered CSV lead parser that automatically detects schemas,
extracts essential information, and prepares leads for outreach campaigns.

This parser uses LLMs to:
1. Detect if CSV has headers or not
2. Identify email columns and validate emails
3. Extract personalization information
4. Detect enrichment platform sources
5. Create a standardized output format for outreach
"""

import csv
import json
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict
from enum import Enum
import pandas as pd
from openai import OpenAI
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class EnrichmentPlatform(Enum):
    APOLLO = "apollo"
    LINKEDIN = "linkedin" 
    INSTAGRAM = "instagram"
    TWITTER = "twitter"
    FACEBOOK = "facebook"
    UNKNOWN = "unknown"

@dataclass
class LeadData:
    """Standardized lead data structure"""
    # Essential fields
    email: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    full_name: Optional[str] = None
    
    # Company information
    company: Optional[str] = None
    title: Optional[str] = None
    industry: Optional[str] = None
    
    # Contact information
    phone: Optional[str] = None
    location: Optional[str] = None
    
    # Social/Web presence
    linkedin_url: Optional[str] = None
    website: Optional[str] = None
    
    # Enrichment tracking
    enrichment_platforms: List[str] = None
    has_personalization_data: bool = False
    needs_enrichment: bool = True
    
    # Additional context
    notes: Optional[str] = None
    raw_data: Dict = None
    
    def __post_init__(self):
        if self.enrichment_platforms is None:
            self.enrichment_platforms = []
        if self.raw_data is None:
            self.raw_data = {}

class LeadParser:
    def __init__(self, api_key: Optional[str] = None):
        """Initialize the parser with OpenAI API key"""
        self.api_key = api_key or os.getenv('OPENAI_API_KEY')
        self.client = None
        self.use_fallback = False
        
        if self.api_key:
            try:
                self.client = OpenAI(api_key=self.api_key)
                print("✅ Using OpenAI LLM for enhanced analysis")
            except Exception as e:
                print(f"⚠️  OpenAI initialization failed: {e}")
                print("🔄 Falling back to basic analysis")
                self.use_fallback = True
        else:
            print("⚠️  No OpenAI API key found")
            print("🔄 Using fallback analysis mode")
            self.use_fallback = True
        
    def analyze_csv_structure(self, file_path: str) -> Dict[str, Any]:
        """Use LLM to analyze CSV structure and detect schema"""
        
        # Read first few rows to analyze
        sample_rows = []
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            for i, row in enumerate(reader):
                sample_rows.append(row)
                if i >= 10:  # Analyze first 10 rows
                    break
        
        # Use fallback if no OpenAI client
        if self.use_fallback or not self.client:
            return self._fallback_analysis(sample_rows)
        
        # Create analysis prompt
        prompt = f"""
        Analyze this CSV data and provide a JSON response with the following structure:
        
        {{
            "has_headers": boolean,
            "detected_columns": {{
                "email": ["column_names_that_contain_emails"],
                "first_name": ["column_names_for_first_name"],
                "last_name": ["column_names_for_last_name"], 
                "full_name": ["column_names_for_full_name"],
                "company": ["column_names_for_company"],
                "title": ["column_names_for_job_title"],
                "phone": ["column_names_for_phone"],
                "location": ["column_names_for_location"],
                "linkedin": ["column_names_for_linkedin"],
                "website": ["column_names_for_website"],
                "industry": ["column_names_for_industry"]
            }},
            "enrichment_indicators": {{
                "apollo": ["column_names_indicating_apollo_source"],
                "linkedin": ["column_names_indicating_linkedin_source"], 
                "instagram": ["column_names_indicating_instagram_source"],
                "twitter": ["column_names_indicating_twitter_source"],
                "facebook": ["column_names_indicating_facebook_source"]
            }},
            "personalization_columns": ["columns_with_rich_personal_info"],
            "confidence_score": float_0_to_1
        }}
        
        CSV Data Sample:
        {chr(10).join([str(row) for row in sample_rows[:5]])}
        
        Consider:
        - Look for email patterns in data
        - Check if first row looks like headers vs data
        - Identify social media URLs, platform mentions
        - Detect rich personalization data (interests, bio, etc.)
        - Return only valid JSON, no explanations
        """
        
        try:
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1
            )
            
            analysis = json.loads(response.choices[0].message.content)
            analysis['sample_rows'] = sample_rows
            return analysis
            
        except Exception as e:
            print(f"Error analyzing CSV structure: {e}")
            # Fallback analysis
            return self._fallback_analysis(sample_rows)
    
    def _fallback_analysis(self, sample_rows: List[List[str]]) -> Dict[str, Any]:
        """Fallback analysis without LLM"""
        if not sample_rows:
            return {"has_headers": False, "detected_columns": {}, "confidence_score": 0.0}
        
        first_row = sample_rows[0]
        
        # Simple heuristic: if first row contains common header terms
        header_indicators = ['email', 'name', 'company', 'phone', 'title', 'linkedin', 'first', 'last']
        has_headers = any(any(indicator in str(cell).lower() for indicator in header_indicators) 
                         for cell in first_row)
        
        detected_columns = {
            'email': [],
            'first_name': [],
            'last_name': [],
            'full_name': [],
            'company': [],
            'title': [],
            'phone': [],
            'location': [],
            'linkedin': [],
            'website': [],
            'industry': []
        }
        
        enrichment_indicators = {
            'apollo': [],
            'linkedin': [],
            'instagram': [],
            'twitter': [],
            'facebook': []
        }
        
        personalization_columns = []
        
        if has_headers:
            # Map column names to fields
            for i, header in enumerate(first_row):
                header_lower = str(header).lower()
                
                # Email detection
                if 'email' in header_lower:
                    detected_columns['email'].append(header)
                
                # Name detection
                if 'first' in header_lower and 'name' in header_lower:
                    detected_columns['first_name'].append(header)
                elif 'last' in header_lower and 'name' in header_lower:
                    detected_columns['last_name'].append(header)
                elif 'name' in header_lower and 'first' not in header_lower and 'last' not in header_lower:
                    detected_columns['full_name'].append(header)
                
                # Company and title
                if 'company' in header_lower or 'organization' in header_lower:
                    detected_columns['company'].append(header)
                if 'title' in header_lower or 'position' in header_lower or 'job' in header_lower:
                    detected_columns['title'].append(header)
                
                # Contact info
                if 'phone' in header_lower:
                    detected_columns['phone'].append(header)
                if 'city' in header_lower or 'state' in header_lower or 'country' in header_lower or 'location' in header_lower:
                    detected_columns['location'].append(header)
                
                # Social/Web
                if 'linkedin' in header_lower:
                    detected_columns['linkedin'].append(header)
                    enrichment_indicators['linkedin'].append(header)
                if 'website' in header_lower or 'url' in header_lower:
                    detected_columns['website'].append(header)
                if 'industry' in header_lower:
                    detected_columns['industry'].append(header)
                
                # Enrichment platforms
                if 'apollo' in header_lower:
                    enrichment_indicators['apollo'].append(header)
                if 'twitter' in header_lower:
                    enrichment_indicators['twitter'].append(header)
                if 'facebook' in header_lower:
                    enrichment_indicators['facebook'].append(header)
                if 'instagram' in header_lower:
                    enrichment_indicators['instagram'].append(header)
                
                # Personalization data (look for long text fields)
                if any(keyword in header_lower for keyword in ['research', 'bio', 'description', 'about', 'custom']):
                    personalization_columns.append(header)
        
        else:
            # No headers - try to detect by data patterns
            if len(sample_rows) > 1:
                data_row = sample_rows[1]
                email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
                
                for i, cell in enumerate(data_row):
                    if email_pattern.search(str(cell)):
                        detected_columns['email'].append(f"column_{i}")
        
        return {
            "has_headers": has_headers,
            "detected_columns": detected_columns,
            "enrichment_indicators": enrichment_indicators,
            "personalization_columns": personalization_columns,
            "confidence_score": 0.7 if has_headers else 0.3,
            "sample_rows": sample_rows
        }
    
    def parse_csv(self, file_path: str) -> Tuple[List[LeadData], Dict[str, Any]]:
        """Parse CSV file and extract lead data"""
        
        print(f"Analyzing CSV structure: {file_path}")
        analysis = self.analyze_csv_structure(file_path)
        
        print(f"Structure analysis confidence: {analysis.get('confidence_score', 0):.2f}")
        print(f"Has headers: {analysis.get('has_headers', False)}")
        
        # Read CSV with pandas for easier manipulation
        try:
            if analysis.get('has_headers', True):
                df = pd.read_csv(file_path)
            else:
                df = pd.read_csv(file_path, header=None)
                # Generate column names
                df.columns = [f'column_{i}' for i in range(len(df.columns))]
        except Exception as e:
            print(f"Error reading CSV: {e}")
            return [], analysis
        
        leads = []
        detected_cols = analysis.get('detected_columns', {})
        
        for _, row in df.iterrows():
            lead = self._extract_lead_from_row(row, detected_cols, analysis)
            if lead.email:  # Only include rows with valid emails
                leads.append(lead)
        
        # Post-process leads
        self._enrich_leads_metadata(leads, analysis)
        
        print(f"Extracted {len(leads)} valid leads from {len(df)} total rows")
        
        return leads, analysis
    
    def _extract_lead_from_row(self, row: pd.Series, detected_cols: Dict, analysis: Dict) -> LeadData:
        """Extract lead data from a single row"""
        lead = LeadData()
        lead.raw_data = row.to_dict()
        
        # Extract email
        email_cols = detected_cols.get('email', [])
        for col in email_cols:
            if col in row.index and pd.notna(row[col]):
                email_candidate = str(row[col]).strip()
                if self._is_valid_email(email_candidate):
                    lead.email = email_candidate
                    break
        
        # Extract names
        first_name_cols = detected_cols.get('first_name', [])
        for col in first_name_cols:
            if col in row.index and pd.notna(row[col]):
                lead.first_name = str(row[col]).strip()
                break
                
        last_name_cols = detected_cols.get('last_name', [])
        for col in last_name_cols:
            if col in row.index and pd.notna(row[col]):
                lead.last_name = str(row[col]).strip()
                break
        
        full_name_cols = detected_cols.get('full_name', [])
        for col in full_name_cols:
            if col in row.index and pd.notna(row[col]):
                lead.full_name = str(row[col]).strip()
                break
        
        # If we have first and last name but no full name, combine them
        if lead.first_name and lead.last_name and not lead.full_name:
            lead.full_name = f"{lead.first_name} {lead.last_name}"
        
        # Extract other fields
        field_mappings = {
            'company': 'company',
            'title': 'title', 
            'phone': 'phone',
            'location': 'location',
            'linkedin': 'linkedin_url',
            'website': 'website',
            'industry': 'industry'
        }
        
        for detected_field, lead_field in field_mappings.items():
            cols = detected_cols.get(detected_field, [])
            for col in cols:
                if col in row.index and pd.notna(row[col]):
                    setattr(lead, lead_field, str(row[col]).strip())
                    break
        
        # Detect enrichment platforms
        enrichment_indicators = analysis.get('enrichment_indicators', {})
        for platform, cols in enrichment_indicators.items():
            for col in cols:
                if col in row.index and pd.notna(row[col]):
                    lead.enrichment_platforms.append(platform)
                    break
        
        # Check for personalization data
        personalization_cols = analysis.get('personalization_columns', [])
        lead.has_personalization_data = any(
            col in row.index and pd.notna(row[col]) and len(str(row[col]).strip()) > 50
            for col in personalization_cols
        )
        
        # Determine if enrichment is needed
        lead.needs_enrichment = not (
            lead.has_personalization_data and 
            len(lead.enrichment_platforms) > 0 and
            lead.linkedin_url
        )
        
        return lead
    
    def _is_valid_email(self, email: str) -> bool:
        """Validate email format"""
        pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
        return bool(pattern.match(email))
    
    def _enrich_leads_metadata(self, leads: List[LeadData], analysis: Dict):
        """Add metadata and notes to leads"""
        total_leads = len(leads)
        needs_enrichment = sum(1 for lead in leads if lead.needs_enrichment)
        has_personalization = sum(1 for lead in leads if lead.has_personalization_data)
        
        for lead in leads:
            notes = []
            if not lead.has_personalization_data:
                notes.append("Low personalization data")
            if lead.needs_enrichment:
                notes.append("Needs enrichment")
            if not lead.linkedin_url:
                notes.append("No LinkedIn profile")
            if not lead.company:
                notes.append("Missing company info")
                
            lead.notes = "; ".join(notes) if notes else "Ready for outreach"
    
    def export_parsed_leads(self, leads: List[LeadData], output_file: str = "parsed_leads.csv"):
        """Export parsed leads to CSV format suitable for outreach"""
        
        if not leads:
            print("No leads to export")
            return
        
        # Convert leads to dictionaries
        lead_dicts = []
        for lead in leads:
            lead_dict = asdict(lead)
            # Convert list to string for CSV compatibility
            lead_dict['enrichment_platforms'] = ','.join(lead.enrichment_platforms)
            # Remove raw_data for cleaner output
            del lead_dict['raw_data']
            lead_dicts.append(lead_dict)
        
        # Create DataFrame and export
        df = pd.DataFrame(lead_dicts)
        df.to_csv(output_file, index=False)
        
        print(f"Exported {len(leads)} leads to {output_file}")
        
        # Print summary statistics
        self._print_summary(leads)
    
    def _print_summary(self, leads: List[LeadData]):
        """Print summary statistics"""
        total = len(leads)
        if total == 0:
            return
            
        with_emails = sum(1 for lead in leads if lead.email)
        with_names = sum(1 for lead in leads if lead.first_name or lead.full_name)
        with_companies = sum(1 for lead in leads if lead.company)
        with_linkedin = sum(1 for lead in leads if lead.linkedin_url)
        needs_enrichment = sum(1 for lead in leads if lead.needs_enrichment)
        has_personalization = sum(1 for lead in leads if lead.has_personalization_data)
        
        print("\n" + "="*50)
        print("LEAD PARSING SUMMARY")
        print("="*50)
        print(f"Total leads processed: {total}")
        print(f"Valid emails: {with_emails} ({with_emails/total*100:.1f}%)")
        print(f"With names: {with_names} ({with_names/total*100:.1f}%)")
        print(f"With companies: {with_companies} ({with_companies/total*100:.1f}%)")
        print(f"With LinkedIn profiles: {with_linkedin} ({with_linkedin/total*100:.1f}%)")
        print(f"Has personalization data: {has_personalization} ({has_personalization/total*100:.1f}%)")
        print(f"Needs enrichment: {needs_enrichment} ({needs_enrichment/total*100:.1f}%)")
        
        # Platform breakdown
        all_platforms = []
        for lead in leads:
            all_platforms.extend(lead.enrichment_platforms)
        
        if all_platforms:
            print("\nEnrichment Platform Sources:")
            from collections import Counter
            platform_counts = Counter(all_platforms)
            for platform, count in platform_counts.most_common():
                print(f"  {platform}: {count}")
        
        print("="*50)

def main():
    """Main function to run the lead parser"""
    if len(sys.argv) < 2:
        print("Usage: python lead_parser.py <csv_file> [output_file]")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else "parsed_leads.csv"
    
    if not Path(input_file).exists():
        print(f"Error: File {input_file} not found")
        sys.exit(1)
    
    try:
        parser = LeadParser()
        leads, analysis = parser.parse_csv(input_file)
        
        if leads:
            parser.export_parsed_leads(leads, output_file)
            print(f"\nSuccess! Parsed leads saved to {output_file}")
            print(f"Ready for email outreach using send_emails.py")
        else:
            print("No valid leads found in the CSV file")
            
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 