#!/usr/bin/env python3
"""
Outreach Manager: Complete pipeline from lead parsing to email outreach.
Integrates the lead parser with personalized email generation and sending.
"""

import csv
import json
import sys
from pathlib import Path
from typing import List, Dict, Optional
from lead_parser import LeadParser, LeadData
from send_emails import load_smtp_config, send_email
import smtplib

class OutreachManager:
    def __init__(self, api_key: Optional[str] = None):
        """Initialize the outreach manager"""
        self.parser = LeadParser(api_key) if api_key else None
        
    def parse_and_prepare_outreach(self, csv_file: str, output_file: str = "outreach_ready.csv") -> List[LeadData]:
        """Parse leads and prepare them for outreach"""
        
        if not self.parser:
            try:
                self.parser = LeadParser()
            except ValueError:
                print("Warning: No OpenAI API key found. Using basic parsing...")
                return self._basic_parse(csv_file, output_file)
        
        print("🔍 Parsing leads from CSV...")
        leads, analysis = self.parser.parse_csv(csv_file)
        
        if not leads:
            print("❌ No valid leads found")
            return []
        
        # Filter leads ready for outreach (have email at minimum)
        outreach_ready = [lead for lead in leads if lead.email]
        
        # Export for email sending
        self._export_for_outreach(outreach_ready, output_file)
        
        print(f"✅ Prepared {len(outreach_ready)} leads for outreach")
        return outreach_ready
    
    def _basic_parse(self, csv_file: str, output_file: str) -> List[LeadData]:
        """Basic parsing without LLM"""
        import pandas as pd
        
        df = pd.read_csv(csv_file)
        leads = []
        
        # Simple mapping for common column names
        column_mapping = {
            'email': ['email', 'Email', 'EMAIL', 'email_address'],
            'first_name': ['first_name', 'First Name', 'firstname', 'fname'],
            'last_name': ['last_name', 'Last Name', 'lastname', 'lname'], 
            'company': ['company', 'Company', 'organization', 'org'],
            'title': ['title', 'Title', 'job_title', 'position']
        }
        
        # Find actual columns
        actual_columns = {}
        for field, possible_names in column_mapping.items():
            for col_name in possible_names:
                if col_name in df.columns:
                    actual_columns[field] = col_name
                    break
        
        for _, row in df.iterrows():
            lead = LeadData()
            
            if 'email' in actual_columns:
                lead.email = str(row[actual_columns['email']]).strip()
            if 'first_name' in actual_columns:
                lead.first_name = str(row[actual_columns['first_name']]).strip()
            if 'last_name' in actual_columns:
                lead.last_name = str(row[actual_columns['last_name']]).strip()
            if 'company' in actual_columns:
                lead.company = str(row[actual_columns['company']]).strip()
            if 'title' in actual_columns:
                lead.title = str(row[actual_columns['title']]).strip()
            
            if lead.email and '@' in lead.email:
                leads.append(lead)
        
        self._export_for_outreach(leads, output_file)
        return leads
    
    def _export_for_outreach(self, leads: List[LeadData], output_file: str):
        """Export leads in format compatible with send_emails.py"""
        
        outreach_data = []
        for lead in leads:
            # Create personalized subject and message
            subject = self._generate_subject(lead)
            message = self._generate_message(lead)
            
            outreach_data.append({
                'email': lead.email,
                'subject': subject,
                'message': message,
                'first_name': lead.first_name or '',
                'last_name': lead.last_name or '',
                'company': lead.company or '',
                'title': lead.title or '',
                'needs_enrichment': lead.needs_enrichment,
                'has_personalization': lead.has_personalization_data
            })
        
        # Write to CSV
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            if outreach_data:
                writer = csv.DictWriter(f, fieldnames=outreach_data[0].keys())
                writer.writeheader()
                writer.writerows(outreach_data)
        
        print(f"📧 Outreach data exported to {output_file}")
    
    def _generate_subject(self, lead: LeadData) -> str:
        """Generate personalized subject line"""
        
        if lead.company:
            return f"Partnership opportunity for {lead.company}"
        elif lead.first_name:
            return f"Quick question for you, {lead.first_name}"
        else:
            return "Partnership opportunity"
    
    def _generate_message(self, lead: LeadData) -> str:
        """Generate personalized message"""
        
        # Base template
        greeting = f"Hi {lead.first_name}," if lead.first_name else "Hi there,"
        
        company_line = ""
        if lead.company:
            company_line = f" at {lead.company}"
        
        title_line = ""
        if lead.title:
            title_line = f" I noticed your role as {lead.title}{company_line} and thought you might be interested in a unique opportunity."
        elif lead.company:
            title_line = f" I came across {lead.company} and thought you might be interested in a unique opportunity."
        else:
            title_line = " I thought you might be interested in a unique opportunity."
        
        message = f"""{greeting}

{title_line}

We have a property in St. Johann i. d. Haide, Steiermark, Austria that could be perfect for your business needs or investment portfolio.

Would you be interested in learning more? I'd be happy to share details and arrange a viewing.

Best regards,
Alexander"""

        return message
    
    def send_outreach_emails(self, outreach_file: str = "outreach_ready.csv", limit: Optional[int] = None):
        """Send outreach emails using the prepared CSV"""
        
        if not Path(outreach_file).exists():
            print(f"❌ Outreach file {outreach_file} not found")
            return
        
        # Load SMTP config
        try:
            config = load_smtp_config()
        except Exception as e:
            print(f"❌ SMTP configuration error: {e}")
            return
        
        # Connect to SMTP server
        try:
            print(f"🔗 Connecting to SMTP server...")
            if config.get('use_tls', True):
                server = smtplib.SMTP(config['smtp_server'], config['smtp_port'])
                server.starttls()
            else:
                server = smtplib.SMTP_SSL(config['smtp_server'], config['smtp_port'])
            
            server.login(config['username'], config['password'])
            print("✅ Connected to SMTP server")
            
        except Exception as e:
            print(f"❌ Failed to connect to SMTP server: {e}")
            return
        
        # Send emails
        sent_count = 0
        failed_count = 0
        
        try:
            with open(outreach_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                for i, row in enumerate(reader):
                    if limit and i >= limit:
                        break
                        
                    email = row['email']
                    subject = row['subject']
                    message = row['message']
                    
                    print(f"📧 Sending to {email}...", end=' ')
                    
                    if send_email(server, config['from_email'], email, subject, message):
                        print("✅")
                        sent_count += 1
                    else:
                        print("❌")
                        failed_count += 1
        
        except Exception as e:
            print(f"❌ Error sending emails: {e}")
        
        finally:
            server.quit()
            print(f"\n📊 Email Summary:")
            print(f"   Sent: {sent_count}")
            print(f"   Failed: {failed_count}")
            print(f"   Total: {sent_count + failed_count}")

def main():
    """Main CLI interface"""
    
    if len(sys.argv) < 2:
        print("""
Usage: python outreach_manager.py <command> [options]

Commands:
  parse <csv_file> [output_file]    - Parse leads from CSV
  send [csv_file] [limit]          - Send outreach emails  
  full <csv_file> [limit]          - Complete pipeline: parse + send

Examples:
  python outreach_manager.py parse apollo-contacts-export.csv
  python outreach_manager.py send outreach_ready.csv 10
  python outreach_manager.py full apollo-contacts-export.csv 5
        """)
        sys.exit(1)
    
    command = sys.argv[1]
    manager = OutreachManager()
    
    if command == "parse":
        if len(sys.argv) < 3:
            print("❌ CSV file required for parse command")
            sys.exit(1)
        
        csv_file = sys.argv[2]
        output_file = sys.argv[3] if len(sys.argv) > 3 else "outreach_ready.csv"
        
        leads = manager.parse_and_prepare_outreach(csv_file, output_file)
        print(f"✅ Parsing complete. {len(leads)} leads ready for outreach.")
    
    elif command == "send":
        csv_file = sys.argv[2] if len(sys.argv) > 2 else "outreach_ready.csv"
        limit = int(sys.argv[3]) if len(sys.argv) > 3 else None
        
        manager.send_outreach_emails(csv_file, limit)
    
    elif command == "full":
        if len(sys.argv) < 3:
            print("❌ CSV file required for full command")
            sys.exit(1)
        
        csv_file = sys.argv[2]
        limit = int(sys.argv[3]) if len(sys.argv) > 3 else None
        
        print("🚀 Starting full outreach pipeline...")
        
        # Parse leads
        leads = manager.parse_and_prepare_outreach(csv_file)
        
        if leads:
            # Send emails
            manager.send_outreach_emails("outreach_ready.csv", limit)
        else:
            print("❌ No leads to send emails to")
    
    else:
        print(f"❌ Unknown command: {command}")
        sys.exit(1)

if __name__ == "__main__":
    main() 