#!/usr/bin/env python3
"""
Email sender script that reads SMTP credentials and sends emails from a CSV file.
"""

import csv
import smtplib
import json
import sys
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path


def load_smtp_config(config_file='smtp_config.json'):
    """Load SMTP configuration from JSON file."""
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: {config_file} not found. Please create it with your SMTP credentials.")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in {config_file}")
        sys.exit(1)


def send_email(smtp_server, from_email, to_email, subject, message):
    """Send an email using the provided SMTP server."""
    try:
        # Create message
        msg = MIMEMultipart()
        msg['From'] = from_email
        msg['To'] = to_email
        msg['Subject'] = subject
        
        # Add body to email
        msg.attach(MIMEText(message, 'plain'))
        
        # Send email
        smtp_server.send_message(msg)
        return True
    except Exception as e:
        print(f"Failed to send email to {to_email}: {str(e)}")
        return False


def main():
    # Load SMTP configuration
    config = load_smtp_config()
    
    # Validate required config fields
    required_fields = ['smtp_server', 'smtp_port', 'username', 'password', 'from_email']
    for field in required_fields:
        if field not in config:
            print(f"Error: Missing required field '{field}' in smtp_config.json")
            sys.exit(1)
    
    # CSV file path
    csv_file = 'emails.csv'
    if not Path(csv_file).exists():
        print(f"Error: {csv_file} not found. Please create it with email data.")
        sys.exit(1)
    
    # Connect to SMTP server
    try:
        print(f"Connecting to SMTP server: {config['smtp_server']}:{config['smtp_port']}")
        
        if config.get('use_tls', True):
            server = smtplib.SMTP(config['smtp_server'], config['smtp_port'])
            server.starttls()
        else:
            server = smtplib.SMTP_SSL(config['smtp_server'], config['smtp_port'])
        
        server.login(config['username'], config['password'])
        print("Successfully connected and authenticated!")
        
    except Exception as e:
        print(f"Failed to connect to SMTP server: {str(e)}")
        sys.exit(1)
    
    # Read CSV and send emails
    sent_count = 0
    failed_count = 0
    
    try:
        with open(csv_file, 'r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            # Validate CSV headers
            required_columns = ['email', 'subject', 'message']
            if not all(col in reader.fieldnames for col in required_columns):
                print(f"Error: CSV must contain columns: {', '.join(required_columns)}")
                print(f"Found columns: {', '.join(reader.fieldnames)}")
                sys.exit(1)
            
            print(f"\nStarting to send emails...")
            
            for row_num, row in enumerate(reader, start=2):  # Start at 2 because of header
                email = row['email'].strip()
                subject = row['subject'].strip()
                message = row['message'].strip()
                
                if not email or not subject or not message:
                    print(f"Row {row_num}: Skipping due to empty fields")
                    failed_count += 1
                    continue
                
                print(f"Sending to {email}... ", end='')
                
                if send_email(server, config['from_email'], email, subject, message):
                    print("✓ Sent")
                    sent_count += 1
                else:
                    print("✗ Failed")
                    failed_count += 1
    
    except Exception as e:
        print(f"Error reading CSV file: {str(e)}")
    
    finally:
        server.quit()
        print(f"\n--- Summary ---")
        print(f"Emails sent successfully: {sent_count}")
        print(f"Emails failed: {failed_count}")
        print(f"Total processed: {sent_count + failed_count}")


if __name__ == "__main__":
    main() 