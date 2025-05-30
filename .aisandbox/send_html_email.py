#!/usr/bin/env python3
"""
Modified email sender script that can handle HTML content and attachments.
"""

import csv
import smtplib
import json
import sys
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path
import mimetypes


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


def attach_file(msg, file_path):
    """Attach a file to the email message."""
    if not os.path.isfile(file_path):
        print(f"  ⚠️ Warning: Attachment file '{file_path}' not found, skipping.")
        return False
    
    try:
        # Guess the content type based on the file's extension
        ctype, encoding = mimetypes.guess_type(file_path)
        if ctype is None or encoding is not None:
            # No guess could be made, or the file is encoded (compressed), so use a generic bag-of-bits type
            ctype = 'application/octet-stream'
        
        maintype, subtype = ctype.split('/', 1)
        
        with open(file_path, 'rb') as fp:
            attachment = MIMEBase(maintype, subtype)
            attachment.set_payload(fp.read())
        
        # Encode file in ASCII characters to send by email    
        encoders.encode_base64(attachment)
        
        # Add header as key/value pair to attachment part
        filename = os.path.basename(file_path)
        attachment.add_header(
            'Content-Disposition',
            f'attachment; filename= {filename}',
        )
        
        # Attach the part to message
        msg.attach(attachment)
        return True
    except Exception as e:
        print(f"  ❌ Error attaching file '{file_path}': {e}")
        return False


def send_html_email(smtp_server, from_email, to_email, subject, message, attachments=None):
    """Send an HTML email with optional attachments using the provided SMTP server."""
    try:
        # Create message
        msg = MIMEMultipart('mixed')  # Changed to 'mixed' to support both alternative content and attachments
        msg['From'] = from_email
        msg['To'] = to_email
        msg['Subject'] = subject
        
        # Create the main content part
        content_msg = MIMEMultipart('alternative')
        
        # Detect if content is HTML
        if '<html>' in message or '<body>' in message or '<table>' in message:
            # Convert newlines to HTML line breaks for the plain text parts
            # This handles mixed content (plain text + HTML signature)
            
            # Split the message to separate plain text from HTML
            if '<html>' in message:
                parts = message.split('<html>')
                plain_part = parts[0]
                html_part = '<html>' + parts[1] if len(parts) > 1 else ''
            else:
                # Look for other HTML indicators
                html_start_pos = -1
                for html_tag in ['<body>', '<table>', '<div>', '<p style=']:
                    pos = message.find(html_tag)
                    if pos != -1:
                        if html_start_pos == -1 or pos < html_start_pos:
                            html_start_pos = pos
                
                if html_start_pos != -1:
                    plain_part = message[:html_start_pos]
                    html_part = message[html_start_pos:]
                else:
                    plain_part = message
                    html_part = ''
            
            # Convert newlines to <br> in the plain text part
            plain_part_html = plain_part.replace('\n', '<br>\n')
            
            # Combine into proper HTML structure
            if html_part:
                full_html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="font-family: Arial, sans-serif; margin: 0; padding: 20px;">
{plain_part_html}
{html_part}
</body>
</html>"""
            else:
                full_html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="font-family: Arial, sans-serif; margin: 0; padding: 20px;">
{plain_part_html}
</body>
</html>"""
            
            # Add HTML body to content message
            html_part = MIMEText(full_html, 'html', 'utf-8')
            content_msg.attach(html_part)
        else:
            # Add plain text body to content message
            text_part = MIMEText(message, 'plain', 'utf-8')
            content_msg.attach(text_part)
        
        # Attach the content to the main message
        msg.attach(content_msg)
        
        # Handle attachments
        if attachments:
            attachment_count = 0
            for attachment_path in attachments:
                attachment_path = attachment_path.strip()
                if attachment_path:  # Only process non-empty paths
                    if attach_file(msg, attachment_path):
                        attachment_count += 1
            
            if attachment_count > 0:
                print(f"  📎 {attachment_count} attachment(s) added")
        
        # Send email
        smtp_server.send_message(msg)
        return True
    except Exception as e:
        print(f"Failed to send email to {to_email}: {str(e)}")
        return False


def main():
    # Check for custom CSV file argument
    csv_file = sys.argv[1] if len(sys.argv) > 1 else 'emails.csv'
    
    # Load SMTP configuration
    config = load_smtp_config()
    
    # Validate required config fields
    required_fields = ['smtp_server', 'smtp_port', 'username', 'password', 'from_email']
    for field in required_fields:
        if field not in config:
            print(f"Error: Missing required field '{field}' in smtp_config.json")
            sys.exit(1)
    
    # Check if CSV file exists
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
            
            # Validate CSV headers - attachments column is optional
            required_columns = ['email', 'subject', 'message']
            if not all(col in reader.fieldnames for col in required_columns):
                print(f"Error: CSV must contain columns: {', '.join(required_columns)}")
                print(f"Found columns: {', '.join(reader.fieldnames)}")
                sys.exit(1)
            
            # Check if attachments column exists
            has_attachments = 'attachments' in reader.fieldnames
            if has_attachments:
                print("📎 Attachments column detected - files will be attached when specified")
            
            print(f"\nStarting to send emails from {csv_file}...")
            
            for row_num, row in enumerate(reader, start=2):  # Start at 2 because of header
                email = row['email'].strip()
                subject = row['subject'].strip()
                message = row['message'].strip()
                
                if not email or not subject or not message:
                    print(f"Row {row_num}: Skipping due to empty fields")
                    failed_count += 1
                    continue
                
                # Handle attachments if column exists
                attachments = None
                if has_attachments and row.get('attachments', '').strip():
                    # Support multiple attachments separated by semicolons or commas
                    attachment_paths = [path.strip() for path in row['attachments'].replace(';', ',').split(',')]
                    attachments = [path for path in attachment_paths if path]  # Remove empty strings
                
                print(f"Sending to {email}... ", end='')
                
                if send_html_email(server, config['from_email'], email, subject, message, attachments):
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