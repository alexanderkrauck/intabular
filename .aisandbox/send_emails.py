#!/usr/bin/env python3
"""
Email sender script that reads SMTP credentials and sends emails from a CSV file.
Supports HTML content, attachments, and proper formatting.
Can be used as a module or standalone script.
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
from typing import List, Union, Dict, Optional


def load_smtp_config(config_file='smtp_config.json'):
    """Load SMTP configuration from JSON file."""
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: {config_file} not found. Please create it with your SMTP credentials.")
        raise
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in {config_file}")
        raise


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


def send_email(smtp_server, from_email, to_email, subject, message, attachments=None):
    """Send an email with optional HTML content and attachments using the provided SMTP server."""
    try:
        # Create message
        msg = MIMEMultipart('mixed')  # Support both alternative content and attachments
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


def send_emails_from_csv(
    csv_file: str,
    message_column: str,
    email: Union[str, List[str], None] = None,  # Can be column name, fixed email(s), or None
    email_column: Optional[str] = None,  # Alternative way to specify email column
    subject: Union[str, None] = None,  # Can be column name or fixed string
    subject_column: Optional[str] = None,  # Alternative way to specify subject column
    attachments: Union[str, List[str], None] = None,  # Can be column name, fixed file(s), or None
    attachments_column: Optional[str] = None,  # Alternative way to specify attachments column
    smtp_config_file: str = 'smtp_config.json',
    verbose: bool = True
) -> Dict[str, int]:
    """
    Send emails from a CSV file with flexible column mapping.
    
    Args:
        csv_file: Path to the CSV file
        message_column: Name of the column containing email messages
        email: Either a column name (if email_column is None), fixed email address(es), or None
        email_column: Explicit column name for email addresses (alternative to email parameter)
        subject: Either a column name (if subject_column is None) or a fixed subject string for all emails
        subject_column: Explicit column name for subjects (alternative to subject parameter)
        attachments: Either a column name, fixed file path(s), or None
        attachments_column: Explicit column name for attachments (alternative to attachments parameter)
        smtp_config_file: Path to SMTP configuration file
        verbose: Whether to print progress messages
    
    Returns:
        Dict with 'sent', 'failed', and 'total' counts
    """
    
    # Load SMTP configuration
    try:
        config = load_smtp_config(smtp_config_file)
    except Exception:
        raise ValueError(f"Failed to load SMTP configuration from {smtp_config_file}")
    
    # Validate required config fields
    required_fields = ['smtp_server', 'smtp_port', 'username', 'password', 'from_email']
    for field in required_fields:
        if field not in config:
            raise ValueError(f"Missing required field '{field}' in {smtp_config_file}")
    
    # Check if CSV file exists
    if not Path(csv_file).exists():
        raise FileNotFoundError(f"CSV file '{csv_file}' not found")
    
    # Determine email handling
    email_is_column = False
    email_value = email
    if email_column:
        email_is_column = True
        email_value = email_column
    elif email and not email_column:
        # Could be column name or fixed email(s) - we'll determine when reading CSV
        potential_email_column = email if isinstance(email, str) else None
    else:
        potential_email_column = None
        if email is None:
            raise ValueError("Either email or email_column must be provided")
    
    # Determine subject handling
    subject_is_column = False
    subject_value = subject
    if subject_column:
        subject_is_column = True
        subject_value = subject_column
    elif subject and not subject.strip():
        raise ValueError("Subject cannot be empty")
    elif subject is None:
        raise ValueError("Either subject or subject_column must be provided")
    
    # For backward compatibility: if subject looks like a column name and no explicit subject_column
    # we'll check if it exists in the CSV
    if not subject_column and subject and not any(char in subject for char in [' ', '.', '!', '?']):
        # Might be a column name, we'll check when we read the CSV
        potential_subject_column = subject
    else:
        potential_subject_column = None
    
    # Determine attachments handling
    attachments_is_column = False
    attachments_value = attachments
    if attachments_column:
        attachments_is_column = True
        attachments_value = attachments_column
    elif isinstance(attachments, str) and attachments:
        # Could be a column name or a file path - we'll determine when reading CSV
        potential_attachments_column = attachments
    else:
        potential_attachments_column = None
    
    # Connect to SMTP server
    try:
        if verbose:
            print(f"Connecting to SMTP server: {config['smtp_server']}:{config['smtp_port']}")
        
        if config.get('use_tls', True):
            server = smtplib.SMTP(config['smtp_server'], config['smtp_port'])
            server.starttls()
        else:
            server = smtplib.SMTP_SSL(config['smtp_server'], config['smtp_port'])
        
        server.login(config['username'], config['password'])
        if verbose:
            print("Successfully connected and authenticated!")
        
    except Exception as e:
        raise ConnectionError(f"Failed to connect to SMTP server: {e}")
    
    # Read CSV and send emails
    sent_count = 0
    failed_count = 0
    
    try:
        with open(csv_file, 'r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            # Validate message column exists
            if message_column not in reader.fieldnames:
                raise ValueError(f"Message column '{message_column}' not found in CSV. Available columns: {list(reader.fieldnames)}")
            
            # Auto-detect if email is a column name
            if potential_email_column and potential_email_column in reader.fieldnames:
                email_is_column = True
                email_value = potential_email_column
                if verbose:
                    print(f"📧 Detected '{potential_email_column}' as email column")
            elif email_column and email_column in reader.fieldnames:
                email_is_column = True
                email_value = email_column
                if verbose:
                    print(f"📧 Using email column: '{email_column}'")
            elif email and not email_is_column:
                if verbose:
                    if isinstance(email, list):
                        print(f"📧 Using fixed email addresses: {email}")
                    else:
                        print(f"📧 Using fixed email address: '{email}'")
            
            # Auto-detect if subject is a column name
            if potential_subject_column and potential_subject_column in reader.fieldnames:
                subject_is_column = True
                subject_value = potential_subject_column
                if verbose:
                    print(f"📝 Detected '{potential_subject_column}' as subject column")
            elif not subject_column and subject:
                # Subject is a fixed string
                subject_is_column = False
                subject_value = subject
                if verbose:
                    print(f"📝 Using fixed subject: '{subject[:50]}{'...' if len(subject) > 50 else ''}'")
            
            # Auto-detect if attachments is a column name
            if potential_attachments_column and potential_attachments_column in reader.fieldnames:
                attachments_is_column = True
                attachments_value = potential_attachments_column
                if verbose:
                    print(f"📎 Detected '{potential_attachments_column}' as attachments column")
            elif attachments_column and attachments_column in reader.fieldnames:
                attachments_is_column = True
                attachments_value = attachments_column
                if verbose:
                    print(f"📎 Using attachments column: '{attachments_column}'")
            elif attachments and not attachments_is_column:
                if verbose:
                    if isinstance(attachments, list):
                        print(f"📎 Using fixed attachments: {attachments}")
                    else:
                        print(f"📎 Using fixed attachment: '{attachments}'")
            
            if verbose:
                print(f"\nStarting to send emails from {csv_file}...")
            
            # Get list of emails to process
            rows_to_process = list(reader)
            
            # If using fixed email(s), we'll send to those regardless of CSV content
            if not email_is_column:
                if isinstance(email, list):
                    fixed_emails = email
                else:
                    fixed_emails = [email]
                
                # For fixed emails, we send ALL rows to each fixed email address
                for email_addr in fixed_emails:
                    for row_num, row in enumerate(rows_to_process, start=2):
                        message = row[message_column].strip()
                        
                        if not message:
                            if verbose:
                                print(f"Row {row_num}, Email {email_addr}: Skipping due to empty message")
                            failed_count += 1
                            continue
                        
                        # Get subject
                        if subject_is_column:
                            row_subject = row.get(subject_value, '').strip()
                            if not row_subject:
                                if verbose:
                                    print(f"Row {row_num}, Email {email_addr}: Skipping due to empty subject")
                                failed_count += 1
                                continue
                        else:
                            row_subject = subject_value
                        
                        # Get attachments
                        row_attachments = None
                        if attachments_is_column:
                            attachments_str = row.get(attachments_value, '').strip()
                            if attachments_str:
                                # Support multiple attachments separated by semicolons or commas
                                attachment_paths = [path.strip() for path in attachments_str.replace(';', ',').split(',')]
                                row_attachments = [path for path in attachment_paths if path]  # Remove empty strings
                        elif isinstance(attachments, list):
                            row_attachments = attachments
                        elif isinstance(attachments, str) and attachments:
                            # Single attachment file
                            row_attachments = [attachments]
                        
                        if verbose:
                            print(f"Sending to {email_addr} (row {row_num})... ", end='')
                        
                        if send_email(server, config['from_email'], email_addr, row_subject, message, row_attachments):
                            if verbose:
                                print("✓ Sent")
                            sent_count += 1
                        else:
                            if verbose:
                                print("✗ Failed")
                            failed_count += 1
            else:
                # Column-based emails - process each row normally
                for row_num, row in enumerate(rows_to_process, start=2):
                    email_addr = row[email_value].strip()
                    message = row[message_column].strip()
                    
                    if not email_addr or not message:
                        if verbose:
                            print(f"Row {row_num}: Skipping due to empty email or message")
                        failed_count += 1
                        continue
                    
                    # Get subject
                    if subject_is_column:
                        row_subject = row.get(subject_value, '').strip()
                        if not row_subject:
                            if verbose:
                                print(f"Row {row_num}: Skipping due to empty subject")
                            failed_count += 1
                            continue
                    else:
                        row_subject = subject_value
                    
                    # Get attachments
                    row_attachments = None
                    if attachments_is_column:
                        attachments_str = row.get(attachments_value, '').strip()
                        if attachments_str:
                            # Support multiple attachments separated by semicolons or commas
                            attachment_paths = [path.strip() for path in attachments_str.replace(';', ',').split(',')]
                            row_attachments = [path for path in attachment_paths if path]  # Remove empty strings
                    elif isinstance(attachments, list):
                        row_attachments = attachments
                    elif isinstance(attachments, str) and attachments:
                        # Single attachment file
                        row_attachments = [attachments]
                    
                    if verbose:
                        print(f"Sending to {email_addr}... ", end='')
                    
                    if send_email(server, config['from_email'], email_addr, row_subject, message, row_attachments):
                        if verbose:
                            print("✓ Sent")
                        sent_count += 1
                    else:
                        if verbose:
                            print("✗ Failed")
                        failed_count += 1
    
    except Exception as e:
        raise RuntimeError(f"Error processing CSV file: {e}")
    
    finally:
        server.quit()
        if verbose:
            print(f"\n--- Summary ---")
            print(f"Emails sent successfully: {sent_count}")
            print(f"Emails failed: {failed_count}")
            print(f"Total processed: {sent_count + failed_count}")
    
    return {
        'sent': sent_count,
        'failed': failed_count,
        'total': sent_count + failed_count
    }


def main():
    """Command line interface - maintains backward compatibility."""
    # Check for custom CSV file argument
    csv_file = sys.argv[1] if len(sys.argv) > 1 else 'emails.csv'
    
    # Use default column names for backward compatibility
    try:
        result = send_emails_from_csv(
            csv_file=csv_file,
            message_column='message',
            email='email',  # Will auto-detect if it's a column
            subject='subject',  # Will auto-detect if it's a column
            attachments='attachments'  # Will auto-detect if it's a column
        )
        sys.exit(0 if result['failed'] == 0 else 1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 