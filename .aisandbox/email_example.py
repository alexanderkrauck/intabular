#!/usr/bin/env python3
"""
Example script showing how to use send_emails.py as a module.
"""

from send_emails import send_emails_from_csv

def send_target_emails():
    """Send emails from target_with_emails.csv with custom column mapping."""
    
    try:
        # Example 1: Using target_with_emails.csv with custom column mapping
        result = send_emails_from_csv(
            csv_file='target_with_emails.csv',
            email_column='email',  # Column name containing email addresses
            message_column='generated_email_body',  # Column name containing the message
            subject='Letzte Gartenwohnung in St. Johann i. d. Haide - Exklusive Gelegenheit',  # Fixed subject for all emails
            attachments=['property_flyer.pdf', 'virtual_tour.mp4'],  # Fixed attachments for all emails
            verbose=True
        )
        
        print(f"✅ Email campaign completed: {result['sent']} sent, {result['failed']} failed")
        return result
        
    except Exception as e:
        print(f"❌ Error sending emails: {e}")
        return None

def send_custom_emails():
    """Example with custom CSV and column-based subjects/attachments."""
    
    try:
        # Example 2: Custom CSV with column-based subjects and attachments
        result = send_emails_from_csv(
            csv_file='custom_emails.csv',
            email_column='recipient_email',  # Different column name
            message_column='email_content',  # Different column name
            subject_column='email_subject',  # Use subjects from a column
            attachments_column='files_to_attach',  # Use attachments from a column
            smtp_config_file='smtp_config.json',
            verbose=True
        )
        
        print(f"✅ Custom email campaign completed: {result['sent']} sent, {result['failed']} failed")
        return result
        
    except Exception as e:
        print(f"❌ Error sending custom emails: {e}")
        return None

def send_simple_emails():
    """Simple example with minimal configuration."""
    
    try:
        # Example 3: Simple usage with auto-detection
        result = send_emails_from_csv(
            csv_file='simple_emails.csv',
            email_column='email',
            message_column='message',
            subject='subject',  # Will auto-detect if 'subject' is a column name
            verbose=False  # Quiet mode
        )
        
        print(f"✅ Simple email campaign: {result}")
        return result
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

if __name__ == "__main__":
    print("🚀 Email Campaign Examples")
    print("=" * 50)
    
    # Run the target emails example
    print("\n📧 Example 1: Sending target emails with fixed subject and attachments")
    send_target_emails()
    
    # Uncomment to test other examples:
    # print("\n📧 Example 2: Custom column mapping")
    # send_custom_emails()
    
    # print("\n📧 Example 3: Simple auto-detection")
    # send_simple_emails() 