#!/usr/bin/env python3
"""
IMAP Connection Test Script
Tests IMAP connectivity to verify email receiving capabilities.
"""

import imaplib
import json
import sys
import socket


def load_imap_config(config_file='imap_config.json'):
    """Load IMAP configuration from JSON file."""
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"❌ Error: {config_file} not found.")
        print("💡 Create imap_config.json with your IMAP settings:")
        print("""{
    "imap_server": "imap.your-provider.com",
    "imap_port": 993,
    "username": "your_email@example.com",
    "password": "your_password",
    "use_ssl": true
}""")
        return None
    except json.JSONDecodeError:
        print(f"❌ Error: Invalid JSON in {config_file}")
        return None


def test_imap_connection(config):
    """Test IMAP connection and authentication."""
    print(f"📧 Testing IMAP connection to {config['imap_server']}:{config['imap_port']}...")
    
    try:
        if config.get('use_ssl', True):
            mail = imaplib.IMAP4_SSL(config['imap_server'], config['imap_port'])
            print("✅ SSL IMAP connection established")
        else:
            mail = imaplib.IMAP4(config['imap_server'], config['imap_port'])
            print("✅ IMAP connection established")
        
        # Test authentication
        print("🔑 Testing IMAP authentication...")
        mail.login(config['username'], config['password'])
        print("✅ IMAP authentication successful")
        
        # List mailboxes
        print("📂 Listing mailboxes...")
        status, mailboxes = mail.list()
        if status == 'OK':
            print(f"✅ Found {len(mailboxes)} mailboxes")
            for mailbox in mailboxes[:5]:  # Show first 5 mailboxes
                print(f"   📁 {mailbox.decode()}")
            if len(mailboxes) > 5:
                print(f"   ... and {len(mailboxes) - 5} more")
        
        # Select INBOX and get message count
        print("📬 Checking INBOX...")
        status, messages = mail.select('INBOX')
        if status == 'OK':
            message_count = int(messages[0])
            print(f"✅ INBOX contains {message_count} messages")
        
        mail.logout()
        return True
        
    except Exception as e:
        print(f"❌ IMAP test failed: {e}")
        return False


def main():
    print("📬 IMAP Connection Test Script")
    print("=" * 40)
    
    # Load configuration
    config = load_imap_config()
    if not config:
        print("\n❌ Cannot proceed without valid IMAP configuration.")
        sys.exit(1)
    
    print(f"📋 IMAP Configuration:")
    print(f"   Server: {config.get('imap_server', 'N/A')}")
    print(f"   Port: {config.get('imap_port', 'N/A')}")
    print(f"   Username: {config.get('username', 'N/A')}")
    print(f"   Use SSL: {config.get('use_ssl', True)}")
    print()
    
    # Validate required fields
    required_fields = ['imap_server', 'imap_port', 'username', 'password']
    missing_fields = [field for field in required_fields if not config.get(field)]
    
    if missing_fields:
        print(f"❌ Missing required fields: {', '.join(missing_fields)}")
        sys.exit(1)
    
    # Run IMAP test
    if test_imap_connection(config):
        print("\n🎉 IMAP test passed! You can receive emails successfully.")
    else:
        print("\n⚠️  IMAP test failed. Check your configuration.")


if __name__ == "__main__":
    main() 