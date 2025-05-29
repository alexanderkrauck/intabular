#!/usr/bin/env python3
"""
SMTP Connection Test Script
Tests SMTP connectivity and sends a test email to verify email functionality.
"""

import smtplib
import json
import sys
import socket
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime


def load_smtp_config(config_file='smtp_config.json'):
    """Load SMTP configuration from JSON file."""
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"❌ Error: {config_file} not found.")
        return None
    except json.JSONDecodeError:
        print(f"❌ Error: Invalid JSON in {config_file}")
        return None


def test_dns_resolution(smtp_server):
    """Test if the SMTP server hostname can be resolved."""
    print(f"🔍 Testing DNS resolution for {smtp_server}...")
    try:
        ip = socket.gethostbyname(smtp_server)
        print(f"✅ DNS resolution successful: {smtp_server} -> {ip}")
        return True
    except socket.gaierror as e:
        print(f"❌ DNS resolution failed: {e}")
        return False


def test_connection(smtp_server, smtp_port, use_tls=True):
    """Test basic connection to SMTP server."""
    print(f"🔌 Testing connection to {smtp_server}:{smtp_port}...")
    try:
        if use_tls:
            server = smtplib.SMTP(smtp_server, smtp_port, timeout=10)
            print(f"✅ Connected to {smtp_server}:{smtp_port}")
            
            print("🔐 Starting TLS...")
            server.starttls()
            print("✅ TLS connection established")
        else:
            server = smtplib.SMTP_SSL(smtp_server, smtp_port, timeout=10)
            print(f"✅ SSL connection established to {smtp_server}:{smtp_port}")
        
        server.quit()
        return True
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False


def test_authentication(config):
    """Test SMTP authentication."""
    print(f"🔑 Testing authentication...")
    try:
        if config.get('use_tls', True):
            server = smtplib.SMTP(config['smtp_server'], config['smtp_port'], timeout=10)
            server.starttls()
        else:
            server = smtplib.SMTP_SSL(config['smtp_server'], config['smtp_port'], timeout=10)
        
        server.login(config['username'], config['password'])
        print("✅ Authentication successful")
        server.quit()
        return True
    except smtplib.SMTPAuthenticationError as e:
        print(f"❌ Authentication failed: {e}")
        return False
    except Exception as e:
        print(f"❌ Authentication error: {e}")
        return False


def send_test_email(config, test_email=None):
    """Send a test email."""
    if not test_email:
        test_email = config['from_email']  # Send to self if no test email provided
    
    print(f"📧 Sending test email to {test_email}...")
    
    try:
        # Create test message
        msg = MIMEMultipart()
        msg['From'] = config['from_email']
        msg['To'] = test_email
        msg['Subject'] = f"SMTP Test - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
        body = f"""
This is a test email sent from the SMTP testing script.

Test details:
- SMTP Server: {config['smtp_server']}:{config['smtp_port']}
- From: {config['from_email']}
- To: {test_email}
- Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
- TLS: {'Enabled' if config.get('use_tls', True) else 'Disabled'}

If you received this email, your SMTP configuration is working correctly!
        """
        
        msg.attach(MIMEText(body, 'plain'))
        
        # Connect and send
        if config.get('use_tls', True):
            server = smtplib.SMTP(config['smtp_server'], config['smtp_port'], timeout=10)
            server.starttls()
        else:
            server = smtplib.SMTP_SSL(config['smtp_server'], config['smtp_port'], timeout=10)
        
        server.login(config['username'], config['password'])
        server.send_message(msg)
        server.quit()
        
        print(f"✅ Test email sent successfully to {test_email}")
        print(f"📬 Check the inbox for {test_email} to confirm delivery")
        return True
        
    except Exception as e:
        print(f"❌ Failed to send test email: {e}")
        return False


def main():
    print("🧪 SMTP Connection & Email Test Script")
    print("=" * 50)
    
    # Load configuration
    config = load_smtp_config()
    if not config:
        print("\n❌ Cannot proceed without valid SMTP configuration.")
        sys.exit(1)
    
    print(f"📋 Configuration loaded:")
    print(f"   Server: {config.get('smtp_server', 'N/A')}")
    print(f"   Port: {config.get('smtp_port', 'N/A')}")
    print(f"   Username: {config.get('username', 'N/A')}")
    print(f"   From Email: {config.get('from_email', 'N/A')}")
    print(f"   Use TLS: {config.get('use_tls', True)}")
    print()
    
    # Validate required fields
    required_fields = ['smtp_server', 'smtp_port', 'username', 'password', 'from_email']
    missing_fields = [field for field in required_fields if not config.get(field)]
    
    if missing_fields:
        print(f"❌ Missing required fields: {', '.join(missing_fields)}")
        sys.exit(1)
    
    # Run tests
    tests_passed = 0
    total_tests = 4
    
    # Test 1: DNS Resolution
    if test_dns_resolution(config['smtp_server']):
        tests_passed += 1
    print()
    
    # Test 2: Basic Connection
    if test_connection(config['smtp_server'], config['smtp_port'], config.get('use_tls', True)):
        tests_passed += 1
    print()
    
    # Test 3: Authentication
    if test_authentication(config):
        tests_passed += 1
    print()
    
    # Test 4: Send Test Email
    print("Do you want to send a test email? (y/n): ", end='')
    try:
        response = input().lower().strip()
        if response in ['y', 'yes']:
            print("Enter test email address (or press Enter to send to yourself): ", end='')
            test_email = input().strip()
            if not test_email:
                test_email = config['from_email']
            
            if send_test_email(config, test_email):
                tests_passed += 1
        else:
            print("⏭️  Skipping test email")
            total_tests -= 1
    except KeyboardInterrupt:
        print("\n⏭️  Skipping test email")
        total_tests -= 1
    
    print()
    
    # Summary
    print("=" * 50)
    print(f"📊 Test Results: {tests_passed}/{total_tests} tests passed")
    
    if tests_passed == total_tests:
        print("🎉 All tests passed! Your SMTP configuration is working correctly.")
    else:
        print("⚠️  Some tests failed. Please check your SMTP configuration.")
        print("\n💡 Common issues:")
        print("   - Check username/password (use App Password for Gmail)")
        print("   - Verify SMTP server and port settings")
        print("   - Ensure firewall allows SMTP traffic")
        print("   - Check if 'Less secure app access' is enabled (if required)")


if __name__ == "__main__":
    main() 