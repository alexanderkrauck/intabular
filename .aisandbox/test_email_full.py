#!/usr/bin/env python3
"""
Comprehensive Email Test Script
Tests both SMTP (sending) and IMAP (receiving) functionality.
"""

import subprocess
import sys


def run_smtp_test():
    """Run SMTP test script."""
    print("🚀 Running SMTP (sending) tests...")
    print("=" * 50)
    try:
        # Run SMTP test with automatic "no" to test email prompt
        result = subprocess.run([sys.executable, 'test_smtp.py'], 
                              input='n\n', 
                              text=True, 
                              capture_output=True)
        
        print(result.stdout)
        if result.stderr:
            print("Errors:", result.stderr)
        
        return result.returncode == 0
    except Exception as e:
        print(f"❌ Failed to run SMTP test: {e}")
        return False


def run_imap_test():
    """Run IMAP test script."""
    print("\n📬 Running IMAP (receiving) tests...")
    print("=" * 50)
    try:
        result = subprocess.run([sys.executable, 'test_imap.py'], 
                              capture_output=True, 
                              text=True)
        
        print(result.stdout)
        if result.stderr:
            print("Errors:", result.stderr)
        
        return result.returncode == 0
    except Exception as e:
        print(f"❌ Failed to run IMAP test: {e}")
        return False


def main():
    print("🧪 Comprehensive Email Testing Suite")
    print("Testing both SMTP (sending) and IMAP (receiving)")
    print("=" * 60)
    
    smtp_passed = run_smtp_test()
    imap_passed = run_imap_test()
    
    print("\n" + "=" * 60)
    print("📊 FINAL TEST SUMMARY")
    print("=" * 60)
    
    print(f"📤 SMTP (Sending): {'✅ PASSED' if smtp_passed else '❌ FAILED'}")
    print(f"📥 IMAP (Receiving): {'✅ PASSED' if imap_passed else '❌ FAILED'}")
    
    if smtp_passed and imap_passed:
        print("\n🎉 ALL EMAIL TESTS PASSED!")
        print("✅ You can send and receive emails successfully!")
    elif smtp_passed:
        print("\n✅ SMTP works - you can send emails")
        print("⚠️  IMAP failed - check receiving configuration")
    elif imap_passed:
        print("\n✅ IMAP works - you can receive emails")  
        print("⚠️  SMTP failed - check sending configuration")
    else:
        print("\n❌ Both tests failed - check your email configuration")
    
    print("\n💡 Individual test scripts:")
    print("   python test_smtp.py  - Test sending only")
    print("   python test_imap.py  - Test receiving only")


if __name__ == "__main__":
    main() 