# 🧪 AI Sandbox - Testing & Development Scripts

This directory contains experimental scripts, tests, and development tools that are used for testing functionality but are not part of the main production codebase.

## 📁 Directory Purpose

The `.aisandbox` serves as a playground for:
- ✅ Testing email functionality
- 🧪 Experimental features
- 🔧 Development utilities
- 📊 Diagnostic scripts
- 🎨 Prototype implementations

## 📄 Current Scripts

### Email Testing Suite
- **`test_smtp.py`** - Comprehensive SMTP connection and sending tests
- **`test_imap.py`** - IMAP connection and mailbox access tests  
- **`test_email_full.py`** - Combined SMTP + IMAP testing suite
- **`send_fancy_test.py`** - HTML email template demonstration

### Usage Examples

```bash
# Test SMTP functionality
python .aisandbox/test_smtp.py

# Test IMAP functionality  
python .aisandbox/test_imap.py

# Run comprehensive email tests
python .aisandbox/test_email_full.py

# Send fancy HTML test email
python .aisandbox/send_fancy_test.py
```

## 🚫 What NOT to Include Here

This directory should NOT contain:
- Production email sending scripts
- Main application code
- Configuration files with real credentials
- Final user-facing tools

## 🎯 AI Development Guidelines

**For AI Assistants:** Use this directory for:
1. Testing new functionality before implementing in main code
2. Creating proof-of-concept scripts
3. Debugging and diagnostic tools
4. Experimental features that may not make it to production
5. One-off scripts for specific testing needs

Keep the main project directory clean by placing all experimental and testing code here.

---
*This directory helps maintain a clean separation between production code and development/testing utilities.* 