#!/usr/bin/env python3
"""
Fancy HTML Email Test Script
Sends a beautiful, modern HTML email with CSS styling.
"""

import smtplib
import json
import sys
import textwrap
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
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


def create_fancy_html():
    """Create a fancy HTML email template."""
    html = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>🚀 MailPipe Test Email</title>
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap');
        
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            line-height: 1.6;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
        }
        
        .container {
            max-width: 600px;
            margin: 0 auto;
            background: #ffffff;
            border-radius: 20px;
            overflow: hidden;
            box-shadow: 0 20px 40px rgba(0, 0, 0, 0.1);
        }
        
        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 40px 30px;
            text-align: center;
            color: white;
            position: relative;
            overflow: hidden;
        }
        
        .header::before {
            content: '';
            position: absolute;
            top: -50%;
            left: -50%;
            width: 200%;
            height: 200%;
            background: repeating-linear-gradient(
                45deg,
                rgba(255,255,255,0.1) 0px,
                rgba(255,255,255,0.1) 2px,
                transparent 2px,
                transparent 10px
            );
            animation: slide 20s linear infinite;
        }
        
        @keyframes slide {
            0% { transform: translateX(-50px) translateY(-50px); }
            100% { transform: translateX(50px) translateY(50px); }
        }
        
        .header h1 {
            font-size: 2.5em;
            font-weight: 700;
            margin-bottom: 10px;
            position: relative;
            z-index: 1;
        }
        
        .header p {
            font-size: 1.2em;
            opacity: 0.9;
            position: relative;
            z-index: 1;
        }
        
        .emoji {
            font-size: 3em;
            margin-bottom: 20px;
            display: block;
            animation: bounce 2s infinite;
        }
        
        @keyframes bounce {
            0%, 20%, 50%, 80%, 100% { transform: translateY(0); }
            40% { transform: translateY(-10px); }
            60% { transform: translateY(-5px); }
        }
        
        .content {
            padding: 40px 30px;
        }
        
        .status-badge {
            background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
            color: white;
            padding: 10px 20px;
            border-radius: 25px;
            display: inline-block;
            font-weight: 600;
            margin-bottom: 30px;
            box-shadow: 0 4px 15px rgba(17, 153, 142, 0.3);
        }
        
        .info-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin: 30px 0;
        }
        
        .info-card {
            background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
            padding: 20px;
            border-radius: 15px;
            color: white;
            text-align: center;
            box-shadow: 0 10px 25px rgba(240, 147, 251, 0.3);
            transition: transform 0.3s ease;
        }
        
        .info-card:hover {
            transform: translateY(-5px);
        }
        
        .info-card h3 {
            font-size: 1.1em;
            margin-bottom: 5px;
            opacity: 0.9;
        }
        
        .info-card p {
            font-size: 1.3em;
            font-weight: 600;
        }
        
        .features {
            background: #f8f9fa;
            padding: 30px;
            margin: 30px 0;
            border-radius: 15px;
            border-left: 5px solid #667eea;
        }
        
        .features h3 {
            color: #333;
            margin-bottom: 20px;
            font-size: 1.4em;
        }
        
        .feature-list {
            list-style: none;
        }
        
        .feature-list li {
            padding: 8px 0;
            position: relative;
            padding-left: 30px;
            color: #555;
        }
        
        .feature-list li::before {
            content: '✨';
            position: absolute;
            left: 0;
            top: 8px;
        }
        
        .cta-button {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 15px 30px;
            border: none;
            border-radius: 30px;
            font-size: 1.1em;
            font-weight: 600;
            text-decoration: none;
            display: inline-block;
            margin: 20px 0;
            box-shadow: 0 10px 25px rgba(102, 126, 234, 0.3);
            transition: all 0.3s ease;
        }
        
        .cta-button:hover {
            transform: translateY(-3px);
            box-shadow: 0 15px 35px rgba(102, 126, 234, 0.4);
        }
        
        .footer {
            background: #2c3e50;
            color: #ecf0f1;
            padding: 30px;
            text-align: center;
        }
        
        .footer p {
            margin-bottom: 10px;
            opacity: 0.8;
        }
        
        .tech-stack {
            display: flex;
            justify-content: center;
            gap: 15px;
            margin: 20px 0;
            flex-wrap: wrap;
        }
        
        .tech-item {
            background: rgba(255,255,255,0.1);
            padding: 8px 15px;
            border-radius: 20px;
            font-size: 0.9em;
            backdrop-filter: blur(10px);
        }
        
        @media (max-width: 600px) {
            .container {
                margin: 10px;
                border-radius: 15px;
            }
            
            .header {
                padding: 30px 20px;
            }
            
            .header h1 {
                font-size: 2em;
            }
            
            .content {
                padding: 30px 20px;
            }
            
            .info-grid {
                grid-template-columns: 1fr;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <span class="emoji">🚀</span>
            <h1>MailPipe Test Email</h1>
            <p>Your SMTP Configuration is Working Perfectly!</p>
        </div>
        
        <div class="content">
            <div class="status-badge">
                ✅ Email Delivery Successful
            </div>
            
            <h2>🎉 Congratulations Alexander!</h2>
            <p>This fancy HTML email confirms that your MailPipe email system is working flawlessly. The email was sent with modern web technologies and beautiful styling.</p>
            
            <div class="info-grid">
                <div class="info-card">
                    <h3>📧 From Server</h3>
                    <p>alfa3022.alfahosting-server.de</p>
                </div>
                <div class="info-card">
                    <h3>🕐 Sent At</h3>
                    <p>""" + datetime.now().strftime('%H:%M:%S') + """</p>
                </div>
                <div class="info-card">
                    <h3>📅 Date</h3>
                    <p>""" + datetime.now().strftime('%Y-%m-%d') + """</p>
                </div>
                <div class="info-card">
                    <h3>🔒 Encryption</h3>
                    <p>TLS Enabled</p>
                </div>
            </div>
            
            <div class="features">
                <h3>🛠️ MailPipe Features</h3>
                <ul class="feature-list">
                    <li>SMTP Connection Testing with detailed diagnostics</li>
                    <li>Bulk email sending from CSV files</li>
                    <li>Beautiful HTML email templates</li>
                    <li>Secure TLS/SSL encryption</li>
                    <li>Error handling and progress tracking</li>
                    <li>Virtual environment setup</li>
                    <li>Git integration with sensitive file protection</li>
                </ul>
            </div>
            
            <div style="text-align: center;">
                <a href="https://github.com" class="cta-button">
                    🌟 Explore More Projects
                </a>
            </div>
            
            <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 15px; text-align: center; margin-top: 30px;">
                <h3>📊 System Information</h3>
                <div class="tech-stack">
                    <span class="tech-item">🐍 Python 3.10</span>
                    <span class="tech-item">📧 SMTP</span>
                    <span class="tech-item">🔐 TLS</span>
                    <span class="tech-item">🎨 HTML/CSS</span>
                    <span class="tech-item">⚡ Fast Delivery</span>
                </div>
            </div>
        </div>
        
        <div class="footer">
            <p>📧 Sent via MailPipe Email System</p>
            <p>🔧 Built with Python • Styled with Modern CSS</p>
            <p style="font-size: 0.9em; margin-top: 20px;">
                This email was automatically generated and sent to test the SMTP functionality.<br>
                If you received this, everything is working perfectly! 🎯
            </p>
        </div>
    </div>
</body>
</html>
    """
    return html


def send_fancy_email(config, recipient_email):
    """Send a fancy HTML email."""
    print(f"🎨 Preparing fancy HTML email for {recipient_email}...")
    
    try:
        # Create message
        msg = MIMEMultipart('alternative')
        msg['From'] = config['from_email']
        msg['To'] = recipient_email
        msg['Subject'] = f"🚀 MailPipe Test - Fancy HTML Email • {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        
        # Create plain text version
        text_body = f"""
🚀 MailPipe Test Email - Success!

Hi Alexander!

This is a test email from your MailPipe email system. If you're reading this, 
your SMTP configuration is working perfectly!

Test Details:
- Server: {config['smtp_server']}:{config['smtp_port']}
- From: {config['from_email']}
- To: {recipient_email}
- Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
- Encryption: TLS Enabled

Your email system is ready for bulk sending!

--
Sent via MailPipe Email System 📧
        """
        
        # Create HTML version
        html_body = create_fancy_html()
        
        # Attach parts
        part1 = MIMEText(text_body, 'plain')
        part2 = MIMEText(html_body, 'html')
        
        msg.attach(part1)
        msg.attach(part2)
        
        # Connect and send
        print(f"🔌 Connecting to {config['smtp_server']}:{config['smtp_port']}...")
        
        if config.get('use_tls', True):
            server = smtplib.SMTP(config['smtp_server'], config['smtp_port'], timeout=15)
            server.starttls()
        else:
            server = smtplib.SMTP_SSL(config['smtp_server'], config['smtp_port'], timeout=15)
        
        print("🔑 Authenticating...")
        server.login(config['username'], config['password'])
        
        print("📤 Sending fancy email...")
        server.send_message(msg)
        server.quit()
        
        print(f"✅ Fancy HTML email sent successfully to {recipient_email}!")
        print(f"📬 Check {recipient_email} inbox - you should see a beautiful email!")
        print("🎨 The email includes:")
        print("   • Modern CSS animations and gradients")
        print("   • Responsive design for mobile/desktop")
        print("   • Interactive hover effects")
        print("   • Beautiful typography and layout")
        print("   • System information cards")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to send fancy email: {e}")
        return False


def main():
    print("🎨 Fancy HTML Email Sender")
    print("=" * 40)
    
    # Load configuration
    config = load_smtp_config()
    if not config:
        print("\n❌ Cannot proceed without valid SMTP configuration.")
        sys.exit(1)
    
    print(f"📋 SMTP Configuration:")
    print(f"   Server: {config.get('smtp_server', 'N/A')}")
    print(f"   From: {config.get('from_email', 'N/A')}")
    print()
    
    # Validate required fields
    required_fields = ['smtp_server', 'smtp_port', 'username', 'password', 'from_email']
    missing_fields = [field for field in required_fields if not config.get(field)]
    
    if missing_fields:
        print(f"❌ Missing required fields: {', '.join(missing_fields)}")
        sys.exit(1)
    
    # Send to Alexander
    recipient = "alexander.krauck@gmail.com"
    
    if send_fancy_email(config, recipient):
        print("\n🎉 Mission accomplished!")
        print("✨ Your fancy HTML email has been delivered!")
    else:
        print("\n❌ Failed to send email. Check your configuration.")


if __name__ == "__main__":
    main() 