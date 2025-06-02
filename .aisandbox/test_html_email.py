#!/usr/bin/env python3
"""
Test script to send a single HTML email with Kim's signature.
"""

import smtplib
import json
import sys
import textwrap
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def load_smtp_config(config_file='smtp_config.json'):
    """Load SMTP configuration from JSON file."""
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: {config_file} not found.")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in {config_file}")
        sys.exit(1)


def send_html_email(smtp_server, from_email, to_email, subject, html_content):
    """Send an HTML email using the provided SMTP server."""
    try:
        # Create message
        msg = MIMEMultipart('alternative')
        msg['From'] = from_email
        msg['To'] = to_email
        msg['Subject'] = subject
        
        # Create HTML part
        html_part = MIMEText(html_content, 'html', 'utf-8')
        msg.attach(html_part)
        
        # Send email
        smtp_server.send_message(msg)
        return True
    except Exception as e:
        print(f"Failed to send email to {to_email}: {str(e)}")
        return False


def main():
    # Load SMTP configuration
    config = load_smtp_config()
    
    # Test email content
    to_email = "alexander.krauck@gmail.com"
    subject = "Test HTML Email mit Kim's Signatur"
    
    # Sample email body with Kim's HTML signature
    kim_signature_html = """<html>
  <body style="font-family: Arial, sans-serif; font-size: 14px; color: #000;">
    <!-- Salutation + Image Row -->
    <table cellpadding="0" cellspacing="0" border="0">
      <tr>
        <!-- Left: Image -->
        <td style="vertical-align: middle; padding-right: 15px;">
          <img src="https://www.krauck-systems.com/new/Signaturen/Kim.jpg" alt="Kim" style="width: 100px; height: auto; border-radius: 4px;">
        </td>

        <!-- Right: Text block, vertically centered -->
        <td style="vertical-align: middle;">
          <p style="margin: 0;">Freundliche Grüße,</p>
          <p style="margin: 5px 0 0 0;"><strong>Kim</strong></p>
          <p style="margin: 0;">Ihr KI-Immobilienmakler</p>
        </td>
      </tr>
    </table>

    <br>

    <!-- Logo -->
    <img src="https://www.krauck-systems.com/new/Signaturen/KSPE-Logo.jpg" alt="Krauck Systems Logo" style="width: 250px; height: auto;"><br><br>

    <!-- Contact Info -->
    <p style="margin: 0;">
      <strong>CITY TOWER I</strong><br>
      Lastenstraße 38/15.OG<br>
      A-4020 Linz<br>
      📞 +43-732-995-30380<br>
      ✉️ <a href="mailto:kim@krauck-systems.com">kim@krauck-systems.com</a><br>
      🌐 <a href="https://www.krauck-systems-wohnen.com/">www.krauck-systems-wohnen.com</a>
    </p>

    <br>

    <!-- Social Media -->
    <p style="margin: 0;">Folgen Sie uns jetzt auf:</p>
    <table cellpadding="0" cellspacing="0" role="presentation" style="margin-top: 5px;">
    <tr>
        <td style="padding-right: 8px;">
        <a href="https://www.facebook.com/profile.php?id=61576511609960" target="_blank">
            <img src="https://www.krauck-systems.com/new/Images/facebook-logo.png" alt="Facebook" width="32" height="32" style="border-radius: 6px;">
        </a>
        </td>
        <td style="padding-right: 8px;">
        <a href="https://www.linkedin.com/company/krauck-systems-wohnen/" target="_blank">
            <img src="https://www.krauck-systems.com/new/Images/linkedin-logo.png" alt="LinkedIn" width="32" height="32" style="border-radius: 6px;">
        </a>
        </td>
        <td>
        <a href="https://www.instagram.com/krauck_systems/" target="_blank">
            <img src="https://www.krauck-systems.com/new/Images/instagram-logo.png" alt="Instagram" width="32" height="32" style="border-radius: 6px;">
        </a>
        </td>
    </tr>
    </table>

  </body>
</html>"""
    
    # Complete HTML email with test content
    html_content = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Test Email</title>
</head>
<body style="font-family: Arial, sans-serif; font-size: 14px; color: #000; margin: 0; padding: 20px;">
    <h2>Test HTML Email</h2>
    <p>Hallo Alexander,</p>
    <p>Dies ist eine Test-E-Mail mit Kim's neuer HTML-Signatur. Die Signatur sollte alle Bilder und Links korrekt anzeigen.</p>
    <p>Bitte bestätige, dass die Signatur korrekt gerendert wird.</p>
    <br>
    {kim_signature_html}
</body>
</html>"""
    
    # Connect to SMTP server and send
    try:
        print(f"Connecting to SMTP server: {config['smtp_server']}:{config['smtp_port']}")
        
        if config.get('use_tls', True):
            server = smtplib.SMTP(config['smtp_server'], config['smtp_port'])
            server.starttls()
        else:
            server = smtplib.SMTP_SSL(config['smtp_server'], config['smtp_port'])
        
        server.login(config['username'], config['password'])
        print("Successfully connected and authenticated!")
        
        print(f"Sending test email to {to_email}...")
        
        if send_html_email(server, config['from_email'], to_email, subject, html_content):
            print("✅ Test email sent successfully!")
        else:
            print("❌ Failed to send test email!")
        
        server.quit()
        
    except Exception as e:
        print(f"Failed to connect to SMTP server: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main() 