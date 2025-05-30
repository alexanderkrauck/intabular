#!/usr/bin/env python3
"""
Script to send all emails from target_with_emails.csv to alexander.krauck@gmail.com
for testing and preview purposes.
"""

from send_emails import send_emails_from_csv

def send_all_emails_to_alexander():
    """
    Send all AI-generated emails to alexander.krauck@gmail.com for testing.
    This allows reviewing all the personalized emails before sending to actual recipients.
    """
    
    print("🚀 Sending all AI-generated emails to alexander.krauck@gmail.com for preview")
    print("=" * 70)
    
    try:
        result = send_emails_from_csv(
            csv_file='target_with_emails.csv',
            message_column='generated_email_body',  # AI-generated German emails with Kim's signature
            email='a.krauck@gmail.com',     # Fixed test email - override all recipients
            subject='Gartenwohnung-Investment: St. Johann i. d. Haide, Thermenregion',  # Fixed subject for all
            attachments=['Super-Wohnen-in-der-Steiermark.pdf'],  # Uncomment if you have attachments
            verbose=True
        )
        
        print(f"\n✅ Email campaign preview completed!")
        print(f"📊 Results: {result['sent']} sent, {result['failed']} failed, {result['total']} total")
        
        if result['sent'] > 0:
            print(f"\n📧 Check alexander.krauck@gmail.com for {result['sent']} preview emails")
            print("📝 Each email contains:")
            print("   - Personalized German real estate content")
            print("   - Kim's professional HTML signature")
            print("   - Company branding and contact info")
            print("   - Social media links")
        
        if result['failed'] > 0:
            print(f"\n⚠️  {result['failed']} emails failed to send - check the output above for details")
        
        return result
        
    except Exception as e:
        print(f"❌ Error sending preview emails: {e}")
        return None

def send_sample_emails_to_alexander(sample_size=5):
    """
    Send only a sample of emails for quick preview.
    
    Args:
        sample_size: Number of emails to send (default: 5)
    """
    
    print(f"🧪 Sending {sample_size} sample emails to alexander.krauck@gmail.com")
    print("=" * 60)
    
    try:
        # Read the CSV and limit to sample size
        import pandas as pd
        df = pd.read_csv('target_with_emails.csv')
        sample_df = df.head(sample_size)
        sample_csv = 'sample_emails_preview.csv'
        sample_df.to_csv(sample_csv, index=False)
        
        result = send_emails_from_csv(
            csv_file=sample_csv,
            message_column='generated_email_body',
            email='alexander.krauck@gmail.com',
            subject=f'[SAMPLE] Gartenwohnung Campaign - {sample_size} Examples',
            attachments=['Super-Wohnen-in-der-Steiermark.pdf'],
            verbose=True
        )
        
        # Clean up temp file
        import os
        os.remove(sample_csv)
        
        print(f"\n✅ Sample preview completed: {result}")
        return result
        
    except Exception as e:
        print(f"❌ Error sending sample emails: {e}")
        return None

if __name__ == "__main__":
    print("📧 Email Campaign Preview Options")
    print("=" * 50)
    print("1. Send ALL emails to alexander.krauck@gmail.com")
    print("2. Send SAMPLE (5 emails) to alexander.krauck@gmail.com")
    print()
    
    choice = input("Choose option (1 or 2, or press Enter for ALL): ").strip()
    
    if choice == "2":
        # Send sample
        send_sample_emails_to_alexander(2)
    else:
        # Send all (default)
        send_all_emails_to_alexander()
    
    print("\n🎯 Preview complete! Check your email to review the AI-generated content.") 