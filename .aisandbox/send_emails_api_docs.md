# Send Emails API Documentation

## Overview
The `send_emails.py` module can now be imported and used programmatically with flexible column mapping and configuration options.

## Main Function: `send_emails_from_csv()`

### Parameters

| Parameter | Type | Description | Required |
|-----------|------|-------------|----------|
| `csv_file` | `str` | Path to the CSV file | ✅ |
| `message_column` | `str` | Name of column containing email messages | ✅ |
| `email` | `str \| List[str] \| None` | Fixed email(s) OR column name (auto-detected) | Either this or `email_column` |
| `email_column` | `str \| None` | Explicit column name for email addresses | Either this or `email` |
| `subject` | `str \| None` | Fixed subject OR column name (auto-detected) | Either this or `subject_column` |
| `subject_column` | `str \| None` | Explicit column name for subjects | Either this or `subject` |
| `attachments` | `str \| List[str] \| None` | Fixed file(s) OR column name (auto-detected) | ❌ |
| `attachments_column` | `str \| None` | Explicit column name for attachments | ❌ |
| `smtp_config_file` | `str` | Path to SMTP config (default: 'smtp_config.json') | ❌ |
| `verbose` | `bool` | Print progress messages (default: True) | ❌ |

### Returns
```python
{
    'sent': int,      # Number of emails sent successfully
    'failed': int,    # Number of emails that failed
    'total': int      # Total emails processed
}
```

## Usage Examples

### 1. Basic Usage with target_with_emails.csv
```python
from send_emails import send_emails_from_csv

# Send with column-based emails, fixed subject and attachments
result = send_emails_from_csv(
    csv_file='target_with_emails.csv',
    message_column='generated_email_body',
    email='email',  # Column name containing email addresses
    subject='Letzte Gartenwohnung - Exklusive Gelegenheit',
    attachments=['flyer.pdf', 'images.zip']
)
```

### 2. Fixed Email Recipients
```python
# Send same message to fixed email addresses
result = send_emails_from_csv(
    csv_file='messages.csv',
    message_column='content',
    email=['alexander.krauck@gmail.com', 'team@company.com'],  # Fixed recipients
    subject='Weekly Update',
    attachments='report.pdf'
)
```

### 3. Single Fixed Email
```python
# Send to one fixed email address
result = send_emails_from_csv(
    csv_file='target_with_emails.csv',
    message_column='generated_email_body',
    email='alexander.krauck@gmail.com',  # Single fixed email
    subject='Test Campaign'
)
```

### 4. Column-based Everything
```python
# Use different columns for all fields
result = send_emails_from_csv(
    csv_file='my_emails.csv',
    message_column='content',
    email_column='recipient',           # Use column for emails
    subject_column='subject_line',      # Use column for subjects
    attachments_column='file_paths'     # Use column for attachments
)
```

### 5. Mixed Configuration
```python
# Fixed email + column-based subject + fixed attachments
result = send_emails_from_csv(
    csv_file='campaign.csv',
    message_column='body',
    email='test@example.com',           # Fixed email
    subject_column='custom_subject',    # Column-based subject
    attachments=['doc1.pdf', 'doc2.pdf']  # Fixed attachments
)
```

## CSV Format Examples

### With Column-based Emails
```csv
email,body
user1@example.com,"Hello User 1..."
user2@example.com,"Hello User 2..."
```

### With All Column-based Fields
```csv
email,body,subject,files
user1@example.com,"Hello...","Welcome!","doc1.pdf,img1.jpg"
user2@example.com,"Hi...","Special Offer","doc2.pdf"
```

### For Fixed Email Recipients (only message column needed)
```csv
body
"Message 1 content..."
"Message 2 content..."
```

## Special Use Cases

### Testing/Preview Mode
```python
# Send all messages to yourself for testing
result = send_emails_from_csv(
    csv_file='target_with_emails.csv',
    message_column='generated_email_body',
    email='your-test@email.com',  # Override all recipients for testing
    subject='[TEST] Real Estate Campaign'
)
```

### Broadcast to Team
```python
# Send same content to multiple team members
result = send_emails_from_csv(
    csv_file='announcements.csv',
    message_column='announcement_text',
    email=['manager@company.com', 'team-lead@company.com', 'admin@company.com'],
    subject='Important Announcement'
)
```

## Error Handling
- Raises `ValueError` for configuration errors
- Raises `FileNotFoundError` if CSV doesn't exist
- Raises `ConnectionError` for SMTP issues
- Raises `RuntimeError` for CSV processing errors

## Command Line (Backward Compatible)
```bash
# Still works as before
python send_emails.py
python send_emails.py my_emails.csv
``` 