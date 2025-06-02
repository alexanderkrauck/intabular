#%%
#!/usr/bin/env python3
"""Manual Apollo CSV to target schema transformation (Specific to the provided apollo-contacts-export.csv)
   and generates outreach emails using OpenAI.
"""

import pandas as pd
import numpy as np
import os
import time # For potential rate limiting
from openai import OpenAI # Corrected import
from dotenv import load_dotenv # Added for .env support
import html # Added for HTML escaping
import re # Added for extracting signature parts
import textwrap

# Load environment variables from .env file
load_dotenv()

# --- Configuration for Email Generation ---
KIM_SIGNATURE = """
<html>
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
</html>

"""

email_prompt="""### Deine Rolle
Du bist ein österreichischer Immobilienentwickler mit genau **einer** fertig ausgestatteten Gartenwohnung im Neubau "Haus 3 – St. Johann i. d. Haide 247" die du vermitteln willst.  
Du möchtest lokale Unternehmen anschreiben und ihnen die Wohnung als ideales Zuhause oder Investment für neue Mitarbeitende vorstellen.  
Schreibe kurze, freundliche, aber professionelle E‑Mails auf Deutsch, die hilfreich klingen und **nicht** stalker‑haft wirken.

### E‑Mail‑Anforderungen
- **Anrede:** T‑Form, respektvoll ("Liebe Frau/Herr  [Name]").  
- **Hook:** Kurze aber deutliche Erwähnung eines Research‑Aspekts (z. B. Nachhaltigkeit, Standortnähe).  
- **Kernaussage:** Max. 2 Argumente aus Abschnitt 3, warum die Wohnung ideal für eine*n neue oder bestehende [Company Name]‑Mitarbeiter*in oder als Firmen‑Investment ist.  
- **Sprache & Stil:** Deutsch, unter 300 Wörter im Body (ohne Signatur), freundlich aber professionell.  
- **Signatur:** KEINE SIGNATUR. DIE SIGNATUR WIRD NACHHER HINZUGEFÜGT. Das heißt das was du generierst sollte z.B. OHNE "Mit freundlichen Grüßen," enden. Füge auf keinen Fall [Ihr name] oder ähnliches ein. Es sollte einfach aufhören!
- **Anhang:** Flyer/Rundgang als Anhang.
- **Verteilung** Es ist wichtig, dass wir fokussieren dass diese Information verbreitet wird unter kollegen und nicht nur an die Person. D.h. es sollte explizit eine passende andeutung gemacht werden, dass es ein heißer tipp ist der verbreitet werden soll und wir speziell dieser Person schreiben weil wir denken dass es besonders gut passt bei dieser Firma.

### Ausgabeformat  
Gib **nur** den finalen E‑Mail‑Text zurück (Body, ohne Betreff oder Signatur!!). Keine weiteren Erklärungen.


### 1 · Kontakt‑ & Firmeninfos  
Verwende folgende Daten als Fakten (nicht alles direkt zitieren!):  
{{ContactInfo}}

### 2 · Recherche‑Notizen  
Nimm Anknüpfungspunkte aus diesen unstrukturierten Erkenntnissen auf:  
{{ResearchNotes}}

### 3 · Projekt‑Daten  
Wähle max. **2** Kernaussagen aus diesen Projektdetails:  
{{ProjectData}}"""

def load_project_data(file_path="project_letzte_wohnung.md"):
    """Loads project data from a specified markdown file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        print(f"❌ Error: Project data file '{file_path}' not found.")
        return None
    except Exception as e:
        print(f"❌ Error reading project data file '{file_path}': {e}")
        return None

# --- OpenAI Email Generation Function ---
def generate_emails_with_openai(df, api_key, project_data_str, email_prompt_template):
    print("\n🤖 Starting OpenAI email generation (German Immobilien Prompt)...")
    if not api_key:
        print("❌ OPENAI_API_KEY not found. Skipping email generation.")
        df['generated_email_body'] = "Error: OPENAI_API_KEY not set."
        return df
    if not project_data_str:
        print("❌ Project data not loaded. Skipping email generation.")
        df['generated_email_body'] = "Error: Project data missing."
        return df

    try:
        client = OpenAI(api_key=api_key)
    except Exception as e:
        print(f"❌ Error initializing OpenAI client: {e}")
        df['generated_email_body'] = f"Error: OpenAI client initialization failed."
        return df

    generated_email_bodies = []
    total_contacts = len(df)
    print(f"Found {total_contacts} contacts to generate emails for.")

    for index, row in df.iterrows():
        print(f"\nProcessing contact {index + 1}/{total_contacts}: {row.get('full_name', 'N/A')} at {row.get('company_name', 'N/A')}...")
        
        contact_info_parts = []
        if pd.notna(row.get('full_name')) and row['full_name'].strip():
            contact_info_parts.append(f"- Name: {row['full_name']}")
        else: # Ensure a name is always present for the salutation
            contact_info_parts.append(f"- Name: Kontaktperson") # Fallback if no name
        if pd.notna(row.get('first_name')) and row['first_name'].strip():
            contact_info_parts.append(f"- Vorname: {row['first_name']}")
        if pd.notna(row.get('last_name')) and row['last_name'].strip():
            contact_info_parts.append(f"- Nachname: {row['last_name']}")
        if pd.notna(row.get('email')) and row['email'].strip():
            contact_info_parts.append(f"- E-Mail: {row['email']}")
        if pd.notna(row.get('title')) and row['title'].strip():
            contact_info_parts.append(f"- Position: {row['title']}")
        if pd.notna(row.get('company_name')) and row['company_name'].strip():
            contact_info_parts.append(f"- Firma: {row['company_name']}")
        if pd.notna(row.get('location')) and row['location'].strip():
            contact_info_parts.append(f"- Standort: {row['location']}")
        if pd.notna(row.get('website')) and row['website'].strip():
            contact_info_parts.append(f"- Webseite: {row['website']}")
        contact_info_str = "\n".join(contact_info_parts)

        research_notes_str = "Keine spezifischen Notizen vorhanden."
        if pd.notna(row.get('notes')) and row['notes'].strip():
            notes_list = []
            raw_notes = row['notes'].split('|')
            for note_item in raw_notes:
                if ':' in note_item:
                    key, value = note_item.split(':', 1)
                    if value.strip() and value.strip().lower() != 'nan':
                        notes_list.append(f"- {key.strip()}: {value.strip()}")
                elif note_item.strip() and note_item.strip().lower() != 'nan':
                    notes_list.append(f"- {note_item.strip()}")
            if notes_list:
                research_notes_str = "\n".join(notes_list)
        
        # Fill the prompt template
        current_prompt = email_prompt_template.replace("{{ContactInfo}}", contact_info_str)
        current_prompt = current_prompt.replace("{{ResearchNotes}}", research_notes_str)
        current_prompt = current_prompt.replace("{{ProjectData}}", project_data_str)
        if pd.notna(row.get('company_name')) and row['company_name'].strip(): # For the Kernaussage part
            current_prompt = current_prompt.replace("[Company Name]", row['company_name'])
        else:
            current_prompt = current_prompt.replace("[Company Name]", "Ihres Unternehmens") # Fallback
        if pd.notna(row.get('last_name')) and row['last_name'].strip(): # For the Anrede part
             # Check for gendered title if available, otherwise use generic Anrede
            anrede_name = row['last_name']
            # This is a simple heuristic. A more robust solution might involve a gender detection library or more title keywords.
            if pd.notna(row.get('title')) and any(t in row['title'].lower() for t in ['frau', 'ms.', 'mrs.']):
                anrede = f"Liebe Frau {anrede_name}"
            elif pd.notna(row.get('title')) and any(t in row['title'].lower() for t in ['herr', 'mr.']):
                anrede = f"Lieber Herr {anrede_name}"
            else: # Fallback if title doesn't indicate gender or if title is missing
                anrede = f"Sehr geehrte/r Frau/Herr {anrede_name}" # More formal fallback or use first name if desired
            current_prompt = current_prompt.replace("Liebe Frau/Herr  [Name]", anrede) # Adjusted to match prompt format
        else:
             current_prompt = current_prompt.replace("Liebe Frau/Herr  [Name]", "Sehr geehrte Damen und Herren") # Fallback

        # print(f"--- Debug Prompt for {row.get('full_name', 'N/A')} ---\n{current_prompt}\n---------------------") # Uncomment for debugging

        try:
            response = client.chat.completions.create(
                model="gpt-4o-mini", 
                messages=[
                    {"role": "system", "content": "Du bist ein Assistent, der E-Mail-Texte basierend auf detaillierten Anweisungen generiert."},
                    {"role": "user", "content": current_prompt}
                ],
                temperature=0.5, # Slightly lower for more deterministic output based on strong prompt
                max_tokens=2000  # Allow for a decent length email body in German
            )
            email_body = response.choices[0].message.content.strip()
            print(email_body)
            full_email = f"{email_body}\n\n{KIM_SIGNATURE}"
            generated_email_bodies.append(full_email)
            print(f"  📧 Email body generated for {row.get('full_name', 'N/A')}.")
            # time.sleep(0.5) # Reduced delay, adjust if rate limits hit

        except Exception as e:
            error_message = f"Error generating email for {row.get('full_name', 'N/A')}: {e}"
            print(f"  ❌ {error_message}")
            generated_email_bodies.append(f"Error: {e}\n\n{KIM_SIGNATURE}") # Append signature even on error for consistency

    df['generated_email_body'] = generated_email_bodies # Changed column name
    print("\n✅ OpenAI email generation complete.")
    return df

def create_specific_transformation():
    print("🔄 Creating specific transformation for your apollo-contacts-export.csv...")
    
    # Read Apollo CSV
    try:
        apollo_df = pd.read_csv("apollo-contacts-export.csv")
    except FileNotFoundError:
        print("❌ Error: apollo-contacts-export.csv not found. Please ensure the file is in the same directory.")
        return None
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        return None
        
    print(f"✅ Loaded: {len(apollo_df)} rows from apollo-contacts-export.csv")
    
    actual_columns = list(apollo_df.columns)
    # Strip whitespace from actual column names for reliable matching
    apollo_df.columns = [col.strip() for col in actual_columns]
    actual_columns_stripped = list(apollo_df.columns) # Use this for checks
    print(f"📋 Columns found and stripped in your CSV: {actual_columns_stripped}")
    
    # Create target DataFrame
    target_df = pd.DataFrame()
    used_source_columns = set()
    
    # --- Direct Mappings based on YOUR CSV's columns ---
    if 'Email' in apollo_df.columns:
        target_df['email'] = apollo_df['Email']
        used_source_columns.add('Email')
        print("✅ Mapped 'Email' → 'email'")

    if 'First Name' in apollo_df.columns:
        target_df['first_name'] = apollo_df['First Name']
        used_source_columns.add('First Name')
        print("✅ Mapped 'First Name' → 'first_name'")

    if 'Last Name' in apollo_df.columns:
        target_df['last_name'] = apollo_df['Last Name']
        used_source_columns.add('Last Name')
        print("✅ Mapped 'Last Name' → 'last_name'")

    if 'Company' in apollo_df.columns:
        target_df['company_name'] = apollo_df['Company']
        used_source_columns.add('Company')
        print("✅ Mapped 'Company' → 'company_name'")
    elif 'Company Name for Emails' in apollo_df.columns:
        target_df['company_name'] = apollo_df['Company Name for Emails']
        used_source_columns.add('Company Name for Emails')
        print("✅ Mapped 'Company Name for Emails' → 'company_name'")

    if 'Title' in apollo_df.columns:
        target_df['title'] = apollo_df['Title']
        used_source_columns.add('Title')
        print("✅ Mapped 'Title' → 'title'")

    phone_cols_priority = ["Work Direct Phone", "Mobile Phone", "Corporate Phone", "Home Phone", "Other Phone"]
    for col in phone_cols_priority:
        if col in apollo_df.columns:
            target_df['phone'] = apollo_df[col]
            used_source_columns.add(col)
            print(f"✅ Mapped '{col}' → 'phone'")
            break

    if 'Person Linkedin Url' in apollo_df.columns:
        target_df['linkedin_url'] = apollo_df['Person Linkedin Url']
        used_source_columns.add('Person Linkedin Url')
        print("✅ Mapped 'Person Linkedin Url' → 'linkedin_url'")

    if 'Website' in apollo_df.columns:
        target_df['website'] = apollo_df['Website']
        used_source_columns.add('Website')
        print("✅ Mapped 'Website' → 'website'")
    
    # --- Derived Fields ---
    if 'first_name' in target_df.columns and 'last_name' in target_df.columns:
        target_df['full_name'] = (target_df['first_name'].fillna('') + ' ' + 
                                 target_df['last_name'].fillna('')).str.strip()
        print("✅ Created 'full_name'")
    elif 'first_name' in target_df.columns:
        target_df['full_name'] = target_df['first_name'].fillna('').str.strip()
        print("✅ Created 'full_name' (from first_name only)")
    elif 'last_name' in target_df.columns:
        target_df['full_name'] = target_df['last_name'].fillna('').str.strip()
        print("✅ Created 'full_name' (from last_name only)")
    else:
        print("⚠️ Could not create 'full_name' (missing 'first_name' and/or 'last_name' in target)")

    location_components = []
    location_source_cols_used = []

    # Prioritize contact location, then company for location parts
    city_col_to_use = None
    if 'City' in apollo_df.columns and apollo_df['City'].notna().any():
        city_col_to_use = 'City'
    elif 'Company City' in apollo_df.columns and apollo_df['Company City'].notna().any():
        city_col_to_use = 'Company City'
    if city_col_to_use:
        location_components.append(apollo_df[city_col_to_use].fillna(''))
        location_source_cols_used.append(city_col_to_use)
    else:
        location_components.append(pd.Series([''] * len(apollo_df), name='city_placeholder'))

    state_col_to_use = None
    if 'State' in apollo_df.columns and apollo_df['State'].notna().any():
        state_col_to_use = 'State'
    elif 'Company State' in apollo_df.columns and apollo_df['Company State'].notna().any():
        state_col_to_use = 'Company State'
    if state_col_to_use:
        location_components.append(apollo_df[state_col_to_use].fillna(''))
        location_source_cols_used.append(state_col_to_use)
    else:
        location_components.append(pd.Series([''] * len(apollo_df), name='state_placeholder'))

    country_col_to_use = None
    if 'Country' in apollo_df.columns and apollo_df['Country'].notna().any():
        country_col_to_use = 'Country'
    elif 'Company Country' in apollo_df.columns and apollo_df['Company Country'].notna().any():
        country_col_to_use = 'Company Country'
    if country_col_to_use:
        location_components.append(apollo_df[country_col_to_use].fillna(''))
        location_source_cols_used.append(country_col_to_use)
    else:
        location_components.append(pd.Series([''] * len(apollo_df), name='country_placeholder'))
        
    if location_source_cols_used:
        # Each item in location_components is a Series. zip them up.
        target_df['location'] = pd.Series(zip(*location_components)).apply(
            lambda x: ', '.join(str(val).strip() for val in x if str(val).strip())
        ).str.strip(', ').replace('', np.nan) # Remove leading/trailing commas and empty strings to NaN
        print(f"✅ Created 'location' using data from: {location_source_cols_used}")
        used_source_columns.update(location_source_cols_used)
    else:
        print("⚠️ Could not find sufficient geographic columns to create 'location'")
        
    notes_parts = []
    notes_source_cols_used = []

    if 'Industry' in apollo_df.columns and apollo_df['Industry'].notna().any():
        notes_parts.append("Industry: " + apollo_df['Industry'].astype(str).fillna(''))
        notes_source_cols_used.append('Industry')
    if 'Keywords' in apollo_df.columns and apollo_df['Keywords'].notna().any():
        notes_parts.append("Keywords: " + apollo_df['Keywords'].astype(str).fillna(''))
        notes_source_cols_used.append('Keywords')
    if 'Seniority' in apollo_df.columns and apollo_df['Seniority'].notna().any():
        notes_parts.append("Seniority: " + apollo_df['Seniority'].astype(str).fillna(''))
        notes_source_cols_used.append('Seniority')
    if 'Departments' in apollo_df.columns and apollo_df['Departments'].notna().any():
        notes_parts.append("Departments: " + apollo_df['Departments'].astype(str).fillna(''))
        notes_source_cols_used.append('Departments')
    
    # Add CustomAIResearchBasic to notes
    custom_ai_col = 'CustomAIResearchBasic' # Original name had trailing spaces
    if custom_ai_col in apollo_df.columns and apollo_df[custom_ai_col].notna().any():
        notes_parts.append("CustomAIResearch: " + apollo_df[custom_ai_col].astype(str).fillna(''))
        notes_source_cols_used.append(custom_ai_col)

    if notes_parts:
        # Each item in notes_parts is a Series. zip them up.
        target_df['notes'] = pd.Series(zip(*notes_parts)).apply(
            lambda x: ' | '.join(str(val).strip() for val in x if str(val).split(':', 1)[-1].strip())
        ).str.strip(' | ').replace('', np.nan) # Remove leading/trailing separators and empty strings to NaN
        print(f"✅ Created 'notes' using data from: {notes_source_cols_used}")
        used_source_columns.update(notes_source_cols_used)
    else:
        print("⚠️ Could not find any metadata to create 'notes'")

    # --- Final Cleaning ---
    if 'email' in target_df.columns:
        original_rows = len(target_df)
        # 1. Convert to string to handle mixed types, np.nan becomes 'nan', and strip whitespace
        target_df['email'] = target_df['email'].astype(str).str.strip()
        # 2. Replace 'nan' string (from np.nan converted to str) and empty strings with actual np.nan
        target_df['email'] = target_df['email'].replace(['nan', 'none', ''], np.nan, regex=False) # Added 'none' for safety
        # 3. Drop rows with no email or invalid email format (no '@')
        target_df = target_df.dropna(subset=['email'])
        # Ensure email contains '@' and at least one '.' after '@'
        target_df = target_df[target_df['email'].str.contains('@', na=False) & target_df['email'].str.contains(r'@.+\.', na=False, regex=True)]
        # 4. Drop duplicates by email, keeping the first occurrence
        target_df = target_df.drop_duplicates(subset=['email'], keep='first')
        
        print(f"ℹ️ Email cleaning: Kept rows with valid email, removed duplicates. Went from {original_rows} to {len(target_df)} rows.")
        if original_rows > 0 and len(target_df) == 0 and 'email' in target_df.columns: # Check if email column still exists
             print("⚠️ All rows were removed during email cleaning. The output will be empty if no new rows are added.")
    else:
        print("⚠️ No 'email' column in target, skipping email-based cleaning. This is highly unusual for a contact list.")
        
    # --- Select One Representative per Company based on Role Priority ---
    # Preference: HR > Management > Top Level/CEO > Other
    if not target_df.empty and 'company_name' in target_df.columns and 'title' in target_df.columns:
        print("ℹ️ Starting company representative selection...")
        rows_before_comp_filter = len(target_df)

        def get_role_category(title_str):
            if pd.isna(title_str) or not isinstance(title_str, str) or title_str.strip() == '':
                return 'Other'
            title_lower = title_str.lower()

            # Priority 1: HR
            hr_keywords = [
                "hr", "human resources", "recruiter", "talent acquisition", 
                "chief people", "people operations", "personnel"
            ]
            if any(kw in title_lower for kw in hr_keywords):
                return 'HR'

            # Priority 2: Management
            management_keywords = [
                "manager", "director", "head of", "vp ", "vice president", "lead", "supervisor"
            ]
            # Avoid classifying C-level as Management if they contain these keywords but are better defined as Top Level
            # The Top Level check is later, but this order (HR -> Mgmt -> Top) defines preference for categorization if ambiguous.
            if any(kw in title_lower for kw in management_keywords):
                # Check if it's a very high-level title that should be Top Level despite "manager" or "director"
                # For example, "Managing Director" can be Top Level.
                # This is handled by the order of checks and keyword specificity.
                # If "Managing Director" is in top_level_keywords, and that check comes *after* general "director", it needs care.
                # Current logic: check HR, then Management, then Top. So a "Managing Director of HR" would be HR.
                # A "Managing Director" not in HR will be Management if "manager" or "director" matches.
                # This seems fine for the specified preference.
                return 'Management'

            # Priority 3: Top Level/CEO
            top_level_keywords = [
                "ceo", "chief executive officer", "president", "founder", "owner", "chairman",
                "cfo", "chief financial officer", "cto", "chief technology officer",
                "cmo", "chief marketing officer", "coo", "chief operating officer",
                "cio", "chief information officer", "cso", "chief strategy officer", 
                "chief security officer", "partner", "principal", "managing director"
            ]
            if any(kw in title_lower for kw in top_level_keywords):
                return 'Top Level/CEO'
            
            return 'Other'

        role_priority_map = {'HR': 1, 'Management': 2, 'Top Level/CEO': 3, 'Other': 4}

        target_df['role_category'] = target_df['title'].apply(get_role_category)
        target_df['role_priority'] = target_df['role_category'].map(role_priority_map)
        
        # Ensure NaNs in company_name are handled consistently (grouped together)
        # Fill NaN company_names with a placeholder to ensure they are grouped, then idxmin will pick one.
        # Or use dropna=False in groupby if supported and desired. idxmin handles NaNs in group keys correctly.
        target_df = target_df.loc[target_df.groupby('company_name', dropna=False)['role_priority'].idxmin()]
        
        print(f"✅ Company representative selection: Filtered from {rows_before_comp_filter} to {len(target_df)} contacts (one per company based on role priority).")
        
        # Clean up helper columns
        target_df = target_df.drop(columns=['role_category', 'role_priority'], errors='ignore')

    elif 'company_name' not in target_df.columns and not target_df.empty:
        print("⚠️ Skipping company representative selection: 'company_name' column missing.")
    elif 'title' not in target_df.columns and not target_df.empty:
        print("⚠️ Skipping company representative selection: 'title' column missing.")
    elif target_df.empty:
        print("ℹ️ Skipping company representative selection: DataFrame is empty (possibly after email cleaning).")

    # --- Report Unused Columns (from your specific CSV) ---
    # actual_columns_stripped contains the corrected column names from the input CSV
    unused_columns = [col for col in actual_columns_stripped if col not in used_source_columns]
    if unused_columns:
        print(f"ℹ️ The following {len(unused_columns)} columns from your apollo-contacts-export.csv were NOT used in this specific transformation:")
        for col in sorted(unused_columns):
            print(f"  - {col}")
    else:
        print("✅ All columns from your apollo-contacts-export.csv were considered or used in this specific transformation.")

    # --- Save Output ---
    if not target_df.empty:
        target_df.to_csv("ideal_target_example.csv", index=False)
        print(f"💾 Saved ideal_target_example.csv ({len(target_df)} contacts)")

        # --- Generate Emails with OpenAI ---
        openai_api_key = os.getenv("OPENAI_API_KEY")
        project_data_str = load_project_data() # Load project data

        if openai_api_key and project_data_str:
            print("\n🔑 OpenAI API key found and project data loaded. Proceeding with email generation.")
            # For testing, let's process only the first few rows 
            # sample_df_for_email_generation = target_df.head(1).copy() 
            # print(f"\n🧪 Processing a sample of {len(sample_df_for_email_generation)} contacts for email generation.")
            # target_with_emails_df = generate_emails_with_openai(sample_df_for_email_generation, openai_api_key, project_data_str, email_prompt)
            
            # Process all contacts
            target_with_emails_df = generate_emails_with_openai(target_df.copy(), openai_api_key, project_data_str, email_prompt)

            if 'generated_email_body' in target_with_emails_df.columns:
                target_with_emails_df.to_csv("target_with_emails.csv", index=False)
                print(f"💾 Saved target_with_emails.csv ({len(target_with_emails_df)} contacts with generated email bodies)")
            else:
                print("⚠️ 'generated_email_body' column not found after OpenAI processing. Skipping save of target_with_emails.csv")
        elif not openai_api_key:
            print("\n⚠️ OPENAI_API_KEY environment variable not set. Skipping email generation.")
            print("ℹ️ To enable email generation, please create a .env file with your OPENAI_API_KEY or set the environment variable.")
        elif not project_data_str:
            print("\n⚠️ Project data file (project_letzte_wohnung.md) could not be loaded. Skipping email generation.")

    else:
        print("⚠️ Target DataFrame is empty. Nothing to save to ideal_target_example.csv or generate emails for.")
        
    return target_df
#%%
# Run the specific transformation
create_specific_transformation()
# %%
