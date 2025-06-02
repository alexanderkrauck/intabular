#!/usr/bin/env python3
"""
🎯 InTabular HTML Showcase Generator
===================================

Creates a beautiful HTML visualization of the data transformation
Perfect for business presentations and social media!
"""

import sys
import pandas as pd
from pathlib import Path
import webbrowser
import os

# Add parent directory for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

def create_demo_data():
    """Create the demo data for visualization"""
    
    # LinkedIn Export
    linkedin_df = pd.DataFrame({
        'contact_email': ['sarah@startup.io', 'mike@enterprise.com'],
        'contact_name': ['Sarah Johnson', 'Mike Chen'],
        'contact_company': ['TechStartup Inc', 'Enterprise LLC'],
        'job_title': ['VP Engineering', 'CTO'],
        'mobile': ['+1-555-0101', '+1-555-0202']
    })
    
    # Apollo Export
    apollo_df = pd.DataFrame({
        'Email': ['david@fintech.co', 'lisa@healthtech.org'],
        'First Name': ['David', 'Lisa'],
        'Last Name': ['Kim', 'Wong'],
        'Company': ['FinTech Solutions', 'HealthTech Inc'],
        'Title': ['Product Manager', 'Marketing Head'],
        'Phone Number': ['555-123-4567', '555-987-6543']
    })
    
    # Unified Output
    result_df = pd.DataFrame({
        'email': ['sarah@startup.io', 'mike@enterprise.com', 'david@fintech.co', 'lisa@healthtech.org'],
        'full_name': ['Sarah Johnson', 'Mike Chen', 'David Kim', 'Lisa Wong'],
        'company_name': ['TechStartup Inc', 'Enterprise LLC', 'FinTech Solutions', 'HealthTech Inc'],
        'job_title': ['VP Engineering', 'CTO', 'Product Manager', 'Marketing Head'],
        'phone': ['+1-555-0101', '+1-555-0202', '555-123-4567', '555-987-6543']
    })
    
    return linkedin_df, apollo_df, result_df

def df_to_html_table(df, title, subtitle="", table_class="input-table"):
    """Convert DataFrame to styled HTML table"""
    
    # Create table header
    html = f"""
    <div class="table-container">
        <h3 class="table-title">{title}</h3>
        {f'<p class="table-subtitle">{subtitle}</p>' if subtitle else ''}
        <table class="{table_class}">
            <thead>
                <tr>
    """
    
    # Add column headers
    for col in df.columns:
        html += f'<th>{col}</th>'
    html += '</tr></thead><tbody>'
    
    # Add data rows
    for _, row in df.iterrows():
        html += '<tr>'
        for value in row:
            html += f'<td>{value}</td>'
        html += '</tr>'
    
    html += '</tbody></table></div>'
    return html

def create_html_showcase():
    """Generate the complete HTML showcase"""
    
    linkedin_df, apollo_df, result_df = create_demo_data()
    
    html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>InTabular AI Demo - Data Transformation Showcase</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            line-height: 1.6;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }}
        
        .showcase-container {{
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 20px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.1);
            overflow: hidden;
        }}
        
        .header {{
            background: linear-gradient(135deg, #2196F3 0%, #21CBF3 100%);
            color: white;
            padding: 40px;
            text-align: center;
        }}
        
        .header h1 {{
            font-size: 2.5rem;
            font-weight: 700;
            margin-bottom: 10px;
        }}
        
        .header p {{
            font-size: 1.2rem;
            opacity: 0.9;
        }}
        
        .content {{
            padding: 40px;
        }}
        
        .section {{
            margin-bottom: 50px;
        }}
        
        .section h2 {{
            font-size: 1.8rem;
            color: #333;
            margin-bottom: 20px;
            display: flex;
            align-items: center;
            gap: 10px;
        }}
        
        .input-section {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 30px;
            margin-bottom: 40px;
        }}
        
        .table-container {{
            background: #f8f9fa;
            border-radius: 12px;
            padding: 20px;
            border-left: 5px solid #007bff;
        }}
        
        .table-title {{
            font-size: 1.3rem;
            color: #495057;
            margin-bottom: 10px;
            font-weight: 600;
        }}
        
        .table-subtitle {{
            color: #6c757d;
            font-size: 0.9rem;
            margin-bottom: 15px;
        }}
        
        .input-table {{
            width: 100%;
            border-collapse: collapse;
            background: white;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        
        .input-table th {{
            background: #495057;
            color: white;
            padding: 12px 8px;
            text-align: left;
            font-weight: 600;
            font-size: 0.85rem;
        }}
        
        .input-table td {{
            padding: 10px 8px;
            border-bottom: 1px solid #dee2e6;
            font-size: 0.85rem;
        }}
        
        .input-table tr:hover {{
            background-color: #f8f9fa;
        }}
        
        .output-table {{
            width: 100%;
            border-collapse: collapse;
            background: white;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 4px 20px rgba(0,0,0,0.15);
        }}
        
        .output-table th {{
            background: linear-gradient(135deg, #28a745, #20c997);
            color: white;
            padding: 15px 12px;
            text-align: left;
            font-weight: 600;
        }}
        
        .output-table td {{
            padding: 12px;
            border-bottom: 1px solid #dee2e6;
        }}
        
        .output-table tr:hover {{
            background-color: #f8f9fa;
        }}
        
        .ai-transformation {{
            background: linear-gradient(135deg, #ff6b6b, #ffa726);
            color: white;
            padding: 30px;
            border-radius: 15px;
            margin: 40px 0;
            text-align: center;
        }}
        
        .ai-transformation h3 {{
            font-size: 1.5rem;
            margin-bottom: 20px;
        }}
        
        .mapping-list {{
            list-style: none;
            text-align: left;
            max-width: 800px;
            margin: 0 auto;
        }}
        
        .mapping-list li {{
            background: rgba(255,255,255,0.2);
            margin: 8px 0;
            padding: 12px 20px;
            border-radius: 8px;
            font-weight: 500;
        }}
        
        .metrics {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-top: 30px;
        }}
        
        .metric-card {{
            background: white;
            padding: 25px;
            border-radius: 12px;
            text-align: center;
            box-shadow: 0 4px 15px rgba(0,0,0,0.1);
            border-top: 4px solid #007bff;
        }}
        
        .metric-number {{
            font-size: 2.2rem;
            font-weight: 700;
            color: #007bff;
            margin-bottom: 5px;
        }}
        
        .metric-label {{
            color: #6c757d;
            font-weight: 500;
        }}
        
        .business-impact {{
            background: linear-gradient(135deg, #28a745, #20c997);
            color: white;
            padding: 30px;
            border-radius: 15px;
            margin-top: 40px;
        }}
        
        .business-impact h3 {{
            font-size: 1.5rem;
            margin-bottom: 20px;
            text-align: center;
        }}
        
        .impact-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin-top: 20px;
        }}
        
        .impact-item {{
            background: rgba(255,255,255,0.2);
            padding: 15px;
            border-radius: 8px;
            text-align: center;
        }}
        
        .footer {{
            background: #343a40;
            color: white;
            padding: 30px;
            text-align: center;
        }}
        
        .cta-button {{
            display: inline-block;
            background: #007bff;
            color: white;
            padding: 15px 30px;
            border-radius: 50px;
            text-decoration: none;
            font-weight: 600;
            margin-top: 20px;
            transition: all 0.3s ease;
        }}
        
        .cta-button:hover {{
            background: #0056b3;
            transform: translateY(-2px);
        }}
        
        .emoji {{
            font-size: 1.2em;
        }}
        
        @media (max-width: 768px) {{
            .input-section {{
                grid-template-columns: 1fr;
            }}
            
            .header h1 {{
                font-size: 2rem;
            }}
            
            .content {{
                padding: 20px;
            }}
        }}
    </style>
</head>
<body>
    <div class="showcase-container">
        <div class="header">
            <h1><span class="emoji">🎯</span> InTabular AI Demo</h1>
            <p>Transform messy CSV data into clean, unified databases using AI</p>
        </div>
        
        <div class="content">
            <div class="section">
                <h2><span class="emoji">📥</span> Messy Input Data (Before InTabular)</h2>
                <div class="input-section">
                    {df_to_html_table(linkedin_df, "🔗 LinkedIn Export", "Sales Navigator export with custom field names")}
                    {df_to_html_table(apollo_df, "🎯 Apollo.io Export", "Lead enrichment platform with different structure")}
                </div>
            </div>
            
            <div class="ai-transformation">
                <h3><span class="emoji">🧠</span> AI Semantic Mapping Applied</h3>
                <ul class="mapping-list">
                    <li>✓ contact_email + Email → email (unified email field)</li>
                    <li>✓ contact_name + First Name + Last Name → full_name (smart combination)</li>
                    <li>✓ contact_company + Company → company_name (semantic matching)</li>
                    <li>✓ job_title + Title → job_title (normalized role field)</li>
                    <li>✓ mobile + Phone Number → phone (standardized contact)</li>
                </ul>
            </div>
            
            <div class="section">
                <h2><span class="emoji">📤</span> Clean Output Data (After InTabular)</h2>
                {df_to_html_table(result_df, "🎯 Unified Sales Prospects Database", "Clean, consistent schema ready for business use", "output-table")}
            </div>
            
            <div class="section">
                <h2><span class="emoji">📊</span> Transformation Metrics</h2>
                <div class="metrics">
                    <div class="metric-card">
                        <div class="metric-number">11</div>
                        <div class="metric-label">Input Columns</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-number">5</div>
                        <div class="metric-label">Output Columns</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-number">100%</div>
                        <div class="metric-label">Auto-Mapped</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-number">0</div>
                        <div class="metric-label">Manual Work</div>
                    </div>
                </div>
            </div>
            
            <div class="business-impact">
                <h3><span class="emoji">🎯</span> Business Impact</h3>
                <div class="impact-grid">
                    <div class="impact-item">
                        <strong>Sales Teams</strong><br>
                        Unified data instantly
                    </div>
                    <div class="impact-item">
                        <strong>No Manual Work</strong><br>
                        Zero CSV gymnastics
                    </div>
                    <div class="impact-item">
                        <strong>Automated Workflows</strong><br>
                        Consistent data structure
                    </div>
                    <div class="impact-item">
                        <strong>Time Savings</strong><br>
                        Hours → Minutes
                    </div>
                </div>
            </div>
        </div>
        
        <div class="footer">
            <h3>Ready to Transform Your Data?</h3>
            <p>InTabular uses AI to automatically map any CSV structure to your target schema</p>
            <a href="https://github.com/alexanderkrauck/intabular" class="cta-button">
                <span class="emoji">🚀</span> Get Started with InTabular
            </a>
        </div>
    </div>
</body>
</html>
    """
    
    # Save the HTML file
    html_file = 'intabular_showcase.html'
    with open(html_file, 'w') as f:
        f.write(html_content)
    
    return html_file

def main():
    """Generate and open the HTML showcase"""
    
    print("🎨 Generating beautiful HTML showcase...")
    html_file = create_html_showcase()
    
    print(f"✅ HTML showcase created: {html_file}")
    print("🌐 Opening in browser...")
    
    # Get absolute path for browser
    abs_path = os.path.abspath(html_file)
    webbrowser.open(f'file://{abs_path}')
    
    print(f"""
🎉 HTML Showcase Complete!

📁 File saved: {html_file}
🌐 Opened in browser automatically

🎯 Perfect for:
   • Business presentations
   • Social media screenshots
   • Investor demos
   • Technical showcases
   
💡 The HTML file is self-contained and can be shared anywhere!
""")

if __name__ == "__main__":
    main() 