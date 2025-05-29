# InTabular - Intelligent Table Data Ingestion

**Automatically map unknown CSV structures to your well-defined schemas using AI**

Transform messy, unknown CSV files into clean, structured data that fits your target schema - without manual field mapping or complex ETL pipelines.

[![PyPI version](https://badge.fury.io/py/intabular.svg)](https://badge.fury.io/py/intabular)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## 🎯 What InTabular Does

**The Problem**: You have a well-structured master table, but data comes from various sources with different column names, formats, and structures.

**The Solution**: InTabular uses AI to automatically understand your source data and intelligently map it to your target schema.

```bash
# 1. Define your target schema once
intabular config customers.csv "Customer relationship database"

# 2. Ingest any CSV automatically  
intabular customer_config.yaml apollo-export.csv
intabular customer_config.yaml linkedin-contacts.csv
intabular customer_config.yaml salesforce-leads.csv
```

## 🚀 Quick Start

### Installation

```bash
pip install intabular
```

### Setup

1. **Get an OpenAI API key** from [platform.openai.com](https://platform.openai.com/api-keys)
2. **Set your API key**:
   ```bash
   export OPENAI_API_KEY="your-api-key-here"
   # or create .env file with: OPENAI_API_KEY=your-api-key-here
   ```

### Basic Usage

**Step 1: Create a target schema configuration**
```bash
# Generate config from existing table
intabular config customers.csv "Customer master database for CRM and outreach"
```

This creates `customers_config.yaml`:
```yaml
purpose: "Customer master database for CRM and outreach"
enrichment_columns:
  email: "Primary email address for customer contact"
  full_name: "Complete customer name for personalized communication"
  company_name: "Customer's organization name"
  phone: "Primary phone number for urgent communication"
  location: "Customer location for geographic analysis"
column_policy: "Maintain high data quality. Prefer complete, accurate information."
target: "customers.csv"
```

**Step 2: Ingest unknown CSV files**
```bash
# AI automatically maps fields and transforms data
intabular customers_config.yaml unknown-leads.csv
```

**That's it!** Your data is now mapped to your schema and saved to `customers.csv`.

## 🧠 How It Works

InTabular uses a 3-step AI pipeline:

### 1. **Semantic Analysis**
- Analyzes column names AND actual data content
- Identifies business context (leads, customers, contacts, etc.)
- Detects data source platform (Apollo, LinkedIn, Salesforce, etc.)
- Assesses data quality and completeness

### 2. **Intelligent Mapping Strategy**
- Maps source columns to target schema fields
- Chooses optimal strategies for each field:
  - **Replace**: Direct column mapping
  - **Derive**: Combine multiple columns (first + last name)
  - **Transform**: Clean and standardize formats
  - **Concat**: Merge related information
- Handles unmapped columns intelligently

### 3. **Quality-Aware Processing**  
- Executes field-by-field transformations
- Applies data validation and cleanup
- Maintains data integrity throughout the process

## 📊 Example Transformations

### Input CSV (unknown structure):
```csv
fname,lname,email_address,company,job_title,city_state
John,Doe,john@acme.com,Acme Corp,CEO,"San Francisco, CA"
Jane,Smith,jane@techco.com,TechCo,CTO,"New York, NY"
```

### Target Schema:
```yaml
enrichment_columns:
  email: "Primary email for contact"
  full_name: "Complete customer name"  
  company_name: "Organization name"
  location: "Geographic location"
```

### Output (automatically mapped):
```csv
email,full_name,company_name,location
john@acme.com,John Doe,Acme Corp,"San Francisco, CA"
jane@techco.com,Jane Smith,TechCo,"New York, NY"
```

**AI automatically**:
- Combined `fname` + `lname` → `full_name`
- Mapped `email_address` → `email`  
- Mapped `company` → `company_name`
- Mapped `city_state` → `location`
- Dropped unmapped `job_title` (not in target schema)

## 🎛️ Advanced Usage

### Programmatic API

```python
from intabular import AdaptiveMerger

# Initialize with API key
merger = AdaptiveMerger(api_key="your-openai-key")

# Analyze unknown CSV
analysis = merger.analyze_unknown_csv("mystery-data.csv")
print(f"Detected purpose: {analysis['table_purpose']}")

# Full ingestion pipeline
result_df = merger.ingest_csv("config.yaml", "unknown-data.csv")
print(f"Processed {len(result_df)} rows")
```

### Custom Configuration

Create rich schema configurations:

```yaml
purpose: "Sales prospect database with deal tracking and personalization data"

enrichment_columns:
  email: "Primary email address. Must be unique and valid format."
  full_name: "Complete name for personalized outreach. Combine first/last if needed."
  company_name: "Full company name. Standardize format and capitalization."
  deal_stage: "Current pipeline stage in sales process"
  deal_value: "Estimated or actual deal value in USD"
  phone: "Primary contact number. Format as (XXX) XXX-XXXX for US numbers."
  location: "Geographic location for territory management"
  last_contacted: "Date of last outreach or interaction"

column_policy: "Prioritize completeness for contact fields (email, name, company). 
               Keep deal information when available. 
               Store social profiles as metadata."

target: "sales_prospects.csv"
```

### Command Line Options

```bash
# Analyze CSV without ingesting
intabular analyze unknown-data.csv

# Create configuration interactively  
intabular config contacts.csv "Contact management system"

# Full ingestion with custom config
intabular my-schema.yaml leads-export.csv
```

## 🛠️ Real-World Use Cases

### **CRM Data Consolidation**
Merge contacts from Salesforce, HubSpot, Apollo, and LinkedIn into unified customer database.

### **Lead Processing Pipeline**  
Automatically process lead exports from multiple platforms with consistent field mapping.

### **Data Migration**
Migrate from legacy systems by mapping old schemas to new table structures.

### **Multi-Source Analytics**
Combine data from different tools into standardized format for analysis and reporting.

## ⚡ Key Features

- **🧠 AI-Powered**: Uses GPT models for intelligent field mapping
- **🔧 Zero Configuration**: Works out-of-the-box with sensible defaults  
- **📊 Quality-Aware**: Maintains data integrity with validation and cleanup
- **🎯 Business-Focused**: Understands business context and data relationships
- **🔄 Iterative**: Improves mapping quality with usage patterns
- **📈 Scalable**: Handles large CSV files efficiently
- **🛡️ Privacy-Conscious**: Only sends metadata and sample data to AI

## 🔒 Privacy & Security

InTabular is designed with privacy in mind:

- **Limited Data Sharing**: Only column names and 3 sample values per column are sent to OpenAI
- **No Bulk Data**: Your actual dataset never leaves your environment  
- **Local Processing**: All data transformation happens locally
- **API Key Control**: You control your own OpenAI API key and usage

## 📋 Requirements

- Python 3.8+
- OpenAI API key (for AI-powered mapping)
- Dependencies: `pandas`, `openai`, `pyyaml`, `python-dotenv`, `numpy`

## 🤝 Contributing

We welcome contributions! Whether it's:

- 🐛 Bug reports and fixes
- ✨ New feature suggestions  
- 📚 Documentation improvements
- 🧪 Test coverage expansion

See our [Contributing Guidelines](CONTRIBUTING.md) for details.

## 📄 License

MIT License - see [LICENSE](LICENSE) file for details.

## 🔗 Links

- **Documentation**: [GitHub Wiki](https://github.com/yourusername/intabular/wiki)
- **Issues**: [Bug Reports & Feature Requests](https://github.com/yourusername/intabular/issues)
- **PyPI**: [Package on PyPI](https://pypi.org/project/intabular/)

---

**Transform your messy CSV data into structured intelligence with InTabular** 🎯✨