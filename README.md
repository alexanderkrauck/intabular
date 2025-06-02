# InTabular - Intelligent CSV Data Ingestion (In Development - Feedback Welcome)

[![PyPI version](https://badge.fury.io/py/intabular.svg)](https://badge.fury.io/py/intabular)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Automatically map unknown CSV structures to your target schemas using AI**

Transform messy, unknown CSV files into clean, structured data that fits your target schema - without manual field mapping or complex ETL pipelines.

## 🎯 What InTabular Does

**The Problem**: You have a well-structured target table, but data comes from various sources with different column names, formats, and structures.

**The Solution**: InTabular uses AI to automatically understand your source data and intelligently map it to your target schema.

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

**Recommended: Create config manually**

Create a YAML file (e.g., `customers_config.yaml`) that defines your target schema:

```yaml
purpose: >
  Customer master database for CRM and outreach. Contains qualified customers 
  with complete contact information for sales team follow-up and relationship management.

enrichment_columns:
  email:
    description: "Primary email address for customer contact. Must be unique and valid."
    supports_purpose_by: "Essential for email outreach and lead tracking"
    is_entity_identifier: true
    identity_indication: 1.0
    
  full_name:
    description: "Complete customer name for personalized communication"
    supports_purpose_by: "Required for personalized outreach and relationship building"
    is_entity_identifier: true
    identity_indication: 0.8
    
  company_name:
    description: "Customer's organization name for B2B relationship management"
    supports_purpose_by: "Critical for account-based sales and company research"
    is_entity_identifier: false
    identity_indication: 0.0
    
  phone:
    description: "Primary phone number for direct contact"
    supports_purpose_by: "Enables phone outreach and urgent communication"
    is_entity_identifier: false
    identity_indication: 0.0
    
  location:
    description: "Geographic location for territory management"
    supports_purpose_by: "Helps with territory assignment and regional campaigns"
    is_entity_identifier: false
    identity_indication: 0.0

target_file_path: "customers.csv"
```

**Alternative: Automatic config generation (experimental)**
```bash
# Generate basic config from existing table - needs manual editing
python -m intabular config customers.csv "Customer master database for CRM and outreach"
```

*Note: The automatic config generation creates a basic template that requires manual editing to add proper `supports_purpose_by`, `is_entity_identifier`, and `identity_indication` fields for production use.*

**Step 2: Ingest unknown CSV files**
```bash
# AI automatically maps fields and transforms data
python -m intabular customers_config.yaml unknown-leads.csv
```

**That's it!** Your data is now mapped to your schema and saved to `customers.csv`.

## 📋 Configuration Guide

### Configuration Fields Explained

- **`purpose`**: Describes what your target database is for (helps AI understand context)
- **`enrichment_columns`**: Defines each column in your target schema
  - **`description`**: What this field contains (be specific!)
  - **`supports_purpose_by`**: How this field helps achieve the database purpose
  - **`is_entity_identifier`**: Whether this field helps identify unique entities
  - **`identity_indication`**: How strong an identifier this is (0.0 to 1.0)
- **`target_file_path`**: Where to save the final result

### Example Configurations

**Sales Prospects Database:**
```yaml
purpose: "Sales prospect database for B2B outreach and lead management"
enrichment_columns:
  email:
    description: "Business email address"
    supports_purpose_by: "Required for email outreach campaigns"
    is_entity_identifier: true
    identity_indication: 1.0
  full_name:
    description: "Contact's full name"
    supports_purpose_by: "Personalizes outreach messages"
    is_entity_identifier: true
    identity_indication: 0.7
  company_name:
    description: "Company or organization name"
    supports_purpose_by: "Enables account-based selling"
    is_entity_identifier: false
    identity_indication: 0.0
  job_title:
    description: "Professional role or position"
    supports_purpose_by: "Helps target decision makers"
    is_entity_identifier: false
    identity_indication: 0.0
target_file_path: "sales_prospects.csv"
```

**Event Attendees Database:**
```yaml
purpose: "Event attendee database for conference management and networking"
enrichment_columns:
  email:
    description: "Attendee email for event communications"
    supports_purpose_by: "Essential for event updates and follow-up"
    is_entity_identifier: true
    identity_indication: 1.0
  full_name:
    description: "Attendee full name for badges and networking"
    supports_purpose_by: "Required for event experience and networking"
    is_entity_identifier: true
    identity_indication: 0.9
  company:
    description: "Attendee's organization"
    supports_purpose_by: "Enables business networking and sponsor matching"
    is_entity_identifier: false
    identity_indication: 0.0
  registration_type:
    description: "Type of ticket or registration"
    supports_purpose_by: "Determines access levels and benefits"
    is_entity_identifier: false
    identity_indication: 0.0
target_file_path: "event_attendees.csv"
```

## 🧠 How It Works

InTabular uses a 4-step AI pipeline:

### 1. **Data Analysis**
- Analyzes column names and actual data content
- Identifies business context and data quality
- Detects data types (identifier vs text content)

### 2. **Strategy Creation**
- Analyzes source columns against target schema fields
- Creates separate strategies for entity identifier vs descriptive columns
- Uses parallel processing for performance with multiple columns
- Chooses optimal transformation approaches:
  - **format**: Deterministic Python transformation rules (e.g., combine fields, normalize data)
  - **llm_format**: LLM-powered intelligent parsing when complex interpretation is needed
  - **none**: No suitable source mapping found

### 3. **Quality Processing**
- Executes field-by-field transformations
- Applies data validation and cleanup
- Handles conflicts intelligently

### 4. **Results**
- Maintains data integrity throughout
- Provides detailed logging and confidence scores

## 📊 Example Transformation

### Input CSV (unknown structure):
```csv
fname,lname,email_address,company,job_title,city_state
John,Doe,john@acme.com,Acme Corp,CEO,"San Francisco, CA"
Jane,Smith,jane@techco.com,TechCo,CTO,"New York, NY"
```

### Target Schema:
```yaml
purpose: "Customer contact database for sales outreach"
enrichment_columns:
  email:
    description: "Primary email address for contact"
    supports_purpose_by: "Required for email outreach campaigns"
    is_entity_identifier: true
    identity_indication: 1.0
  full_name:
    description: "Complete customer name"
    supports_purpose_by: "Personalizes communication"
    is_entity_identifier: true
    identity_indication: 0.8
  company_name:
    description: "Customer's organization"
    supports_purpose_by: "Account-based relationship management"
    is_entity_identifier: false
    identity_indication: 0.0
  location:
    description: "Geographic location"
    supports_purpose_by: "Territory management and localization"
    is_entity_identifier: false
    identity_indication: 0.0
target_file_path: "customers.csv"
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

## 🎛️ Programmatic Usage

InTabular provides flexible DataFrame and CSV APIs:

### Core DataFrame API

```python
import pandas as pd
from intabular import ingest_to_schema, ingest_with_explicit_schema
from intabular.core.config import GatekeeperConfig

# Load your DataFrames
df_new = pd.read_csv("unknown-data.csv")
df_target = pd.read_csv("existing-customers.csv")  # Can be empty

# Load or create schema
config = GatekeeperConfig.from_yaml("customers_config.yaml")

# Mode 1: Transform new data to match schema
result_df = ingest_to_schema(df_new, config)
print(f"Transformed {len(result_df)} rows to schema")

# Mode 2: Merge new data with existing target data
result_df = ingest_with_explicit_schema(df_new, df_target, config)
print(f"Processed {len(result_df)} rows")
```

### CSV Convenience API

```python
from intabular.csv_component import run_csv_ingestion_pipeline, create_config_from_csv

# Create configuration from existing CSV
create_config_from_csv("customers.csv", "Customer relationship database", "config.yaml")

# Run CSV ingestion pipeline  
result_df = run_csv_ingestion_pipeline("config.yaml", "unknown-data.csv")
print(f"Processed {len(result_df)} rows")
```

## 🛠️ Command Line Interface

```bash
# Ingest CSV with existing configuration (recommended workflow)
python -m intabular <yaml_config> <csv_file>

# Generate basic config template (experimental - requires manual editing)
python -m intabular config <table_path> <purpose>

# Examples:
python -m intabular customers_config.yaml new_leads.csv
python -m intabular event_config.yaml registrations.csv
python -m intabular config customers.csv "Customer database for sales outreach"  # experimental
```

**Recommended workflow:**
1. Create config manually using examples from Configuration Guide below
2. Run ingestion: `python -m intabular your_config.yaml your_data.csv`

## 🎯 Core Philosophy: Semantic Data Management

**InTabular represents a step toward truly intelligent data management systems that understand the semantic meaning of your data, not just its structure.**

### Key Concepts

- **Semantic Schema**: Describes what data *means* in business terms
- **Intelligent Mapping**: Understanding content meaning, not just column matching
- **Context Awareness**: AI analyzes both structure and business purpose
- **Quality Preservation**: Maintains data integrity through transformations

### Example Semantic Understanding

- Recognize that "fname + lname" should become "full_name"
- Understand "corp_email" and "personal_email" both represent contact information
- Detect that "San Francisco, CA" and "SF, California" represent the same location

## 🌟 Advanced Use Cases

### Multi-Source Contact Management

**Scenario**: Merge contacts from Salesforce, LinkedIn, and manual CSV exports

**Traditional Problem**:
- Different column names ("email_address" vs "work_email" vs "contact_email")
- Manual field mapping required
- No intelligent deduplication

**InTabular Solution**:
- Semantic understanding: All email fields → unified "email" concept
- Intelligent mapping based on content analysis
- Automatic field consolidation

### Company Data Consolidation

**Scenario**: Maintain clean company records from various employee lists

**Intelligence**:
- Detects multiple people from same company
- Consolidates company information intelligently
- Preserves data relationships while cleaning duplicates

## ⚡ Key Features

- **🧠 AI-Powered**: Uses GPT models for intelligent field mapping
- **🔧 Minimal Configuration**: Simple YAML-based schema definition
- **📊 Quality-Aware**: Maintains data integrity with validation
- **🎯 Business-Focused**: Understands business context and relationships
- **🔄 Flexible Processing**: Handles both new and existing target data
- **📈 Scalable**: Efficient processing of large datasets
- **🛡️ Privacy-Conscious**: Only sends metadata and samples to AI

## 🔒 Privacy & Security

InTabular is designed with privacy in mind:

- **Limited Data Sharing**: Only column names and sample values sent to OpenAI
- **No Bulk Data**: Your full dataset never leaves your environment  
- **Local Processing**: All transformations happen locally
- **API Key Control**: You control your OpenAI API usage and costs

## 📋 Requirements

- Python 3.8+
- OpenAI API key
- Dependencies: `pandas`, `openai`, `pyyaml`, `python-dotenv`, `numpy`

## 🚀 Installation & Development

```bash
# Production installation
pip install intabular

# Development installation
git clone https://github.com/alexanderkrauck/intabular.git
cd intabular
pip install -e .
```

## 📚 Documentation & Support

- **PyPI Package**: [https://pypi.org/project/intabular/](https://pypi.org/project/intabular/)
- **Source Code**: [https://github.com/alexanderkrauck/intabular](https://github.com/alexanderkrauck/intabular)
- **Issues**: [https://github.com/alexanderkrauck/intabular/issues](https://github.com/alexanderkrauck/intabular/issues)

---

## 🧠 Theoretical Foundation & Advanced Concepts (I JUST WROTE THIS DOWN IN 30 MINUTES AND LET IT BE WRITTEN NICELY, DONT JUDGE MY MATH!!)

*The following sections explore the theoretical underpinnings and long-term vision for semantic data management. This is background theory - the practical functionality described above works today.*

### Vision: Revolutionary Semantic CRUD System

**InTabular aims to be a foundation toward truly intelligent data management systems that understand the semantic meaning of your data, not just its structure.**

#### Semantic vs Syntactic Schema

- **Traditional Approach**: Column names, data types, constraints (syntactic)
- **InTabular Approach**: Business meaning, entity relationships, semantic purpose (semantic)
- **Hybrid Reality**: Semantic understanding drives syntactic decisions

The system maintains a **semantic schema** that describes what the data *means* in business terms, while automatically managing the underlying syntactic structure to support those semantics.

### Target Capabilities (Roadmap)

#### 1. **Advanced Semantic Data Ingestion**

Transform any CSV into your target schema by understanding content meaning, not just column matching.

**Future Examples:**
- Intelligent deduplication: "John Doe at Acme Corp" = "J. Doe, Acme Corporation"
- Multi-field semantic matching across different data representations
- Context-aware field combination and splitting

#### 2. **Intelligent CRUD Operations**

**Add Data (INSERT)**
```
Input: New CSV with unknown structure
Action: Analyze, map, and append new records
Intelligence: Understand new data semantics and fit to existing schema
```

**Merge Data (UPDATE/INSERT)**
```
Input: CSV with potential duplicates/updates
Action: Smart merge based on semantic identity
Intelligence: Detect same entities across different data representations
```

**Merge-Add Data (UPSERT)** *[Default Strategy]*
```
Input: Any CSV file
Action: Add new entries OR update existing ones intelligently
Intelligence: Semantic duplicate detection and conflict resolution
```

**Smart Delete (DELETE)**
```
Input: Deletion criteria (CSV or natural language)
Action: Remove matching records with semantic understanding
Intelligence: Handle edge cases and relationship preservation
```

**Query & Retrieve (SELECT)**
```
Input: Natural language queries or structured filters
Action: Return semantically relevant results
Intelligence: Understand intent beyond literal matches
```

### Future Use Cases

#### Natural Language Data Operations

**Text-Based Commands**:
```
"Remove all contacts from companies with less than 50 employees"
"Merge this lead list, but only add people we don't already have"
"Update all contacts from TechCorp with this new company information"
"Delete duplicate entries, keeping the most recent ones"
```

**CSV + Instructions**:
```
CSV: updated_contacts.csv
Instruction: "Update existing contacts and add new ones, but don't create duplicates"
```

#### Dynamic Schema Evolution

**Scenario**: Target schema needs to adapt to new data types

**Intelligence**:
- Detect valuable unmapped columns: "This data has 'deal_value' info we're not capturing"
- Suggest schema enhancements: "Consider adding 'industry' field for better segmentation"
- Auto-evolve schema while preserving existing data integrity

### Implementation Roadmap

#### **Phase 1: Core Semantic CRUD** *(Current)*
- ✅ Intelligent CSV ingestion and mapping
- ✅ Basic semantic understanding
- ✅ Schema-aware transformations

#### **Phase 2: Natural Language Interface**
- Text-based operation commands
- Query understanding and execution
- User preference learning

#### **Phase 3: Advanced Intelligence**
- Cross-table relationship management
- Predictive data suggestions
- Automatic schema evolution
- Advanced semantic deduplication

---

## 🔬 AGI-Aware Software Architecture

*This section explores the mathematical and philosophical foundations underlying InTabular's approach to semantic data management.*

AGI will eventually build an even more advanced semantic layer around information management. This is but a draft on how an initial version might unfold itself. In order to enable this, the structure is created in a modular and adaptive way. Ultimately, it can be imagined as a "knowledge core" that is managed by InTabular. Any information that wants to become part of the knowledge effectively will need to pass the knowledge gatekeeping system, for which InTabular provides a possible implementation. However, there could be other, more advanced implementations for this.

InTabular attempts not only to provide the first-ever implementation of this, but also to showcase the general philosophy of how a gatekeeper can be imagined in a general sense. We suspect that any AGI-like system will need to have its own knowledge core managed by a very advanced gatekeeper.

### Mathematical Foundation

Let $A$ be some incoming data in the form of a `.csv` file. Let $D$ be the curated database. Let $I$ be the actual intention that the gatekeeper (i.e., the person who instantiated the database) has for this database. Then fundamentally, the gatekeeper has a write function $g_w$ which is to be used to write into $D$.

For $A$, that would look like:

$g_w(A, D, I) \rightarrow D'$

This essentially means that the gatekeeper is a function of the current knowledge and the incoming information, producing a new curated data structure $D'$.

More generally, it holds that for any $d \in D$ (i.e., a unit of knowledge), we can define (I KNOW THIS IS NOT CORRECT FORMALLY - WILL FIX LATER):

$g_w(A, D, I) = \forall d \in D,\ g_{d_w}(A, d, I)$

This means that the gatekeeper performs write operations on **each unit of information** with respect to the incoming data $A$ and intent $I$.

Furthermore, any restriction of $g$ within the realization is constrained by the **fundamental law** that:

> *Without any assumption, no learning can occur.*

We denote this by $L_1$.

This is a core tenet of learning theory, and thus this law applies universally. Any realization of the gatekeeper necessarily carries certain assumptions imposed by what one might call the **causality of $I$**. If $I$ is the fundamental goal, then by constructing the database, we inherently impose assumptions — possibly unknowingly.

These assumptions cannot be escaped by generalization because of $L_1$.

More specifically, $I$ often carries far more than is practically specified in the realization of InTabular. For example, if we write:

> "We want customer relationship data that helps us maintain good relations with customers around Linz, Austria"

— this imposes hidden assumptions:
- Why do we want happy customers?
- What does it mean to *be* a customer?
- Is a tabular format appropriate for representing relationships?
- Why is one row equal to one customer?

All of these are **epistemic impositions** embedded into the use of InTabular. So be warned.

### Practical Implementation

Taking all those assumptions into account, InTabular bluntly assumes it is reasonable to use:

**1. Column merging:**

We assume that both humans (and modern LLMs) can, to a large extent, understand what a column means by:
- The **column name**
- The **first two non-empty values**

This is used to allow **column merging** across tabular datasets.

To simplify this, we assume **semantic independence** of all columns in $D$ — i.e., columns like `"first name"` and `"last name"` are treated as entirely independent (we iterate over them separately).

**2. Row merging:**

We assume that some columns can act as **pseudo-unique keys** (which in turn assumes that **entities are a thing**, kek).

However, we **do not assume** exact value matches. Instead, we apply **heuristic similarity**:

> `"Alexander Krauck"` ≈ `"Krauck Alexander"`

This behavior is **schema-configurable**, so strictness can be adjusted.

**3. What we do not yet do:**

- 3a. Check **inter-row relationships**
- 3b. Perform **derived reasoning** (i.e. second-order inference guided by $I$)
- 3c. Allow **Prosa-based read/write**

Prosa can be very broad, so we will likely start small:
- First with **single-row read/write**
- Then expand as capability allows

But we will probably leave the truly insane parts to **AGI**, the boy.

Not that he gets bored or so. 🙂

---

## 📄 License

MIT License - see [LICENSE](LICENSE) file for details.

---

**Transform your messy CSV data into structured intelligence** 🎯✨