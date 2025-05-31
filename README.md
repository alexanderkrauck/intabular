# InTabular - Intelligent CSV Data Ingestion

**Automatically map unknown CSV structures to your target schemas using AI**

Transform messy, unknown CSV files into clean, structured data that fits your target schema - without manual field mapping or complex ETL pipelines.

## 🎯 What InTabular Does

**The Problem**: You have a well-structured target table, but data comes from various sources with different column names, formats, and structures.

**The Solution**: InTabular uses AI to automatically understand your source data and intelligently map it to your target schema.

## 🚀 Quick Start

### Installation

```bash
pip install -e .  # Install from source
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
# Generate config from table purpose
python -m intabular config customers.csv "Customer master database for CRM and outreach"
```

This creates `customers_config.yaml`:
```yaml
purpose: "Customer master database for CRM and outreach"
enrichment_columns:
  - email
  - first_name
  - last_name
  - company
  - title
  - phone
  - website
column_policy: "balance_completeness_conciseness"
target_file_path: "customers.csv"
```

**Step 2: Ingest unknown CSV files**
```bash
# AI automatically maps fields and transforms data
python -m intabular customers_config.yaml unknown-leads.csv
```

**That's it!** Your data is now mapped to your schema and saved to `customers.csv`.

## 🧠 How It Works

InTabular uses a 4-step AI pipeline:

### 1. **Data Analysis**
- Analyzes column names and actual data content
- Identifies business context and data quality
- Detects data types (identifier vs text content)

### 2. **Strategy Creation**
- Maps source columns to target schema fields  
- Creates dual strategies for existing vs empty target data
- Chooses optimal transformation approaches:
  - **Replace**: Direct column mapping
  - **Derive**: Format-based combination of fields
  - **Concat**: Simple concatenation with separators
  - **Prompt_merge**: LLM-powered intelligent merging
  - **Preserve**: Keep existing target data

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
enrichment_columns:
  - email
  - full_name  
  - company_name
  - location
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

```python
from intabular import run_ingestion_pipeline, create_config

# Create configuration
create_config("customers.csv", "Customer relationship database")

# Run ingestion pipeline  
result_df = run_ingestion_pipeline("customers_config.yaml", "unknown-data.csv", "output.csv")
print(f"Processed {len(result_df)} rows")
```

## 🛠️ Command Line Interface

```bash
# Create configuration from table structure
python -m intabular config <table_path> <purpose>

# Ingest CSV with existing configuration
python -m intabular <yaml_config> <csv_file> [output_file]
```

## 🎯 Vision: Revolutionary Semantic CRUD System

**InTabular aims to be the world's first truly intelligent data management system that understands the semantic meaning of your data, not just its structure.**

### Semantic vs Syntactic Schema

- **Traditional Approach**: Column names, data types, constraints (syntactic)
- **InTabular Approach**: Business meaning, entity relationships, semantic purpose (semantic)
- **Hybrid Reality**: Semantic understanding drives syntactic decisions

The system maintains a **semantic schema** that describes what the data *means* in business terms, while automatically managing the underlying syntactic structure to support those semantics.

## 🚀 Target Capabilities

### 1. **Semantic Data Ingestion**

Transform any CSV into your target schema by understanding content meaning, not just column matching.

**Examples:**

- Recognize that "fname + lname" → "full_name"
- Understand "corp_email" and "personal_email" both map to "email" field
- Detect that "San Francisco, CA" and "SF, California" represent the same location concept

### 2. **Intelligent CRUD Operations**

#### **Add Data (INSERT)**

```
Input: New CSV with unknown structure
Action: Analyze, map, and append new records
Intelligence: Understand new data semantics and fit to existing schema
```

#### **Merge Data (UPDATE/INSERT)**

```
Input: CSV with potential duplicates/updates
Action: Smart merge based on semantic identity
Intelligence: Detect same entities across different data representations
```

#### **Merge-Add Data (UPSERT)** *[Default Strategy]*

```
Input: Any CSV file
Action: Add new entries OR update existing ones intelligently
Intelligence: Semantic duplicate detection and conflict resolution
```

#### **Smart Delete (DELETE)**

```
Input: Deletion criteria (CSV or natural language)
Action: Remove matching records with semantic understanding
Intelligence: Handle edge cases and relationship preservation
```

#### **Query & Retrieve (SELECT)**

```
Input: Natural language queries or structured filters
Action: Return semantically relevant results
Intelligence: Understand intent beyond literal matches
```

## 🌟 Advanced Use Cases

### **Use Case 1: Multi-Source Contact Management**

**Scenario**: Merge contacts from Salesforce, LinkedIn, Apollo, and manual CSV exports

**Traditional Problem**:

- Different column names ("email_address" vs "work_email" vs "contact_email")
- Duplicate detection limited to exact matches
- Manual field mapping required

**InTabular Solution**:

- Semantic understanding: All email fields → unified "email" concept
- Intelligent deduplication: "John Doe at Acme Corp" = "J. Doe, Acme Corporation"
- Automatic mapping based on content analysis

### **Use Case 2: Company-Centric Data Consolidation**

**Scenario**: Maintain one record per company, but ingest employee lists

**Intelligence**:

- Detects multiple people from same company
- Automatically consolidates: "Keep best contact from each company"
- Preserves company information while selecting optimal representative

**Natural Language Control**:

```
"Add this employee list, but keep only the highest-ranking person from each company"
"Merge this data, preferring contacts with phone numbers"
```

### **Use Case 3: Dynamic Schema Evolution**

**Scenario**: Target schema needs to adapt to new data types

**Intelligence**:

- Detect valuable unmapped columns: "This data has 'deal_value' info we're not capturing"
- Suggest schema enhancements: "Consider adding 'industry' field for better segmentation"
- Auto-evolve schema while preserving existing data integrity

### **Use Case 4: Natural Language Data Operations**

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

## 🛣️ Implementation Roadmap

### **Phase 1: Core Semantic CRUD** *(Current)*

- Intelligent CSV ingestion and mapping
- Basic semantic deduplication
- Schema-aware transformations

### **Phase 2: Natural Language Interface**

- Text-based operation commands
- Query understanding and execution
- User preference learning

### **Phase 3: Advanced Intelligence**

- Cross-table relationship management
- Predictive data suggestions
- Automatic schema evolution
- True truth-seeking database

## 🧠 AGI-Aware Software Architecture

AGI will eventually build an even more advanced semantic layer around information management. This is but a draft on how an initial version might unfold itself. In order to enable this, the structure is created in a modular and adaptive way. Ultimately, it can be imagined as a "knowledge core" that is managed by Intabular. Any information that wants to become part of the knowledge effectively will need to pass the knowledge gatekeeping system, for which Intabular provides a possible implementation. However, there could be other, more advanced implementations for this.

Intabular attempts not only to provide the first-ever implementation of this, but also to showcase the general philosophy of how a gatekeeper can be imagined in a general sense. We suspect that any AGI-like system will need to have its own knowledge core managed by a very advanced gatekeeper.

### Mathematical Foundation

Let $A$ be some incoming data in the form of a `.csv` file. Let $D$ be the curated database. Let $I$ be the actual intention that the gatekeeper (i.e., the person who instantiated the database) has for this database. Then fundamentally, the gatekeeper has a write function $g_w$ which is to be used to write into $D$.

For $A$, that would look like:

$g_w(A, D, I) \rightarrow D'$

This essentially means that the gatekeeper is a function of the current knowledge and the incoming information, producing a new curated data structure $D'$.

More generally, it holds that for any $d \in D$ (i.e., a unit of knowledge), we can define:

$g_w(A, D, I) = \forall d \in D,\ g_{d_w}(A, d, I)$

This means that the gatekeeper performs write operations on **each unit of information** with respect to the incoming data $A$ and intent $I$.

Furthermore, any restriction of $g$ within the realization is constrained by the **fundamental law** that:

> *Without any assumption, no learning can occur.*

We denote this by $L_1$.

This is a core tenet of learning theory, and thus this law applies universally. Any realization of the gatekeeper necessarily carries certain assumptions imposed by what one might call the **causality of $I$**. If $I$ is the fundamental goal, then by constructing the database, we inherently impose assumptions — possibly unknowingly.

These assumptions cannot be escaped by generalization because of $L_1$.

More specifically, $I$ often carries far more than is practically specified in the realization of Intabular. For example, if we write:

> "We want customer relationship data that helps us maintain good relations with customers around Linz, Austria"

— this imposes hidden assumptions:
- Why do we want happy customers?
- What does it mean to *be* a customer?
- Is a tabular format appropriate for representing relationships?
- Why is one row equal to one customer?

All of these are **epistemic impositions** embedded into the use of Intabular. So be warned.

### Practical Implementation

Taking all those assumptions into account, Intabular bluntly assumes it is reasonable to use:

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

## ⚡ Key Features

- **🧠 AI-Powered**: Uses GPT models for intelligent field mapping
- **🔧 Minimal Configuration**: Simple YAML-based schema definition
- **📊 Quality-Aware**: Maintains data integrity with validation
- **🎯 Business-Focused**: Understands business context and relationships
- **🔄 Dual-Strategy**: Handles both existing and empty target data
- **📈 Scalable**: Efficient parallel processing
- **🛡️ Privacy-Conscious**: Only sends metadata and samples to AI

## 🔒 Privacy & Security

InTabular is designed with privacy in mind:

- **Limited Data Sharing**: Only column names and sample values sent to OpenAI
- **No Bulk Data**: Your dataset never leaves your environment  
- **Local Processing**: All transformations happen locally
- **API Key Control**: You control your OpenAI API usage

## 📋 Requirements

- Python 3.8+
- OpenAI API key
- Dependencies: `pandas`, `openai`, `pyyaml`, `python-dotenv`, `numpy`

## 📄 License

MIT License - see [LICENSE](LICENSE) file for details.

---

**Transform your messy CSV data into structured intelligence** 🎯✨