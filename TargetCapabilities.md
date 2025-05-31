# TargetCapabilities.md

## 🎯 Vision: Revolutionary Semantic CRUD System

**InTabular aims to be the world's first truly intelligent data management system that understands the semantic meaning of your data, not just its structure.**

## 🧠 Core Philosophy

### Semantic vs Syntactic Schema

- **Traditional Approach**: Column names, data types, constraints (syntactic)
- **InTabular Approach**: Business meaning, entity relationships, semantic purpose (semantic)
- **Hybrid Reality**: Semantic understanding drives syntactic decisions

The system maintains a **semantic schema** that describes what the data *means* in business terms, while automatically managing the underlying syntactic structure to support those semantics.

## 🛣️ Implementation Strategy

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

## Use Cases

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
