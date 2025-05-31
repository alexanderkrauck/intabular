# TargetCapabilities.md

## 🎯 Vision: Revolutionary Semantic CRUD System

**InTabular aims to be the world's first truly intelligent data management system that understands the semantic meaning of your data, not just its structure.**

## 🧠 Core Philosophy

### Semantic vs Syntactic Schema

- **Traditional Approach**: Column names, data types, constraints (syntactic)
- **InTabular Approach**: Business meaning, entity relationships, semantic purpose (semantic)
- **Hybrid Reality**: Semantic understanding drives syntactic decisions

The system maintains a **semantic schema** that describes what the data *means* in business terms, while automatically managing the underlying syntactic structure to support those semantics.

### AGI Aware Software Architecture

AGI will eventually build an even more advanced semantic layer around information management. This is but a draft on how an initial version might unfold itself. In order to enable this, the structure is created in a modular and adaptive way. Ultimately, it can be imagined as a "knowledge core" that is managed by Intabular. Any information that wants to become part of the knowledge effectively will need to pass the knowledge gatekeeping system, for which Intabular provides a possible implementation. However, there could be other, more advanced implementations for this.

Intabular attempts not only to provide the first-ever implementation of this, but also to showcase the general philosophy of how a gatekeeper can be imagined in a general sense. We suspect that any AGI-like system will need to have its own knowledge core managed by a very advanced gatekeeper. In the following, we attempt to visually showcase how Intabular attempts to do that.

---

#### Example

Let $A$ be some incoming data in the form of a `.csv` file. Let $D$ be the curated database. Let $I$ be the actual intention that the gatekeeper (i.e., the person who instantiated the database) has for this database. Then fundamentally, the gatekeeper has a write function $g_w$ which is to be used to write into $D$.

For $A$, that would look like:

$g_w(A, D, I) \rightarrow D'$

This essentially means that the gatekeeper is a function of the current knowledge and the incoming information, producing a new curated data structure $D'$.

There are (practically infinitely) many ways that this kind of architecture unfolds itself within currently present and future architectures. For example, in LSTMs/xLSTMs, we have a cell state which can be fully represented by this formulation.

The challenge lies in formulating a very effective $g_w$ that can perfectly fulfill $I$ — which is precisely what Intabular attempts.

For this `.csv`, we recognize that tabular databases can be understood as a list of columns, where each column effectively fulfills a purpose defined by $I$. Moreover, if we have two tables that both attempt to fulfill $I$ in a similar way (notice the nuance), then it can be assumed that there are actually multiple columns in those tables that are **semantically identical** while being labeled differently. More generally, it might also be that multiple columns within one table are present in another table with different labels and potentially different cardinality.

---

##### Demonstrate

- Table 1: `Email Address`
- Table 2: `Full Name`

Then we can — if the email looks like `firstname.lastname@whatever.org` — reasonably argue that we can extract the full name from the email, at least with some level of probabilistic support.

However, this is significantly more nuanced and difficult to resolve than the simple "column-to-column perfect match" approach.

---

##### Further Implications (a bit advanced)

More generally, it holds that for any $d \in D$ (i.e., a unit of knowledge), we can define:

$g_w(A, D, I) = \forall d \in D,\ g_{d_w}(A, d, I)$

This means that the gatekeeper performs write operations on **each unit of information** with respect to the incoming data $A$ and intent $I$.

Furthermore, any restriction of $g$ within the realization is constrained by the **fundamental law** that:

> *Without any assumption, no learning can occur.*

We denote this by $L_1$.

This is a core tenet of learning theory, and thus this law applies universally. Any realization of the gatekeeper necessarily carries certain assumptions imposed by what one might call the **causality of $I$**. If $I$ is the fundamental goal, then by constructing the database, we inherently impose assumptions — possibly unknowingly.

These assumptions cannot be escaped by generalization because of $L_1$.

More specifically, $I$ often carries far more than is practically specified in the realization of Intabular. For example, if we write:

> “We want customer relationship data that helps us maintain good relations with customers around Linz, Austria”

— this imposes hidden assumptions:
- Why do we want happy customers?
- What does it mean to *be* a customer?
- Is a tabular format appropriate for representing relationships?
- Why is one row equal to one customer?

All of these are **epistemic impositions** embedded into the use of Intabular. So be warned.

---

##### Practical Implementation

Taking all those assumptions into account, Intabular bluntly assumes it is reasonable to use:

---

**1. Column merging:**

We assume that both humans (and modern LLMs) can, to a large extent, understand what a column means by:
- The **column name**
- The **first two non-empty values**

This is used to allow **column merging** across tabular datasets.

To simplify this, we assume **semantic independence** of all columns in $D$ — i.e., columns like `"first name"` and `"last name"` are treated as entirely independent (we iterate over them separately).

---

**2. Row merging:**

We assume that some columns can act as **pseudo-unique keys** (which in turn assumes that **entities are a thing**, kek).

However, we **do not assume** exact value matches. Instead, we apply **heuristic similarity**:

> `"Alexander Krauck"` ≈ `"Krauck Alexander"`

This behavior is **schema-configurable**, so strictness can be adjusted.

---

**3. What we do not yet do:**

- 3a. Check **inter-row relationships**
- 3b. Perform **derived reasoning** (i.e. second-order inference guided by $I$)
- 3c. Allow **Prosa-based read/write**

Prosa can be very broad, so we will likely start small:
- First with **single-row read/write**
- Then expand as capability allows

But we will probably leave the truly insane parts to **AGI**, the boy.

Not that he gets bored or so. 🙂


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
