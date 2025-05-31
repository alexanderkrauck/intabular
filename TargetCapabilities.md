# TargetCapabilities.md

## 🎯 Vision: Revolutionary Semantic CRUD System

**InTabular aims to be the world's first truly intelligent data management system that understands the semantic meaning of your data, not just its structure.**

## 🧠 Core Philosophy

### Semantic vs Syntactic Schema

- **Traditional Approach**: Column names, data types, constraints (syntactic)
- **InTabular Approach**: Business meaning, entity relationships, semantic purpose (semantic)
- **Hybrid Reality**: Semantic understanding drives syntactic decisions

The system maintains a **semantic schema** that describes what the data *means* in business terms, while automatically managing the underlying syntactic structure to support those semantics.

### AGI aware software architecture

AGI will eventually build an even more advanced semantic layer around information management. This is but a draft on how an initial version might unfold itself. In order to enable this, the structure is created in a modular and adaptive way. Ultimately it can be imagined as a "knowledge core" that is managed by intabular. Any information that wants to become part of the knowledge effectively will need to pass the knowledge gatekeeping system which intabular provides a possible implementation for. However, there could be other more advanced implementations for this.

Intabular attempts to not only provide the first ever implementation of this but instead it also attempts to showcase the general philosophy of how a gatekeeper can be imagined in a general sense. We suspect that any AGI like system will need to have its own knowledge core managed by a very advanced gatekeeper.  In the following we attempt to visually showcase how intabular attempts to do that.

#### Example

Let A be some incoming data in the form of a .csv file. Also let D be the curated dataabse. Also let I be the actual intention that the gatekeeper/person who instantiated the database/whatever has for this database. Then fundamentally the gate keeper has a write function, call it g_w which is to be used to write into D. For A that would then look like

g_w(A, D, I) -> D'

which essentially means that the gatekeeper is a function of the current knowledge and the incoming information to produce a new curated data structure D'.

There are (practically infinitely) many way that this kind of architecture unfolds itself within currently present architectures and also future architectures. For example in LSTMs/xLSTMs we have a cell state which can be fully represented by this.

The challenge lies in formulating a very effective g_w that can perfectly fulfill I which is precisely what intabular attempts.

For this .csv we do that by recognizing that within tabular databases can be understood as a list of columns, where each column effectively fulfills a purpose for I. Moreover, we can recognize that if we had two tables that both in a similar way attempt to fulfill I (notice strong nuance here) then it can be assumed that there are actually multiple columns within those to tables that are semantically identical while being labeled differently. More generally, it might also be that there are actually multiple columns within one table that are present in the other table as different columns with different labels and possible different amount.

##### Demonstrate

Table 1: Email Address,
Table 2: Full Name

Then we can, in the case that the email address might look like fistname.lastname@whatever.org actually reasonably argue that we can extract the full name from the email, at least with a certain level of probabilistic support.

This however, is significantly more nuanced and difficult to resolve than the initial "column-to-column perfect match" way of doing it.

##### Further implications (a bit advanced)

More generally one could see that it effectively holds that for any d in D a unit of knowledge actually g_w(A, D, I) = forall d in D gd_w(A,d,I). That effectively means that the gatekeeper does write on any unit of information with any incoming information.

Further it means that any restriction of g within the realization is formed by the fundamental law that without any assumption there can not be any learning, from now on denoted by L1. This here is at its core learning theory and thus is law applies. Thus, any realization of the gatekeeper always carries a certain assumption imposed by something one might called causality of I. If I is the fundamental goal then by constructing the database we fundamentally impose assumptions on this, possibly unknowlingly. And those assumption can not be escaped by generalization because of L1.

More specifically I carries much more than what might be pracically specified in realization of intabular. For example if we write down "We want customer relationship data that helps us keep nice relations with customers around Linz, Austria", then this imposes hidden assumptions, like why we are actually attempting to have happy customers, what it means to be a customer or, even further, in using intabular with this assumption we impose the assumption that a tabular format is reasonably for storing customer relations ship data, one row for one customer. Those are all impositions made when using intabular, so be warned.

##### Practical implementation

So taking into account all those assumptions, intabular assumes bluntly that it is reasonable to use

1. column merging by
We assume we (and modern LLMs) can to a large extent understand what a column means by looking at the name of the column and the first two values in this column that are not empty/NaN. We use this to allow column merging of the tabular databases. In order to that we assume unrelatedness or semantic independence of all columns in D as it allows for a more effective implementation. That means we treat two columns like "first name" and "last name" if they are in D as entirely independent (we iterate over them).

2. row merging by
We assume that there are certain columns that can effectively represent a unique key (which assumes that we assume that entities are a thing kek). However, we do not assume that the valuation is completely identical but use heuristical methods to give it some freedom. For example "Alexander Krauck" and "Krauck Alexander" can be recognized by us to be the same entity. However, this setting can be tuned by modifying the defined schema of intabular.

3. We do not yet
3a. check inter-row relationships
3b. do derived reasoning (reasoning that makes sense as I imposes)
3c. Allow in prosea write and read. But this is the next thing we will do. Prosa can be very broad so we will also need to start step wise there. Single row read/write will likely be the first thing but of course technically speaking there are many options. But i think we will possibly leave some of the for AGI, the boy. Not that he gets bored or so.

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
