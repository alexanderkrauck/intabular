# New Strategy Algorithm: Entity-Aware Column Mapping and Merging

## Overview

This document outlines the new algorithm for intelligently mapping and merging CSV data based on entity identification and column-level configuration. The algorithm uses a **phased approach** where entity matching is determined first using only the strongest identity columns, then all other columns are processed.

## Key Concepts

### Entity Identifiers
- **`is_entity_identifier: true`**: Columns that help identify unique entities (email, name, phone, etc.)
- **`identity_indication`**: Float value (0.0-1.0) indicating how strongly this column identifies an entity
- **Entity Match Threshold**: Sum of `identity_indication` values ≥ 1.0 means same entity

### Column Types
- **Entity Matching Columns**: `is_entity_identifier: true` AND `identity_indication > 0`
- **Remaining Columns**: All other columns (entity identifiers with `identity_indication = 0` + descriptive columns)

## Algorithm Flow

### Phase 1: Entity Matching Column Processing

**Process ONLY columns where `is_entity_identifier: true` AND `identity_indication > 0`**

These are the columns that actually contribute to entity matching decisions:
- Transform these columns first using `format`/`llm_format`/`none` strategies
- Generate standardized, matchable values for entity comparison

### Phase 2: Entity Matching Decision

Using the transformed Phase 1 columns:
1. **Extract Identity Values**: Get values for all Phase 1 columns from each source row
2. **Calculate Match Scores**: For each existing target row:
   - Compare identity column values (exact match after normalization)
   - Sum `identity_indication` values for matching columns
   - If sum ≥ 1.0: **Entity Match Found**
   - If sum < 1.0: **New Entity**

### Phase 3: Remaining Column Processing

**Process all columns NOT in Phase 1**

This includes:
- Entity identifier columns with `identity_indication = 0` (contribute to entity definition but not matching)
- All descriptive columns (`is_entity_identifier: false`)

### Phase 4: Merging Strategy Application

#### For Entity Matches (same entity):
- **All Entity Identifier Columns** (`is_entity_identifier: true`):
  - **Prefer Target**: Keep existing target values
  - **Fill Empty**: Only use source value if target is empty/null
  - **Preserve Identity**: Maintain entity consistency

- **Descriptive Columns** (`is_entity_identifier: false`):
  - **LLM-Based Merging**: Use intelligent combination strategies

#### For New Entities:
- **Create New Row**: Add as new entity with all transformed values
- **Fill All Columns**: Use transformed source data for all available columns

## Implementation Details

### Phased Processing Structure

```python
# Phase 1: Entity matching columns only (identity_indication > 0)
entity_matching_columns = {
    "email": {"is_entity_identifier": True, "identity_indication": 1.0},
    "full_name": {"is_entity_identifier": True, "identity_indication": 0.5}
}

# Phase 2: Remaining columns  
remaining_columns = {
    "company_name": {"is_entity_identifier": True, "identity_indication": 0.0},  # Entity ID but no match strength
    "deal_stage": {"is_entity_identifier": False, "identity_indication": 0.0},   # Descriptive
    "notes": {"is_entity_identifier": False, "identity_indication": 0.0}         # Descriptive
}
```

### Entity Matching Logic

```python
def calculate_entity_match_score(source_row, target_row, entity_matching_columns):
    total_score = 0.0
    # Only use Phase 1 columns for matching
    for col_name, col_config in entity_matching_columns.items():
        if source_row.get(col_name) == target_row.get(col_name):
            total_score += col_config.get('identity_indication', 0.0)
    return total_score

# Match threshold: score >= 1.0 means same entity
```

### Simplified Transformation Types

**Entity Identifier Columns**:
- **`format`**: Deterministic transformations (combine, regex, normalization)
- **`llm_format`**: LLM-based complex formatting
- **`none`**: No suitable mapping

**Descriptive Columns**:
- **`format`**: Simple transformations
- **`none`**: No suitable mapping

## Advantages

1. **Phased Processing**: Entity matching determined first using strongest identifiers
2. **Efficient**: Avoids unnecessary processing until entity relationships are clear
3. **Accurate**: Uses only relevant columns for entity matching decisions
4. **Flexible**: Handles both identity-contributing and non-contributing entity identifiers
5. **Clear Separation**: Distinct handling of entity matching vs data enrichment

## Example Scenario

### Configuration
```yaml
email: {is_entity_identifier: true, identity_indication: 1.0}      # Phase 1 - Strong matcher
full_name: {is_entity_identifier: true, identity_indication: 0.5}  # Phase 1 - Moderate matcher  
company_name: {is_entity_identifier: true, identity_indication: 0.0}  # Phase 2 - Identity but no match strength
deal_stage: {is_entity_identifier: false, identity_indication: 0.0}   # Phase 2 - Descriptive data
notes: {is_entity_identifier: false, identity_indication: 0.0}        # Phase 2 - Descriptive data
```

### Processing Flow

**Phase 1**: Transform only `email` and `full_name`
- Source: `john@ACME.com, John Smith`
- Transformed: `john@acme.com, john smith`

**Phase 2**: Match against existing entities using Phase 1 data
- Target: `john@acme.com, john smith, ...`
- Match Score: email(1.0) + name(0.5) = 1.5 ≥ 1.0 → **Same Entity**

**Phase 3**: Transform remaining columns
- `company_name`: Transform "ACME Corp" → "acme corp"
- `deal_stage`: Transform "qualified" → "qualified"  
- `notes`: Transform source notes

**Phase 4**: Apply merging strategies
- Entity identifiers: Prefer existing, fill empty
- Descriptive columns: LLM-based intelligent merging

### Result
Entity match found, data merged intelligently with proper precedence rules.

This phased algorithm ensures **accurate entity matching** by using only the most reliable identity indicators, then **comprehensive data enrichment** using all available information. 