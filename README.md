# InTabular

[![PyPI version](https://badge.fury.io/py/intabular.svg)](https://badge.fury.io/py/intabular)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Experimental Python package for mapping unknown CSV files into a target schema with LLM assistance.

Status: in development. The core idea is useful, but this repository should be treated as an experimental package rather than a mature ETL product.

## Problem

Real-world CSV files rarely match the schema you want:

- column names differ
- fields are split or merged
- formats are inconsistent
- identity columns are ambiguous
- manual mapping is repetitive

InTabular explores whether an LLM can help map messy source files into a user-defined target schema while keeping the target schema explicit.

## Core Idea

The user defines the target schema in YAML. The system inspects an unknown CSV, proposes mappings, and transforms rows into the target structure.

The target schema is not just column names. It includes descriptions, purpose, and identity hints so the model has more context for mapping decisions.

## Example Target Schema

```yaml
purpose: "Customer master database for CRM and outreach"

enrichment_columns:
  email:
    description: "Primary business email address"
    supports_purpose_by: "Required for email outreach and deduplication"
    is_entity_identifier: true
    identity_indication: 1.0

  full_name:
    description: "Contact's full name"
    supports_purpose_by: "Used for personalized communication"
    is_entity_identifier: true
    identity_indication: 0.8

  company_name:
    description: "Customer's organization name"
    supports_purpose_by: "Used for account-level grouping"
    is_entity_identifier: false
    identity_indication: 0.0

target_file_path: "customers.csv"
```

## Installation

```bash
pip install intabular
```

PyPI: [https://pypi.org/project/intabular/](https://pypi.org/project/intabular/)

For local development:

```bash
git clone https://github.com/alexanderkrauck/intabular.git
cd intabular
pip install -e .
```

## Configuration

Set an OpenAI API key:

```bash
export OPENAI_API_KEY="your-api-key"
```

or create a local `.env` file:

```text
OPENAI_API_KEY=your-api-key
```

Do not commit `.env` files.

## Usage

Create or edit a schema config:

```bash
python -m intabular config customers.csv "Customer master database for CRM and outreach"
```

Run ingestion:

```bash
python -m intabular customers_config.yaml unknown-leads.csv
```

The transformed data is written to the configured `target_file_path`.

## Repository Structure

```text
intabular/
├── intabular/              # package code
├── test/                   # test data and examples
├── intabular_demo.py       # demo script
├── yaml_example.yaml       # example schema
├── pyproject.toml
└── README.md
```

## Current Limitations

- Mapping quality depends on schema descriptions.
- Automatic config generation is only a starting point.
- The package is not yet hardened for high-volume production ETL.
- LLM outputs need validation for critical data workflows.

## Status

Experimental package. Useful as a prototype for schema-guided ingestion, but not positioned as a finished production data platform.
