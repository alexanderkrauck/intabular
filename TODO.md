# TODOs - Refocused on Core Value Proposition

**Core Vision**: Intent-driven ingestion of arbitrary data into coherent formats through intelligent schema automation.

## Essential (Core Value Proposition)

### 1. Automated Schema Intelligence

- **Auto-generate schemas from data**: When no schema exists, automatically create one by analyzing ingested data patterns and inferring business context
- **Intelligent schema creation from intent**: Enhance current prompt-based schema generation to be more sophisticated and business-context aware
- **Schema evolution**: Allow schemas to intelligently adapt when new data reveals missing but valuable dimensions

### 2. Enhanced Intent-Driven Ingestion

- **Improve ingestion pipeline analysis**: Better analysis of the ingestion process to identify and fix prompt/logic bottlenecks
- **Intelligent column addition**: Automatically suggest and add new columns when incoming data contains valuable information not captured by current schema
- **Robust data input evaluation**: Comprehensive test suite covering various data formats, edge cases, and robustness scenarios

### 3. Quality & Reliability

- **User feedback hooks**: Implement dry-run mode with user confirmation for assumption validation before data changes
- **Data trace architecture**: Complete traceability of data origins for each row, enabling rollbacks and audit trails
- **Error handling & validation**: Robust validation with clear error reporting and recovery mechanisms

## Important (Foundation)

### 4. Multi-Provider Support

- **LLM provider abstraction**: Support for different LLM providers including open-source models (but keep simple - avoid over-engineering)

### 5. Publishing & Documentation

- **PyPI publication**: Package and publish to PyPI for easy installation
- **Clean up codebase**: Remove outdated README sections, unused code, check for TODOs, ensure everything runs smoothly

## Vision & Theory

### 6. Intelligence & Agency Article

Write an article exploring:

- **The concept of intelligence in data systems**
- **Servitude of agents and representation in humans vs evolution**
- **Implications for AGI development**
- **How intent-driven data systems relate to broader intelligence concepts**

## Explicitly NOT Pursuing for now (Let Other Frameworks Handle)

❌ **Retrieval systems** - MCP and other frameworks handle this  
❌ **Complex data format support** (MySQL, etc.) - Standard ETL tools exist  
❌ **Large file memory handling** - Not core differentiator  
❌ **Nested structures** - Adds complexity without core value  
❌ **Semantic retrieval models** - Outside core focus  
❌ **Generalized learning/AGI features** - Too ambitious for this framework  

---

**Focus Principle**: Every feature must directly support "intent-driven ingestion of arbitrary data into coherent formats" or automated schema intelligence. If other frameworks already solve it well, we don't build it.
