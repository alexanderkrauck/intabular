# %%
"""
InTabular Demo - Intelligent CSV Data Ingestion

This demo shows how to use InTabular to automatically map unknown CSV structures 
to your well-defined schemas using AI.

🎯 What we'll cover:
1. Installation & Setup with Logging
2. Creating Target Schemas
3. Analyzing Unknown CSVs  
4. Intelligent Data Mapping
5. Real-world Examples
"""

# %%
# Setup
print("🚀 InTabular Demo")
print("=" * 50)

# Import required libraries
import pandas as pd
import os
from pathlib import Path
from dotenv import load_dotenv

# Import InTabular - using new architecture
from intabular import setup_logging
from intabular.csv_component import run_csv_ingestion_pipeline

print("✅ InTabular imported successfully!")
load_dotenv()

# Enable INFO logging
setup_logging(level="INFO", console_output=True)

# %%
# Run ingestion with full logging - using new CSV component
result_df = run_csv_ingestion_pipeline("yaml_example.yaml", "data/target_with_emails_test10.csv")
print(f"✅ Processed {len(result_df)} rows")

# %%
