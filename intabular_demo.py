# %%
"""
InTabular Demo - Intelligent CSV Data Ingestion

This demo shows how to use InTabular to automatically map unknown CSV structures 
to your well-defined schemas using AI.

🎯 What we'll cover:
1. Installation & Setup
2. Creating Target Schemas
3. Analyzing Unknown CSVs  
4. Intelligent Data Mapping
5. Real-world Examples
"""

# %%
# 1. Installation & Setup
print("🚀 InTabular Demo")
print("=" * 50)

# Import required libraries
import pandas as pd
import os
from pathlib import Path
from dotenv import load_dotenv

# Import InTabular
from intabular import AdaptiveMerger, TableConfig

print("✅ InTabular imported successfully!")
print("📦 Ready for intelligent CSV data ingestion")
load_dotenv()
# %%

merger = AdaptiveMerger()
#%%
merger.ingest_csv("yaml_example.yaml", "apollo-contacts-export.csv")


# %%
