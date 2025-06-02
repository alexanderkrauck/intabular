import pandas as pd
import json
from intabular.core.config import GatekeeperConfig
from intabular.core.analyzer import DataframeAnalyzer
from intabular.core.strategy import DataframeIngestionStrategy
from intabular.main import setup_llm_client

def debug_strategy_creation():
    """Debug why company_name gets 'none' transformation type"""
    
    # Load the same data and config as the test
    config = GatekeeperConfig.from_yaml('test/data/configs/customer_crm.yaml')
    df = pd.read_csv('test/data/csv/perfect_match.csv')
    
    # Setup the same components as the test
    client = setup_llm_client()
    analyzer = DataframeAnalyzer(client, config)
    strategy_creator = DataframeIngestionStrategy(client)
    
    # Analyze the dataframe
    print("=== ANALYZING DATAFRAME ===")
    analysis = analyzer.analyze_dataframe_structure(df, "Debug test")
    
    print("=== DATAFRAME ANALYSIS ===")
    print(json.dumps(analysis.dataframe_column_analysis, indent=2))
    
    print("\n=== ENTITY COLUMNS FROM CONFIG ===")
    for col_name, col_config in config.entity_columns.items():
        print(f"{col_name}: {col_config}")
    
    print("\n=== CREATING STRATEGY ===")
    strategy = strategy_creator.create_ingestion_strategy(config, analysis)
    
    print("\n=== NO MERGE COLUMN MAPPINGS ===")
    for col_name, mapping in strategy.no_merge_column_mappings.items():
        print(f"{col_name}: {mapping}")
        
    print("\n=== PROBLEMATIC ENTITY COLUMNS ===")
    for col_name, mapping in strategy.no_merge_column_mappings.items():
        if col_name in config.entity_columns:
            if mapping['transformation_type'] == 'none':
                print(f"PROBLEM: {col_name} has transformation_type 'none'")
                print(f"  Config: {config.entity_columns[col_name]}")
                print(f"  Analysis: {analysis.dataframe_column_analysis.get(col_name, 'NOT FOUND')}")
                print(f"  Mapping: {mapping}")

if __name__ == "__main__":
    debug_strategy_creation() 