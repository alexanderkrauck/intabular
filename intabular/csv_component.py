"""
CSV and file handling component for InTabular.
Separates file I/O concerns from core business logic.
"""

import pandas as pd
from pathlib import Path
from typing import Optional
from openai import OpenAI
import os

from intabular.core.config import GatekeeperConfig
from intabular.core.logging_config import get_logger
from intabular.core.analyzer import DataframeAnalyzer
from intabular.core.strategy import DataframeIngestionStrategy
from intabular.core.processor import DataframeIngestionProcessor


def setup_llm_client() -> OpenAI:
    """Initialize LLM client - copied from main to avoid circular import"""
    logger = get_logger('csv_component')
    
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        logger.critical("OPENAI_API_KEY environment variable not set")
        raise ValueError("OPENAI_API_KEY environment variable is required")
    
    base_url = os.getenv('INTABULAR_BASE_URL')
    organization = os.getenv('INTABULAR_ORGANIZATION')
    
    client_kwargs = {'api_key': api_key}
    if base_url:
        client_kwargs['base_url'] = base_url
    if organization:
        client_kwargs['organization'] = organization
    
    client = OpenAI(**client_kwargs)
    
    # Test connection
    try:
        client.models.list()
        logger.info("LLM client initialized")
    except Exception as e:
        logger.error(f"LLM API connection failed: {e}")
        raise
    
    return client


def run_csv_ingestion_pipeline(yaml_config_file: str, csv_to_ingest: str) -> pd.DataFrame:
    """
    CSV wrapper around core ingestion logic.
    Loads CSV files, runs Mode 3 ingestion, saves result.
    
    Args:
        yaml_config_file: Path to YAML configuration
        csv_to_ingest: Path to CSV file to ingest
        
    Returns:
        pd.DataFrame: The processed result
    """
    logger = get_logger('csv_component')
    
    logger.info(f"CSV ingestion: {csv_to_ingest} -> {yaml_config_file}")
    
    # Load configuration and CSV files
    schema = GatekeeperConfig.from_yaml(yaml_config_file)
    df_ingest = pd.read_csv(csv_to_ingest)
    
    # Load or create target DataFrame
    if Path(schema.target_file_path).exists():
        df_target = pd.read_csv(schema.target_file_path)
        logger.info(f"Loaded existing target: {len(df_target)} rows")
    else:
        df_target = pd.DataFrame(columns=schema.get_enrichment_column_names())
        logger.info(f"Created empty target with {len(schema.get_enrichment_column_names())} columns")
    
    # Core Mode 3 logic (explicit schema ingestion) - implemented directly here
    logger.info(f"Mode 3: Explicit schema ingestion - {len(df_ingest)} + {len(df_target)} rows")
    
    # Initialize components
    client = setup_llm_client()
    analyzer = DataframeAnalyzer(client, schema)
    strategy_creator = DataframeIngestionStrategy(client)
    processor = DataframeIngestionProcessor(client)
    
    logger.info(f"Schema: {schema.purpose[:80]}... ({len(schema.get_enrichment_column_names())} columns)")
    
    # Analyze the ingestion DataFrame
    logger.info("Analyzing ingestion DataFrame...")
    df_analysis = analyzer.analyze_dataframe_structure(df_ingest)
    
    # Create intelligent strategy
    logger.info("Creating field-mapping strategy...")
    strategy = strategy_creator.create_ingestion_strategy(schema, df_analysis)
    
    # Execute ingestion
    logger.info("Executing ingestion...")
    result = processor.execute_ingestion(
        df_ingest,
        df_target,
        strategy,
        schema,
        df_analysis.general_ingestion_analysis
    )
    
    # Save results
    result.to_csv(schema.target_file_path, index=False)
    logger.info(f"Saved {len(result)} rows to {schema.target_file_path}")
    
    return result


def create_config_from_csv(table_path: str, purpose: str, output_yaml: Optional[str] = None) -> str:
    """
    Create YAML configuration by analyzing existing CSV structure.
    
    Args:
        table_path: Path to existing CSV table
        purpose: Business purpose description
        output_yaml: Optional output YAML file path
        
    Returns:
        str: Path to created YAML file
    """
    logger = get_logger('csv_component')
    
    if Path(table_path).exists():
        logger.info(f"Analyzing CSV structure: {table_path}")
        df = pd.read_csv(table_path)
        enrichment_columns = {col: {"description": f"Auto-detected column: {col}", "match_type": "semantic"} 
                             for col in df.columns}
        logger.info(f"Found {len(enrichment_columns)} columns")
    else:
        # Default enrichment columns
        default_cols = ["email", "first_name", "last_name", "company", "title", "phone", "website"]
        enrichment_columns = {col: {"description": f"Default column: {col}", "match_type": "semantic"} 
                             for col in default_cols}
        logger.warning(f"CSV not found, using default columns: {list(enrichment_columns.keys())}")
    
    config = GatekeeperConfig(
        purpose=purpose,
        enrichment_columns=enrichment_columns,
        target_file_path=table_path
    )
    
    yaml_file = output_yaml or f"{Path(table_path).stem}_config.yaml"
    config.to_yaml(yaml_file)
    
    logger.info(f"Configuration saved: {yaml_file}")
    return yaml_file 