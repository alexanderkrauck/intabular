"""
Main entry point for InTabular - Intelligent CSV data ingestion system.
Core business logic for 4 ingestion modes.
"""

import sys
import os
import yaml
import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any, Union, Tuple
from openai import OpenAI

from intabular.core.analyzer import DataframeAnalyzer
from intabular.core.config import GatekeeperConfig
from intabular.core.strategy import DataframeIngestionStrategy
from intabular.core.processor import DataframeIngestionProcessor
from intabular.core.logging_config import setup_logging, get_logger


def setup_llm_client() -> OpenAI:
    """Initialize LLM client with support for custom providers and configurations"""
    logger = get_logger('main')
    
    # Get API key
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        logger.critical("OPENAI_API_KEY environment variable not set")
        raise ValueError("OPENAI_API_KEY environment variable is required")
    
    # Get custom base URL if specified (for alternative LLM providers)
    base_url = os.getenv('INTABULAR_BASE_URL')
    
    # Get organization if specified
    organization = os.getenv('INTABULAR_ORGANIZATION')
    
    # Initialize client with custom parameters
    client_kwargs = {'api_key': api_key}
    if base_url:
        client_kwargs['base_url'] = base_url
        logger.info(f"Using custom LLM endpoint: {base_url}")
    if organization:
        client_kwargs['organization'] = organization
        logger.info(f"Using organization: {organization}")
    
    client = OpenAI(**client_kwargs)
    
    # Log model configuration
    strategy_model = os.getenv('INTABULAR_STRATEGY_MODEL', 'gpt-4o')
    processor_model = os.getenv('INTABULAR_PROCESSOR_MODEL', 'gpt-4o-mini')
    logger.info(f"Strategy model: {strategy_model}")
    logger.info(f"Processor model: {processor_model}")
    
    # Test the connection
    try:
        client.models.list()
        provider_name = "Custom LLM Provider" if base_url else "OpenAI"
        logger.info(f"{provider_name} API connection verified")
    except Exception as e:
        logger.error(f"LLM API connection failed: {e}")
        raise
    
    logger.info("LLM client initialized")
    return client


# =============================================================================
# CORE INGESTION MODES
# =============================================================================

def ingest_with_implicit_schema(df_ingest: pd.DataFrame, df_target: pd.DataFrame) -> Tuple[pd.DataFrame, GatekeeperConfig]:
    """
    Mode 1: Merge df_ingest into df_target with implicit schema inference - IMPLEMENTED.
    
    Args:
        df_ingest: DataFrame to be ingested
        df_target: Target DataFrame to merge into
        
    Returns:
        Tuple[pd.DataFrame, GatekeeperConfig]: (merged_df, inferred_schema)
    """
    logger = get_logger('main')
    logger.info(f"Mode 1: Implicit schema ingestion - {len(df_ingest)} rows into {len(df_target)} target rows")
    
    # Step 1: Use Mode 4 to infer schema from df_target
    inferred_schema = infer_schema_from_target(df_target, "Implicitly inferred schema from target structure")
    logger.info(f"Inferred schema with {len(inferred_schema.get_enrichment_column_names())} columns")
    
    # Step 2: Use Mode 3 to do the actual ingestion with inferred schema
    result_df = ingest_with_explicit_schema(df_ingest, df_target, inferred_schema)
    
    logger.info(f"Mode 1 complete: {len(result_df)} total rows with inferred schema")
    return result_df, inferred_schema


def ingest_to_schema(df_ingest: pd.DataFrame, schema: GatekeeperConfig) -> pd.DataFrame:
    """
    Mode 2: Transform df_ingest to match schema (no existing target) - IMPLEMENTED.
    
    Args:
        df_ingest: DataFrame to be transformed
        schema: Target schema configuration
        
    Returns:
        pd.DataFrame: Transformed DataFrame matching schema
    """
    logger = get_logger('main')
    logger.info(f"Mode 2: Schema transformation - {len(df_ingest)} rows to schema")
    
    # Create empty df_target with schema columns
    df_target = pd.DataFrame(columns=schema.get_enrichment_column_names())
    logger.info(f"Created empty target with {len(schema.get_enrichment_column_names())} columns")
    
    # Use Mode 3 with empty target
    return ingest_with_explicit_schema(df_ingest, df_target, schema)


def ingest_with_explicit_schema(df_ingest: pd.DataFrame, df_target: pd.DataFrame, schema: GatekeeperConfig) -> pd.DataFrame:
    """
    Mode 3: Merge df_ingest into df_target with explicit schema (IMPLEMENTED).
    
    Args:
        df_ingest: DataFrame to be ingested
        df_target: Target DataFrame to merge into
        schema: Explicit schema configuration
        
    Returns:
        pd.DataFrame: Merged DataFrame
    """
    logger = get_logger('main')
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
    result_df = processor.execute_ingestion(
        df_ingest,
        df_target,
    strategy,
        schema,
    df_analysis.general_ingestion_analysis
    )
    
    logger.info(f"Ingestion complete: {len(result_df)} total rows")
    return result_df


def infer_schema_from_target(df_target: pd.DataFrame, purpose: str = "Inferred schema") -> GatekeeperConfig:
    """
    Mode 4: Analyze df_target and return inferred schema.
    
    Args:
        df_target: DataFrame to analyze
        purpose: Business purpose description
        
    Returns:
        GatekeeperConfig: Inferred schema configuration
    """
    logger = get_logger('main')
    logger.info(f"Mode 4: Schema inference from {len(df_target)} rows, {len(df_target.columns)} columns")
    
    # TODO: Implement intelligent schema inference from DataFrame structure
    # TODO: Analyze column types, patterns, sample data to infer semantic meaning
    raise NotImplementedError("Mode 4: Schema inference not yet implemented")