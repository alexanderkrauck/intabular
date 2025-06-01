"""
Main entry point for InTabular - Intelligent CSV data ingestion system.
"""

import sys
import os
import yaml
import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any
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


def run_ingestion_pipeline(yaml_config_file: str, csv_to_ingest: str) -> pd.DataFrame:
    """Run the complete intelligent CSV ingestion pipeline"""
    
    logger = get_logger('main')
    
    logger.info(f"Starting ingestion pipeline: {csv_to_ingest} -> {yaml_config_file}")
    
    # Initialize components
    client = setup_llm_client()
    
    # Load target configuration
    target_config = GatekeeperConfig.from_yaml(yaml_config_file)
    
    analyzer = DataframeAnalyzer(client, target_config)
    strategy_creator = DataframeIngestionStrategy(client)
    processor = DataframeIngestionProcessor(client)
    

    
    logger.info(f"Target: {target_config.purpose[:80]}... ({len(target_config.get_enrichment_column_names())} columns)")
        

    # Read the CSV to ingest and the target table
    df_to_ingest = pd.read_csv(csv_to_ingest)#TODO This already assumes we have a header
    df_to_enrich = pd.read_csv(target_config.target_file_path) if Path(target_config.target_file_path).exists() else pd.DataFrame(columns=target_config.get_enrichment_column_names())
    
    # Analyze the CSV
    logger.info("Analyzing CSV...")
    df_analysis = analyzer.analyze_dataframe_structure(df_to_ingest)
    
    
    # Create intelligent strategy
    logger.info("Creating field-mapping strategy...")
    strategy = strategy_creator.create_ingestion_strategy(target_config, df_analysis)
    
    # Execute ingestion
    logger.info("Executing ingestion...")
    enriched_df = processor.execute_ingestion(
    df_to_ingest,
    df_to_enrich,
    strategy,
    target_config,
    df_analysis.general_ingestion_analysis
    )
    
    # Save results
    enriched_df.to_csv(target_config.target_file_path, index=False)
    
    # Final summary
    logger.info(f"Ingestion complete: {len(enriched_df)} rows saved to {target_config.target_file_path}")
    
    return enriched_df


def infer_config_from_table(table_path: str, purpose: str) -> GatekeeperConfig:
    """Infer configuration from an existing table structure"""
    
    logger = get_logger('main')
    
    if Path(table_path).exists():
        logger.info(f"Inferring schema from existing table: {table_path}")
        df = pd.read_csv(table_path)
        enrichment_columns = list(df.columns)
        logger.info(f"Found {len(enrichment_columns)} columns")
    else:
        # Default enrichment columns
        enrichment_columns = ["email", "first_name", "last_name", "company", "title", "phone", "website"]
        logger.warning(f"Table not found, using default columns: {enrichment_columns}")
    
    return GatekeeperConfig(
        purpose=purpose,
        enrichment_columns=enrichment_columns
    )


def create_config(table_path: str, purpose: str, output_yaml: Optional[str] = None) -> str:
    """Create a YAML configuration file for a table"""
    
    logger = get_logger('main')
    
    config = infer_config_from_table(table_path, purpose)
    
    yaml_file = output_yaml or f"{Path(table_path).stem}_config.yaml"
    config.to_yaml(yaml_file)
    
    logger.info(f"Configuration saved: {yaml_file} ({len(config.get_enrichment_column_names())} columns)")
    
    return yaml_file


def show_usage():
    """Display usage information"""
    logger = get_logger('main')
    
    logger.info("Usage:")
    logger.info("  python -m intabular <yaml_config> <csv_file>     # Ingest CSV")
    logger.info("  python -m intabular config <table> <purpose>     # Create config")
    logger.info("  python -m intabular analyze <csv_file>           # Analyze CSV")


def handle_config_command(args):
    """Handle the config creation command"""
    logger = get_logger('main')
    
    if len(args) < 4:
        logger.error("Usage: python -m intabular config <table_path> <purpose>")
        return
    
    table_path = args[2]
    purpose = args[3]
    
    create_config(table_path, purpose)


def handle_ingestion_command(args):
    """Handle the main ingestion command"""
    logger = get_logger('main')
    
    if len(args) < 3:
        logger.error("Usage: python -m intabular <yaml_config> <csv_file>")
        return
    
    yaml_config = args[1]
    csv_file = args[2]
    output_file = args[3] if len(args) > 3 else None
    
    try:
        result = run_ingestion_pipeline(yaml_config, csv_file, output_file)
        logger.info(f"Successfully ingested {len(result)} rows")
        
    except Exception as e:
        logger.error(f"Error: {e}")


def main():
    """Main entry point with enhanced logging"""
    
    # Set up logging first
    log_file = os.getenv('INTABULAR_LOG_FILE', 'logs/intabular.log')
    log_level = os.getenv('INTABULAR_LOG_LEVEL', 'INFO')
    json_format = os.getenv('INTABULAR_LOG_JSON', 'false').lower() == 'true'
    
    setup_logging(
        level=log_level,
        log_file=log_file,
        console_output=True,
        json_format=json_format
    )
    
    logger = get_logger('main')
    
    args = sys.argv
    
    if len(args) < 2:
        show_usage()
        return
    
    # Route commands
    if args[1] == "config":
        handle_config_command(args)
    else:
        # Default: ingestion
        handle_ingestion_command(args)


if __name__ == "__main__":
    main() 