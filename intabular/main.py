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


def setup_openai_client() -> OpenAI:
    """Initialize OpenAI client with API key validation"""
    logger = get_logger('main')
    
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        logger.critical("OPENAI_API_KEY environment variable not set")
        raise ValueError("OPENAI_API_KEY environment variable is required")
    
    client = OpenAI(api_key=api_key)
    
    # Test the connection
    try:
        client.models.list()
        logger.info("✅ OpenAI API connection verified")
    except Exception as e:
        logger.error(f"❌ OpenAI API connection failed: {e}")
        raise
    
    logger.info("✅ Using OpenAI LLM for intelligent data ingestion")
    return client


def run_ingestion_pipeline(yaml_config_file: str, csv_to_ingest: str) -> pd.DataFrame:
    """Run the complete intelligent CSV ingestion pipeline"""
    
    logger = get_logger('main')
    
    logger.info("🚀 Starting intelligent CSV ingestion pipeline")
    logger.info(f"📋 Config: {yaml_config_file}")
    logger.info(f"📄 Input CSV: {csv_to_ingest}")
    
    # Initialize components
    client = setup_openai_client()
        # Load target configuration
    logger.info("📝 Loading target schema configuration...")
    target_config = GatekeeperConfig.from_yaml(yaml_config_file)
    
    
    analyzer = DataframeAnalyzer(client, target_config)
    strategy_creator = DataframeIngestionStrategy(client)
    processor = DataframeIngestionProcessor(client)
    

    
    logger.info(f"🎯 Purpose: {target_config.purpose[:80]}...")
    logger.info(f"📊 Target columns: {len(target_config.get_enrichment_column_names())}")
        

    # Read the CSV to ingest and the target table
    df_to_ingest = pd.read_csv(csv_to_ingest)#TODO This already assumes we have a header
    df_to_enrich = pd.read_csv(target_config.target_file_path) if Path(target_config.target_file_path).exists() else pd.DataFrame()
    
    # Analyze the CSV
    logger.info("📊 Analyzing CSV...")
    df_analysis = analyzer.analyze_dataframe_structure(df_to_ingest)
    
    
    # Create intelligent strategy
    logger.info("🧠 Creating intelligent field-mapping strategy...")
    strategy = strategy_creator.create_ingestion_strategy(target_config, df_analysis)
    
    # Execute ingestion
    logger.info("🔀 Executing intelligent field-by-field ingestion...")
    ingested_df = processor.execute_ingestion(
    df_to_ingest,
    df_to_enrich,
    strategy,
    target_config,
    df_analysis.general_ingestion_analysis
    )
    
    # Save results
    logger.info(f"💾 Saving results to: {target_config.target_file_path}")
    ingested_df.to_csv(target_config.target_file_path, index=False)
    
    # Final summary
    logger.info("\n🎉 Ingestion Pipeline Complete!")
    
    return ingested_df


def infer_config_from_table(table_path: str, purpose: str) -> GatekeeperConfig:
    """Infer configuration from an existing table structure"""
    
    logger = get_logger('main')
    
    if Path(table_path).exists():
        logger.info(f"📋 Inferring schema from existing table: {table_path}")
        df = pd.read_csv(table_path)
        enrichment_columns = list(df.columns)
        logger.info(f"📊 Found {len(enrichment_columns)} columns")
    else:
        # Default enrichment columns
        enrichment_columns = ["email", "first_name", "last_name", "company", "title", "phone", "website"]
        logger.warning(f"⚠️  Table not found, using default columns: {enrichment_columns}")
    
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
    
    logger.info(f"✅ Configuration saved to: {yaml_file}")
    logger.info(f"🎯 Purpose: {purpose}")
    logger.info(f"📊 Columns: {len(config.get_enrichment_column_names())}")
    
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
        logger.info(f"\n🎉 Successfully ingested {len(result)} rows!")
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")


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