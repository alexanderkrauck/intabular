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

# Add current directory to path for imports
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

from core.config import TableConfig, ColumnPolicy
from core.analyzer import CSVAnalyzer
from core.strategy import IngestionStrategy
from core.processor import IntelligentProcessor
from core.logging_config import setup_logging, get_logger


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


def run_ingestion_pipeline(yaml_config_file: str, csv_to_ingest: str, 
                          output_file: Optional[str] = None) -> pd.DataFrame:
    """Run the complete intelligent CSV ingestion pipeline"""
    
    logger = get_logger('main')
    
    logger.info("🚀 Starting intelligent CSV ingestion pipeline")
    logger.info(f"📋 Config: {yaml_config_file}")
    logger.info(f"📄 Input CSV: {csv_to_ingest}")
    
    # Initialize components
    client = setup_openai_client()
    analyzer = CSVAnalyzer(client)
    strategy_creator = IngestionStrategy(client)
    processor = IntelligentProcessor(client)
    
    # Load target configuration
    logger.info("📝 Loading target schema configuration...")
    target_config = TableConfig.from_yaml(yaml_config_file)
    
    logger.info(f"🎯 Purpose: {target_config.purpose[:80]}...")
    logger.info(f"📊 Target columns: {len(target_config.get_enrichment_column_names())}")
    logger.info(f"📜 Policy: {target_config.column_policy}")
    
    # Determine output file
    if not output_file:
        config_data = yaml.safe_load(open(yaml_config_file))
        output_file = config_data.get('target_table', 'enriched_contacts.csv')
        logger.warning(f"⚠️  No target specified in YAML, using default: {output_file}")
    
    logger.info(f"📂 Target file: {output_file}")
    
    # Create target table structure if it doesn't exist
    target_table_path = Path(output_file)
    if not target_table_path.exists():
        target_columns = target_config.get_enrichment_column_names()
        empty_df = pd.DataFrame(columns=target_columns)
        empty_df.to_csv(target_table_path, index=False)
        
        logger.info(f"📋 Created target table structure with {len(target_columns)} columns")
    
    # Analyze unknown CSV
    logger.info("🔍 Analyzing unknown CSV structure...")
    unknown_analysis = analyzer.analyze_csv_structure(csv_to_ingest)
    
    logger.info("✅ CSV Analysis Complete:")
    logger.info(f"📊 Purpose: {unknown_analysis.get('table_purpose', 'Unknown')}")
    logger.info(f"📦 Source: {unknown_analysis.get('data_source', 'Unknown')}")
    logger.info(f"📋 Input columns: {unknown_analysis.get('column_count', 0)}")
    logger.info(f"📈 Input rows: {unknown_analysis.get('row_count', 0)}")
    
    # Create intelligent strategy
    logger.info("🧠 Creating intelligent field-mapping strategy...")
    strategy = strategy_creator.create_ingestion_strategy(target_config, unknown_analysis)
    
    # Log strategy results
    strategy_confidence = strategy.get('confidence_score', 0)
    field_count = len(strategy.get('field_strategies', {}))
    unmapped_count = len(strategy.get('unmapped_source_columns', {}))
    
    logger.info("✅ Strategy Created:")
    logger.info(f"🎯 Overall confidence: {strategy_confidence:.2f}")
    logger.info(f"🗺️  Target fields mapped: {field_count}")
    logger.info(f"⚠️  Source columns unmapped: {unmapped_count}")
    
    # Execute ingestion
    logger.info("🔀 Executing intelligent field-by-field ingestion...")
    ingested_df = processor.execute_ingestion(
        target_table=str(target_table_path),
        source_csv=csv_to_ingest,
        strategy=strategy,
        target_config=target_config
    )
    
    # Save results
    logger.info(f"💾 Saving results to: {output_file}")
    ingested_df.to_csv(output_file, index=False)
    
    # Final summary
    logger.info("\n🎉 Ingestion Pipeline Complete!")
    logger.info(f"📄 Input: {csv_to_ingest} ({unknown_analysis.get('row_count', 0)} rows)")
    logger.info(f"📊 Output: {output_file} ({len(ingested_df)} rows)")
    logger.info(f"🎯 Columns mapped: {field_count}/{len(target_config.get_enrichment_column_names())}")
    logger.info(f"✨ Strategy confidence: {strategy_confidence:.2f}")
    
    return ingested_df


def infer_config_from_table(table_path: str, purpose: str) -> TableConfig:
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
    
    return TableConfig(
        purpose=purpose,
        enrichment_columns=enrichment_columns,
        column_policy=ColumnPolicy.ENRICHMENT_FOCUSED
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
    logger.info(f"📜 Policy: {config.column_policy}")
    
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
    column_policy = args[4] if len(args) > 4 else "enrichment_focused"
    
    create_config(table_path, purpose)


def handle_analyze_command(args):
    """Handle the CSV analysis command"""
    logger = get_logger('main')
    
    if len(args) < 3:
        logger.error("Usage: python -m intabular analyze <csv_file>")
        return
    
    csv_file = args[2]
    client = setup_openai_client()
    analyzer = CSVAnalyzer(client)
    
    analysis = analyzer.analyze_csv_structure(csv_file)
    
    logger.info(f"\n📊 Analysis complete for: {csv_file}")
    logger.info(f"Business Purpose: {analysis.get('table_purpose', 'Unknown')}")
    logger.info(f"Data Source: {analysis.get('data_source', 'Unknown')}")


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
    elif args[1] == "analyze":
        handle_analyze_command(args)
    else:
        # Default: ingestion
        handle_ingestion_command(args)


if __name__ == "__main__":
    main() 