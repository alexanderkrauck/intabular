"""
Command Line Interface for InTabular.
Handles CLI commands and user interaction, separate from core business logic.
"""

import sys
import os
from intabular.core.logging_config import setup_logging, get_logger


def show_usage():
    """Display usage information"""
    logger = get_logger('cli')
    
    logger.info("Usage:")
    logger.info("  python -m intabular <yaml_config> <csv_file>     # Ingest CSV")
    logger.info("  python -m intabular config <table> <purpose>     # Create config")


def handle_config_command(args):
    """Handle the config creation command"""
    logger = get_logger('cli')
    
    if len(args) < 4:
        logger.error("Usage: python -m intabular config <table_path> <purpose>")
        return
    
    table_path = args[2]
    purpose = args[3]
    
    from intabular.csv_component import create_config_from_csv
    create_config_from_csv(table_path, purpose)


def handle_ingestion_command(args):
    """Handle the main ingestion command"""
    logger = get_logger('cli')
    
    if len(args) < 3:
        logger.error("Usage: python -m intabular <yaml_config> <csv_file>")
        return
    
    yaml_config = args[1]
    csv_file = args[2]
    
    try:
        from intabular.csv_component import run_csv_ingestion_pipeline
        result = run_csv_ingestion_pipeline(yaml_config, csv_file)
        logger.info(f"Successfully processed {len(result)} rows")
        
    except Exception as e:
        logger.error(f"Error: {e}")


def main():
    """Main CLI entry point with enhanced logging"""
    
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
    
    logger = get_logger('cli')
    
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