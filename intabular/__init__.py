"""
InTabular - Intelligent CSV Data Ingestion

Automatically maps unknown CSV structures to well-defined target schemas using LLM intelligence.
"""

import os
from .core.config import TableConfig, ColumnPolicy
from .core.analyzer import CSVAnalyzer
from .core.strategy import IngestionStrategy
from .core.processor import IntelligentProcessor
from .core.logging_config import setup_logging, get_logger
from .main import run_ingestion_pipeline, create_config, setup_openai_client

# Set up default logging configuration when package is imported
_log_file = os.getenv('INTABULAR_LOG_FILE')
_log_level = os.getenv('INTABULAR_LOG_LEVEL', 'WARNING')  # Less verbose by default
_json_format = os.getenv('INTABULAR_LOG_JSON', 'false').lower() == 'true'

setup_logging(
    level=_log_level,
    log_file=_log_file,
    console_output=False,  # Don't clutter console unless explicitly running CLI
    json_format=_json_format
)

__version__ = "0.2.0"
__author__ = "Alexander Krauck"

__all__ = [
    "TableConfig",
    "ColumnPolicy",
    "CSVAnalyzer", 
    "IngestionStrategy",
    "IntelligentProcessor",
    "run_ingestion_pipeline",
    "create_config",
    "setup_openai_client",
    "setup_logging",
    "get_logger"
] 