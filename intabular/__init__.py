"""
InTabular - Intelligent Table Data Ingestion

Automatically maps unknown CSV structures to well-defined target schemas using LLM intelligence.
"""

from .core.config import TableConfig
from .core.analyzer import CSVAnalyzer
from .core.strategy import IngestionStrategy
from .core.processor import DataProcessor
from .main import AdaptiveMerger

__version__ = "0.1.0"
__author__ = "Alexander Krauck"
__email__ = "your-email@example.com"

__all__ = [
    "TableConfig",
    "CSVAnalyzer", 
    "IngestionStrategy",
    "DataProcessor",
    "AdaptiveMerger"
] 