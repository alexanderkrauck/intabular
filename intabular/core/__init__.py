"""
Core modules for InTabular data processing.
"""

from .config import TableConfig
from .analyzer import CSVAnalyzer
from .strategy import IngestionStrategy
from .processor import DataProcessor

__all__ = [
    "TableConfig",
    "CSVAnalyzer",
    "IngestionStrategy", 
    "DataProcessor"
] 