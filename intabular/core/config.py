"""
Configuration classes for gatekeeper schema and policies.
"""

import yaml
from pathlib import Path
from typing import Union, List, Dict, Any
from .logging_config import get_logger


class GatekeeperConfig:
    """Configuration for gatekeeper function g_w(A, D, I) → D' for csv/tables"""
    
    def __init__(self, purpose: str, enrichment_columns: Union[List[str], Dict[str, str]], 
                 target_file_path: str = None, sample_rows: int = 5):
        """
        Initialize gatekeeper configuration
        
        Args:
            purpose: Business purpose/description of the table (the 'I' in g_w)
            enrichment_columns: List of column names or dict of {column: description}
            target_file_path: Optional target file path
            sample_rows: Number of sample rows to analyze for column classification
        """
        self.logger = get_logger('config')
        
        self.purpose = purpose
        self.enrichment_columns = enrichment_columns
        self.target_file_path = target_file_path
        self.sample_rows = sample_rows
        
        self.logger.debug(f"Created GatekeeperConfig with {len(self.get_enrichment_column_names())} columns",
                         extra={
                             'purpose': purpose,
                             'column_count': len(self.get_enrichment_column_names()),
                             'sample_rows': sample_rows
                         })
    
    def get_enrichment_column_names(self) -> List[str]:
        """Get list of enrichment column names"""
        if isinstance(self.enrichment_columns, dict):
            return list(self.enrichment_columns.keys())
        return list(self.enrichment_columns)
    
    def to_yaml(self, filename: str):
        """Save configuration to YAML file"""
        
        self.logger.info(f"Saving configuration to {filename}")
        
        config_data = {
            'purpose': self.purpose,
            'enrichment_columns': self.enrichment_columns,
            'sample_rows': self.sample_rows,
            'target_file_path': self.target_file_path
        }
        
        try:
            with open(filename, 'w') as f:
                yaml.dump(config_data, f, default_flow_style=False, sort_keys=False)
            
            self.logger.info(f"✅ Configuration saved successfully",
                           extra={
                               'filename': filename,
                               'purpose': self.purpose,
                               'column_count': len(self.get_enrichment_column_names()),
                               'sample_rows': self.sample_rows
                           })
        except Exception as e:
            self.logger.error(f"Failed to save configuration to {filename}: {e}")
            raise
    
    @classmethod
    def from_yaml(cls, filename: str) -> 'GatekeeperConfig':
        """Load configuration from YAML file"""
        
        logger = get_logger('config')
        logger.info(f"Loading configuration from {filename}")
        
        if not Path(filename).exists():
            logger.error(f"Configuration file not found: {filename}")
            raise FileNotFoundError(f"Configuration file not found: {filename}")
        
        try:
            with open(filename, 'r') as f:
                data = yaml.safe_load(f)
            
            config = cls(
                purpose=data['purpose'],
                enrichment_columns=data['enrichment_columns'],
                target_file_path=data.get('target_file_path'),
                sample_rows=data.get('sample_rows', 5)  # Default to 5 if not specified
            )
            
            logger.info(f"✅ Configuration loaded successfully",
                       extra={
                           'config_file': filename,
                           'purpose': config.purpose,
                           'column_count': len(config.get_enrichment_column_names()),
                           'sample_rows': config.sample_rows
                       })
            
            return config
            
        except Exception as e:
            logger.error(f"Failed to load configuration from {filename}: {e}")
            raise 