"""
Configuration classes for gatekeeper schema and policies.
"""

import yaml
from pathlib import Path
from typing import Union, List, Dict, Any
from .logging_config import get_logger


class GatekeeperConfig:
    """Configuration for gatekeeper function g_w(A, D, I) → D' for csv/tables"""
    
    def __init__(self, purpose: str, enrichment_columns: Dict[str, Dict[str, str]], 
                 additional_columns: Dict[str, Dict[str, str]] = None,
                 target_file_path: str = None, sample_rows: int = 5):
        """
        Initialize gatekeeper configuration
        
        Args:
            purpose: Business purpose/description of the table (the 'I' in g_w)
            enrichment_columns: Dict of {column: {description, match_type}}
            additional_columns: Dict of {column: {description, match_type}}
            target_file_path: Optional target file path
            sample_rows: Number of sample rows to analyze for column classification
        """
        self.logger = get_logger('config')
        
        self.purpose = purpose
        self.enrichment_columns = enrichment_columns
        self.additional_columns = additional_columns or {}
        self.target_file_path = target_file_path
        self.sample_rows = sample_rows
        
        self.logger.debug(f"Created GatekeeperConfig with {len(self.enrichment_columns)} enrichment columns",
                         extra={
                             'purpose': purpose,
                             'enrichment_column_count': len(self.enrichment_columns),
                             'additional_column_count': len(self.additional_columns),
                             'sample_rows': sample_rows
                         })
    
    def get_enrichment_column_names(self) -> List[str]:
        """Get list of enrichment column names"""
        return list(self.enrichment_columns.keys())
    
    def get_additional_column_names(self) -> List[str]:
        """Get list of additional column names"""
        return list(self.additional_columns.keys())
    
    def get_all_column_names(self) -> List[str]:
        """Get list of all column names"""
        return self.get_enrichment_column_names() + self.get_additional_column_names()
    
    def get_column_description(self, column_name: str) -> str:
        """Get description for a specific column"""
        if column_name in self.enrichment_columns:
            return self.enrichment_columns[column_name].get('description', '')
        elif column_name in self.additional_columns:
            return self.additional_columns[column_name].get('description', '')
        return ''
    
    def get_column_match_type(self, column_name: str) -> str:
        """Get match_type for a specific column"""
        if column_name in self.enrichment_columns:
            return self.enrichment_columns[column_name].get('match_type', 'semantic')
        elif column_name in self.additional_columns:
            return self.additional_columns[column_name].get('match_type', 'semantic')
        return 'semantic'
    
    def to_yaml(self, filename: str):
        """Save configuration to YAML file"""
        
        self.logger.info(f"Saving configuration to {filename}")
        
        config_data = {
            'purpose': self.purpose,
            'enrichment_columns': self.enrichment_columns,
            'sample_rows': self.sample_rows,
            'target_file_path': self.target_file_path
        }
        
        if self.additional_columns:
            config_data['additional_columns'] = self.additional_columns
        
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
                additional_columns=data.get('additional_columns'),
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