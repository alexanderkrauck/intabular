"""
Configuration classes for table schema and ingestion policies.
"""

import yaml
from pathlib import Path
from enum import Enum
from typing import Union, List, Dict, Any
from .logging_config import get_logger


class ColumnPolicy(Enum):
    """Policies for handling columns during ingestion"""
    STRICT = "strict"               # Only allow predefined columns
    ENRICHMENT_FOCUSED = "enrichment_focused"  # Focus on enrichment columns
    BALANCED = "balanced"           # Balance between existing and new data


class TableConfig:
    """Configuration for target table schema and policies"""
    
    def __init__(self, purpose: str, enrichment_columns: Union[List[str], Dict[str, str]], 
                 column_policy: Union[str, ColumnPolicy] = ColumnPolicy.BALANCED,
                 target: str = None):
        """
        Initialize table configuration
        
        Args:
            purpose: Business purpose/description of the table
            enrichment_columns: List of column names or dict of {column: description}
            column_policy: Policy for handling columns during ingestion
            target: Optional target file path
        """
        self.logger = get_logger('config')
        
        self.purpose = purpose
        self.enrichment_columns = enrichment_columns
        self.target = target
        
        # Convert string to enum if needed
        if isinstance(column_policy, str):
            try:
                self.column_policy = ColumnPolicy(column_policy.lower())
            except ValueError:
                self.logger.warning(f"Unknown column policy '{column_policy}', using BALANCED")
                self.column_policy = ColumnPolicy.BALANCED
        else:
            self.column_policy = column_policy
        
        self.logger.debug(f"Created TableConfig with {len(self.get_enrichment_column_names())} columns",
                         extra={
                             'purpose': purpose,
                             'column_count': len(self.get_enrichment_column_names()),
                             'column_policy': self.column_policy.value
                         })
    
    def get_enrichment_column_names(self) -> List[str]:
        """Get list of enrichment column names"""
        if isinstance(self.enrichment_columns, dict):
            return list(self.enrichment_columns.keys())
        return list(self.enrichment_columns)
    
    def get_column_policy_text(self) -> str:
        """Get human-readable column policy description"""
        policy_descriptions = {
            ColumnPolicy.STRICT: "Only predefined columns allowed",
            ColumnPolicy.ENRICHMENT_FOCUSED: "Focus on enrichment columns",
            ColumnPolicy.BALANCED: "Balance existing and new data"
        }
        return policy_descriptions.get(self.column_policy, "Unknown policy")
    
    def to_yaml(self, filename: str):
        """Save configuration to YAML file"""
        
        self.logger.info(f"Saving configuration to {filename}")
        
        config_data = {
            'purpose': self.purpose,
            'enrichment_columns': self.enrichment_columns,
            'column_policy': self.column_policy.value,
        }
        
        if self.target:
            config_data['target_table'] = self.target
        
        try:
            with open(filename, 'w') as f:
                yaml.dump(config_data, f, default_flow_style=False, sort_keys=False)
            
            self.logger.info(f"✅ Configuration saved successfully",
                           extra={
                               'filename': filename,
                               'purpose': self.purpose,
                               'column_count': len(self.get_enrichment_column_names())
                           })
        except Exception as e:
            self.logger.error(f"Failed to save configuration to {filename}: {e}")
            raise
    
    @classmethod
    def from_yaml(cls, filename: str) -> 'TableConfig':
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
                column_policy=data.get('column_policy', 'balanced'),
                target=data.get('target_table')
            )
            
            logger.info(f"✅ Configuration loaded successfully",
                       extra={
                           'filename': filename,
                           'purpose': config.purpose,
                           'column_count': len(config.get_enrichment_column_names()),
                           'column_policy': config.column_policy.value
                       })
            
            return config
            
        except Exception as e:
            logger.error(f"Failed to load configuration from {filename}: {e}")
            raise 