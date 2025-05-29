"""
Configuration management for InTabular table schemas.
"""

import yaml
from dataclasses import dataclass
from typing import Dict, List, Optional, Union


@dataclass
class TableConfig:
    """Configuration for a table's semantic purpose and merge behavior"""
    purpose: str
    enrichment_columns: Union[List[str], Dict[str, str]]  # Support both formats
    column_policy: str  # Free-form text describing data retention policy
    target: Optional[str] = None  # Target file path where data should be saved
    
    @classmethod
    def from_yaml(cls, yaml_path: str) -> 'TableConfig':
        """Load configuration from YAML file"""
        with open(yaml_path, 'r') as f:
            data = yaml.safe_load(f)
        
        # Handle enrichment_columns - support both list and dict formats
        enrichment_columns = data.get('enrichment_columns', [])
        
        # Handle column_policy as free-form text
        column_policy = data.get('column_policy', 'Balance between completeness and conciseness')
        
        # Get target file path
        target = data.get('target', None)
        
        return cls(
            purpose=data['purpose'],
            enrichment_columns=enrichment_columns,
            column_policy=column_policy,
            target=target
        )
    
    def to_yaml(self, yaml_path: str):
        """Save configuration to YAML file"""
        data = {
            'purpose': self.purpose,
            'enrichment_columns': self.enrichment_columns,
            'column_policy': self.column_policy
        }
        
        if self.target:
            data['target'] = self.target
        
        with open(yaml_path, 'w') as f:
            yaml.dump(data, f, default_flow_style=False)
    
    def get_enrichment_column_names(self) -> List[str]:
        """Get list of enrichment column names regardless of format"""
        if isinstance(self.enrichment_columns, dict):
            return list(self.enrichment_columns.keys())
        return self.enrichment_columns
    
    def get_enrichment_column_description(self, column_name: str) -> str:
        """Get description for an enrichment column"""
        if isinstance(self.enrichment_columns, dict):
            return self.enrichment_columns.get(column_name, f"Enrichment field: {column_name}")
        return f"Enrichment field: {column_name}"
    
    def get_column_policy_text(self) -> str:
        """Get column policy as descriptive text"""
        return str(self.column_policy) 