"""
Simple entity-focused CSV ingestion processor.
"""

import numpy as np
import pandas as pd
from typing import Dict, Any, List, Tuple, Optional
from pathlib import Path
from openai import OpenAI

from intabular.core.config import GatekeeperConfig
from intabular.core.strategy import DataframeIngestionStrategyResult
from intabular.core.transformer import TransformationExecutor, apply_column_mapping
from .logging_config import get_logger


class DataframeIngestionProcessor:
    """Simple entity-focused ingestion processor"""
    
    def __init__(self, openai_client: OpenAI):
        self.client = openai_client
        self.logger = get_logger('processor')
        self.transformer = TransformationExecutor()
    
    def execute_ingestion(self, source_df: pd.DataFrame, target_df: pd.DataFrame, strategy: DataframeIngestionStrategyResult, target_config: GatekeeperConfig) -> pd.DataFrame:
        """Execute entity-focused ingestion with identity-based merging"""
        
        self.logger.info("🔀 Executing entity-focused ingestion...")
        
        # Extract mappings from strategy
        no_merge_mappings = strategy.no_merge_column_mappings
        merge_mappings = strategy.merge_column_mappings
                
        # Filter to only mappings that have actual transformations
        entity_mappings = {col: mapping for col, mapping in no_merge_mappings.items() 
                          if mapping.get('transformation_type') != 'none' and col in target_config.entity_columns}
        
        if not entity_mappings:
            raise ValueError("No entity mappings found, but entity columns are required for ingestion for now")
        
        self.logger.info(f"📊 Processing {len(source_df)} source rows against {len(target_df)} target rows")
        self.logger.info(f"🎯 Entity columns: {list(entity_mappings.keys())}")
        self.logger.info(f"📝 Merge columns: {list(merge_mappings.keys())}")
        
        # Process each source row: merge or add #TODO: possibly reconsider copying
        target_df = target_df.copy()
        
        merged_count = 0
        added_count = 0
        
        #Note: doing this in parallel might be tricky since there are concurrency issues when writing to the same dataframe
        for idx, source_row in source_df.iterrows():
            source_data = source_row.to_dict()
            
            # Transform entity fields for matching
            #TODO: this should be map_parallel (ed) since it might include LLM calls
            entity_values = self._transform_entity_fields(source_data, entity_mappings)
            
            # Find best match based on entity values
            match_idx, identity_sum = self._find_best_match(entity_values, target_df, target_config)
            
            if match_idx is not None and identity_sum >= 1.0:
                # Merge into existing row
                target_df = self._merge_row(target_df, match_idx, source_data, entity_values, merge_mappings)
                merged_count += 1
            else:
                # Add as new row
                target_df = self._add_row(target_df, source_data, entity_values, merge_mappings)
                added_count += 1
        
        self.logger.info(f"✅ Complete: {merged_count} merged, {added_count} added, {len(target_df)} total rows")
        return target_df
    
    def _transform_entity_fields(self, source_data: Dict[str, Any], entity_mappings: Dict[str, Dict[str, Any]]) -> Dict[str, str]:
        """Transform source data to entity field values using transformation rules"""
        entity_values = {}
        
        for target_col, mapping in entity_mappings.items():
            try:
                # Apply transformation using the transformer
                transformed_value = apply_column_mapping(mapping, source_data)
                
                if transformed_value is not None:
                    entity_values[target_col] = str(transformed_value).strip()
                    
            except Exception as e:
                self.logger.warning(f"Failed to transform {target_col}: {e}")
                continue
            
        
        return entity_values
    
    def _find_best_match(self, entity_values: Dict[str, str], target_df: pd.DataFrame, gatekeeper_config: GatekeeperConfig) -> Tuple[Optional[int], Optional[float]]:
        """Find best matching target row and calculate identity indication sum"""
        if len(target_df) == 0:
            return None, None
        
        entity_keys = list(entity_values.keys())
        
        matches = np.zeros((len(target_df), len(entity_keys)))
        
        for idx, key in enumerate(entity_keys):
            current_matches = (target_df[key] == entity_values[key]) * gatekeeper_config.entity_columns[key]['identity_indication']
            matches[:, idx] = current_matches
            
        matches = matches.sum(axis=1)
        
        best_match_idx = matches.argmax()
        best_identity_sum = matches[best_match_idx]
        
        return best_match_idx, best_identity_sum
        
    
    
    def _merge_row(self, target_df: pd.DataFrame, target_idx: int, source_data: Dict[str, Any],
                  entity_values: Dict[str, str], merge_mappings: Dict[str, Dict[str, Any]]) -> pd.DataFrame:
        """Merge source row into existing target row"""
        
        # Update entity fields (only if target is empty, using precomputed values)
        for target_col, new_value in entity_values.items():
            try:
                current_value = str(target_df.loc[target_idx, target_col]).strip()
                if not current_value or current_value == 'nan':
                    target_df.loc[target_idx, target_col] = new_value
            except Exception as e:
                self.logger.warning(f"Failed to merge entity field {target_col}: {e}")
        
        # Update merge fields (intelligent merging with current values)
        for target_col, mapping in merge_mappings.items():
            if mapping.get('transformation_type') == 'none':
                continue
                
            try:
                if target_col in target_df.columns:
                    current_value = target_df.loc[target_idx, target_col]
                    merged_value = apply_column_mapping(mapping, source_data, current_value)
                else:
                    merged_value = apply_column_mapping(mapping, source_data)
                    
                if merged_value is not None:
                    target_df.loc[target_idx, target_col] = str(merged_value).strip()
                    
            except Exception as e:
                self.logger.warning(f"Failed to merge descriptive field {target_col}: {e}")
        
        return target_df
    
    def _add_row(self, target_df: pd.DataFrame, source_data: Dict[str, Any],
                entity_values: Dict[str, str], merge_mappings: Dict[str, Dict[str, Any]]) -> pd.DataFrame:
        """Add source row as new row to target"""
        new_row = {}
        
        # Add entity fields (use precomputed values)
        for target_col, new_value in entity_values.items():
            new_row[target_col] = new_value
        
        # Add merge fields
        for target_col, mapping in merge_mappings.items():
            if mapping.get('transformation_type') == 'none':
                new_row[target_col] = ""
                continue
                
            try:
                new_value = apply_column_mapping(mapping, source_data)
                new_row[target_col] = str(new_value).strip() if new_value is not None else ""
            except Exception as e:
                self.logger.warning(f"Failed to add descriptive field {target_col}: {e}")
                new_row[target_col] = ""
        
        # Ensure all target columns exist
        for col in target_df.columns:
            if col not in new_row:
                new_row[col] = ""
        
        return pd.concat([target_df, pd.DataFrame([new_row])], ignore_index=True) 