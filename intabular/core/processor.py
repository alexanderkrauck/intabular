"""
Simple entity-focused CSV ingestion processor.
"""

import json
import re
import ast
import os
import textwrap
import numpy as np
import pandas as pd
from typing import Dict, Any, Tuple, Optional
from openai import OpenAI

from intabular.core.config import GatekeeperConfig
from .logging_config import get_logger
from .llm_logger import log_llm_call

SAFE_NAMESPACE = {
            're': re,
            'str': str,
            'int': int,
            'float': float,
            'len': len,
            'min': min,
            'max': max,
            'abs': abs,
            'round': round,
            'bool': bool,
            'list': list,
            'dict': dict,
            'set': set,
            'tuple': tuple,
        }


class DataframeIngestionProcessor:
    """Simple entity-focused ingestion processor with integrated transformation capabilities"""
    
    def __init__(self, openai_client: OpenAI):
        self.client = openai_client
        self.logger = get_logger('processor')
    
    def execute_transformation(
        self, 
        transformation_rule: str, 
        source_data: Dict[str, Any], 
        current_value: Any = None
    ) -> Any:
        """
        Execute a transformation rule with provided data
        
        Args:
            transformation_rule: Python expression to execute
            source_data: Dictionary of source column data
            current_value: Current value in target column (optional)
            
        Returns:
            Transformed value
            
        Raises:
            ValueError: If transformation execution fails
        """
        
        if not transformation_rule or transformation_rule.strip() == "":
            return None
            
        # Create execution namespace
        namespace = SAFE_NAMESPACE.copy()
        
        # Add source column data - convert to strings and handle None values
        for col_name, value in source_data.items():
            if value is None or pd.isna(value): #this is a heuristic to handle None values and could be an issue if the string was actually "nan"
                namespace[col_name] = ""
            elif isinstance(value, str):
                namespace[col_name] = value
            else:
                namespace[col_name] = str(value)
        
        # Add current target value
        if current_value is None:
            namespace['current'] = ""
        elif isinstance(current_value, str):
            namespace['current'] = current_value
        else:
            namespace['current'] = str(current_value)
        
        try:
            self.logger.debug(f"Executing transformation: {transformation_rule}")
            self.logger.debug(f"With data: {list(source_data.keys())}")
            
            # Execute the transformation rule
            result = eval(transformation_rule, {"__builtins__": {}}, namespace)
            
            self.logger.debug(f"Transformation result: {result}")
            return result
            
        except Exception as e:
            error_msg = f"Failed to execute transformation '{transformation_rule}': {e}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
    
    def validate_transformation_rule(self, transformation_rule: str) -> bool:
        """
        Validate that a transformation rule is safe to execute
        
        Args:
            transformation_rule: Python expression to validate
            
        Returns:
            True if safe, False otherwise
        """
        
        if not transformation_rule or transformation_rule.strip() == "":
            return False
            
        try:
            # Parse the expression into an AST
            tree = ast.parse(transformation_rule, mode='eval')
            
            # Check for dangerous operations
            for node in ast.walk(tree):
                if isinstance(node, (ast.Import, ast.ImportFrom)):
                    return False
                if isinstance(node, ast.Call):
                    # Check for dangerous function calls
                    if isinstance(node.func, ast.Name):
                        if node.func.id in ['exec', 'eval', 'compile', 'open', '__import__']:
                            return False
                    elif isinstance(node.func, ast.Attribute):
                        # Check for dangerous method calls
                        if node.func.attr in ['write', 'read', 'delete', 'remove']:
                            return False
                            
            return True
            
        except SyntaxError:
            return False
        except Exception:
            return False

    def apply_column_mapping(
        self, 
        mapping_result: Dict[str, Any], 
        source_row: Dict[str, Any], 
        target_column_name: str,
        target_config: GatekeeperConfig,
        general_ingestion_analysis: Dict[str, Any],
        current_value: Any = None
    ) -> Any:
        """
        Apply a column mapping to transform source data
        
        Args:
            mapping_result: Result from strategy creation with transformation_type and transformation_rule
            source_row: Dictionary of source column data for this row
            target_column_name: Name of the target column being filled
            target_config: Configuration for the target dataset
            general_ingestion_analysis: General analysis of the ingestion process
            current_value: Current value in target column (for merging)
            
        Returns:
            Transformed value or None if no mapping
        """
        
        if mapping_result['transformation_type'] == 'none':
            return None
        elif mapping_result['transformation_type'] == 'format':
            return self.execute_transformation(
                mapping_result['transformation_rule'], 
                source_row, 
                current_value
            )
        elif mapping_result['transformation_type'] == 'llm_format':
            # Direct LLM parsing - skip intermediate transformation
            return self._apply_llm_transformation(
                target_column_name,
                target_config,
                general_ingestion_analysis,
                source_row,
                current_value,
                mapping_result
            )
        else:
            raise ValueError(f"Unknown transformation type: {mapping_result['transformation_type']}")

    def _apply_llm_transformation(
        self,
        target_column_name: str,
        target_config: GatekeeperConfig,
        general_ingestion_analysis: Dict[str, Any],
        source_row: Dict[str, Any],
        current_value: Any = None,
        mapping_result: Dict[str, Any] = None
    ) -> Any:
        """
        Apply LLM-based transformation for direct parsing of source columns
        
        Args:
            target_column_name: Name of the target column being filled
            target_config: Configuration for the target dataset
            general_ingestion_analysis: General analysis of the ingestion process
            source_row: Original source row data
            current_value: Current value in target column (for merging)
            mapping_result: Strategy mapping result containing llm_source_columns specification
            
        Returns:
            LLM-processed transformation result
        """
        
        try:
            # Get target column information
            column_info = target_config.get_interpretable_column_information(target_column_name)
            
            # Determine which source columns to include
            if mapping_result and mapping_result.get('llm_source_columns'):
                # Use specified columns only
                source_columns = mapping_result['llm_source_columns']
                filtered_source_data = {col: source_row.get(col, '') for col in source_columns if col in source_row}
            else:
                # Use all source columns
                filtered_source_data = source_row
            
            # Format source data with types for LLM
            source_data_formatted = {}
            for col_name, value in filtered_source_data.items():
                # Include both value and inferred type
                if value is None or pd.isna(value):
                    source_data_formatted[col_name] = {"value": "", "type": "empty"}
                elif isinstance(value, str):
                    source_data_formatted[col_name] = {"value": value.strip(), "type": "text"}
                elif isinstance(value, (int, float)):
                    source_data_formatted[col_name] = {"value": str(value), "type": "number"}
                else:
                    source_data_formatted[col_name] = {"value": str(value), "type": "text"}
            
            # Prepare prompt for direct LLM parsing
            prompt = textwrap.dedent(f"""
                You are parsing source data directly into a target database column. Your task is to analyze all the provided source columns and extract/transform the appropriate value for the target column.
                
                GENERAL PURPOSE OF DATABASE: {target_config.purpose}
                
                TARGET COLUMN INFORMATION:
                {column_info}
                
                GENERAL INGESTION ANALYSIS (context about the source data):
                {json.dumps(general_ingestion_analysis, indent=2)}
                
                SOURCE COLUMNS AND VALUES:
                {json.dumps(source_data_formatted, indent=2)}
                
                CURRENT TARGET VALUE: {current_value if current_value else "None"}
                
                INSTRUCTIONS:
                1. Analyze all the source columns and their values
                2. Determine which source data is relevant for the target column
                3. Extract, combine, or transform the relevant data to fit the target column requirements
                4. Consider the target column type, format, and business purpose
                5. If there's a current value, decide whether to replace it, merge with it, or keep it
                6. Return only the final transformed value that should go into the target column
                
                Return only the transformed value, nothing else. If no relevant data is found, return an empty string.
            """).strip()
            
            llm_kwargs = {
                "model": os.getenv('INTABULAR_PROCESSOR_MODEL', 'gpt-4o-mini'),
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.1,
                "max_tokens": 1000
            }
            
            response = log_llm_call(
                lambda: self.client.chat.completions.create(**llm_kwargs),
                **llm_kwargs
            )
            
            result = response.choices[0].message.content.strip()
            self.logger.debug(f"LLM direct parsing result for {target_column_name}: {result}")
            return result
            
        except Exception as e:
            error_msg = f"Failed LLM direct parsing for {target_column_name}: {e}"
            self.logger.warning(error_msg)
            # Fallback to empty string if LLM fails
            return ""

    def execute_ingestion(self, source_df: pd.DataFrame, target_df: pd.DataFrame, strategy, target_config: GatekeeperConfig, general_ingestion_analysis: Dict[str, Any]) -> pd.DataFrame:
        """Execute entity-focused ingestion with identity-based merging"""
        
        self.logger.info("Executing entity-focused ingestion...")
        
        # Extract mappings from strategy
        no_merge_mappings = strategy.no_merge_column_mappings
        merge_mappings = strategy.merge_column_mappings
                
        # Filter to only mappings that have actual transformations
        entity_mappings = {col: mapping for col, mapping in no_merge_mappings.items() 
                          if mapping.get('transformation_type') != 'none' and col in target_config.entity_columns}
        
        if not entity_mappings:
            raise ValueError("No entity mappings found, but entity columns are required for ingestion for now")
        
        self.logger.info(f"Processing {len(source_df)} source rows against {len(target_df)} target rows")
        self.logger.info(f"Entity columns: {list(entity_mappings.keys())}")
        self.logger.info(f"Merge columns: {list(merge_mappings.keys())}")
        self.logger.info(f"All columns: {list(target_config.entity_columns.keys())}")
        
        # Process each source row: merge or add #TODO: possibly reconsider copying
        target_df = target_df.copy()
        
        merged_count = 0
        added_count = 0
        
        #Note: doing this in parallel might be tricky since there are concurrency issues when writing to the same dataframe
        for idx, source_row in source_df.iterrows():
            source_data = source_row.to_dict()
            
            # Transform entity fields for matching
            #TODO: this should be map_parallel (ed) since it might include LLM calls
            entity_values = self._transform_entity_fields(source_data, entity_mappings, target_config, general_ingestion_analysis)
            
            # Find best match based on entity values
            match_idx, identity_sum = self._find_best_match(entity_values, target_df, target_config)
            
            if match_idx is not None and identity_sum >= 1.0:
                # Merge into existing row
                target_df = self._merge_row(target_df, match_idx, source_data, entity_values, merge_mappings, target_config, general_ingestion_analysis)
                merged_count += 1
            else:
                # Add as new row
                target_df = self._add_row(target_df, source_data, entity_values, merge_mappings, target_config, general_ingestion_analysis)
                added_count += 1
        
        self.logger.info(f"Complete: {merged_count} merged, {added_count} added, {len(target_df)} total rows")
        return target_df
    
    def _transform_entity_fields(self, source_data: Dict[str, Any], entity_mappings: Dict[str, Dict[str, Any]], target_config: GatekeeperConfig, general_ingestion_analysis: Dict[str, Any]) -> Dict[str, str]:
        """Transform source data to entity field values using transformation rules"""
        entity_values = {}
        
        for target_col, mapping in entity_mappings.items():
            try:
                # Apply transformation using the integrated transformation functionality
                transformed_value = self.apply_column_mapping(mapping, source_data, target_col, target_config, general_ingestion_analysis)
                
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
                  entity_values: Dict[str, str], merge_mappings: Dict[str, Dict[str, Any]], target_config: GatekeeperConfig, general_ingestion_analysis: Dict[str, Any]) -> pd.DataFrame:
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
                    merged_value = self.apply_column_mapping(mapping, source_data, target_col, target_config, general_ingestion_analysis, current_value)
                else:
                    merged_value = self.apply_column_mapping(mapping, source_data, target_col, target_config, general_ingestion_analysis)
                    
                if merged_value is not None:
                    target_df.loc[target_idx, target_col] = str(merged_value).strip()
                    
            except Exception as e:
                self.logger.warning(f"Failed to merge descriptive field {target_col}: {e}")
        
        return target_df
    
    def _add_row(self, target_df: pd.DataFrame, source_data: Dict[str, Any],
                entity_values: Dict[str, str], merge_mappings: Dict[str, Dict[str, Any]], target_config: GatekeeperConfig, general_ingestion_analysis: Dict[str, Any]) -> pd.DataFrame:
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
                new_value = self.apply_column_mapping(mapping, source_data, target_col, target_config, general_ingestion_analysis)
                new_row[target_col] = str(new_value).strip() if new_value is not None else ""
            except Exception as e:
                self.logger.warning(f"Failed to add descriptive field {target_col}: {e}")
                new_row[target_col] = ""
        
        # Ensure all target columns exist
        for col in target_df.columns:
            if col not in new_row:
                new_row[col] = ""
        
        return pd.concat([target_df, pd.DataFrame([new_row])], ignore_index=True) 