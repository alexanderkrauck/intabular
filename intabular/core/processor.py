"""
Data processing module that executes ingestion strategies.
"""

import pandas as pd
import re
from typing import Dict, List, Any
from pathlib import Path


class DataProcessor:
    """Executes field-by-field data ingestion based on strategies"""
    
    def execute_ingestion(self, target_table: str, source_csv: str, 
                         strategy: Dict[str, Any], target_config) -> pd.DataFrame:
        """Execute field-by-field ingestion based on strategy"""
        
        print(f"🔀 Executing intelligent field-by-field ingestion...")
        
        # Load data
        source_df = pd.read_csv(source_csv)
        if Path(target_table).exists() and Path(target_table).stat().st_size > 0:
            target_df = pd.read_csv(target_table)
        else:
            target_df = pd.DataFrame()
        
        print(f"   📊 Source: {len(source_df)} rows")
        print(f"   📋 Target: {len(target_df)} rows")
        
        # Start with target data or create empty structure
        result_df = target_df.copy() if not target_df.empty else pd.DataFrame()
        
        # Ensure result_df has enough rows to accommodate source data
        self._extend_target_if_needed(result_df, len(source_df))
        
        # Process each field strategy
        field_strategies = strategy.get('field_strategies', {})
        print(f"   🎯 Processing {len(field_strategies)} field strategies...")
        
        for target_col, field_strategy in field_strategies.items():
            strategy_type = field_strategy.get('strategy', 'preserve')
            source_mapping = field_strategy.get('source_mapping', [])
            transformation_rule = field_strategy.get('transformation_rule', '')
            prompt_template = field_strategy.get('prompt_template', '')
            
            print(f"      • {target_col}: {strategy_type}")
            
            try:
                if strategy_type == "replace" and source_mapping:
                    result_df = self._apply_replace_strategy(result_df, source_df, target_col, source_mapping[0])
                elif strategy_type == "derive" and source_mapping:
                    result_df = self._apply_derive_strategy(result_df, source_df, target_col, source_mapping, transformation_rule)
                elif strategy_type == "concat" and source_mapping:
                    result_df = self._apply_concat_strategy(result_df, source_df, target_col, source_mapping)
                elif strategy_type == "transform" and source_mapping:
                    result_df = self._apply_transform_strategy(result_df, source_df, target_col, source_mapping[0], transformation_rule)
                # Note: prompt_merge would require OpenAI client, skipping for modularity
                # elif strategy_type == "prompt_merge":
                #     result_df = self._apply_prompt_merge_strategy(result_df, source_df, target_col, source_mapping, prompt_template)
                # elif strategy_type == "preserve": skip processing
                
            except Exception as e:
                print(f"        ⚠️  Error applying {strategy_type} strategy: {e}")
                continue
        
        # Handle unmapped source columns
        unmapped_columns = strategy.get('unmapped_source_columns', {})
        for source_col, unmapped_info in unmapped_columns.items():
            action = unmapped_info.get('action', 'ignore')
            if action == "store_as_metadata" and source_col in source_df.columns:
                self._extend_target_if_needed(result_df, len(source_df))
                result_df[f"meta_{source_col}"] = source_df[source_col].values[:len(result_df)]
        
        # Apply data quality rules
        quality_rules = strategy.get('data_quality_rules', {})
        if quality_rules:
            print(f"   🧹 Applying data quality rules...")
            result_df = self._apply_quality_rules(result_df, quality_rules)
        
        print(f"✅ Ingestion complete: {len(result_df)} rows processed")
        
        return result_df
    
    def _extend_target_if_needed(self, target_df: pd.DataFrame, required_rows: int):
        """Extend target DataFrame to accommodate source data"""
        current_rows = len(target_df)
        if current_rows < required_rows:
            # Add empty rows to match source data length
            empty_rows = pd.DataFrame(index=range(current_rows, required_rows), columns=target_df.columns)
            return pd.concat([target_df, empty_rows], ignore_index=True)
        return target_df
    
    def _apply_replace_strategy(self, target_df: pd.DataFrame, source_df: pd.DataFrame, 
                               target_col: str, source_col: str) -> pd.DataFrame:
        """Apply replace strategy: direct column replacement"""
        
        if source_col not in source_df.columns:
            print(f"        ⚠️  Source column '{source_col}' not found")
            return target_df
        
        # Ensure target has the column
        if target_col not in target_df.columns:
            target_df[target_col] = None
        
        # Ensure target_df has enough rows
        target_df = self._extend_target_if_needed(target_df, len(source_df))
        
        # Replace target column with source data
        target_df[target_col] = source_df[source_col].values[:len(target_df)]
        
        return target_df
    
    def _apply_derive_strategy(self, target_df: pd.DataFrame, source_df: pd.DataFrame,
                              target_col: str, source_cols: List[str], transformation_rule: str) -> pd.DataFrame:
        """Apply derive strategy: combine multiple source columns"""
        
        # Check if all source columns exist
        missing_cols = [col for col in source_cols if col not in source_df.columns]
        if missing_cols:
            print(f"        ⚠️  Missing source columns: {missing_cols}")
            return target_df
        
        # Ensure target has the column
        if target_col not in target_df.columns:
            target_df[target_col] = None
        
        # Ensure target_df has enough rows
        target_df = self._extend_target_if_needed(target_df, len(source_df))
        
        # Apply transformation based on rule
        if "first_last_name" in transformation_rule and len(source_cols) >= 2:
            # Combine first and last name
            first_name = source_df[source_cols[0]].fillna('')
            last_name = source_df[source_cols[1]].fillna('')
            combined = (first_name + ' ' + last_name).str.strip()
        else:
            # Default: concatenate with space
            combined = source_df[source_cols].fillna('').agg(' '.join, axis=1).str.strip()
        
        target_df[target_col] = combined.values[:len(target_df)]
        
        return target_df
    
    def _apply_concat_strategy(self, target_df: pd.DataFrame, source_df: pd.DataFrame,
                              target_col: str, source_cols: List[str]) -> pd.DataFrame:
        """Apply concat strategy: concatenate multiple source columns"""
        
        # Check if all source columns exist
        missing_cols = [col for col in source_cols if col not in source_df.columns]
        if missing_cols:
            print(f"        ⚠️  Missing source columns: {missing_cols}")
            return target_df
        
        # Combine source columns
        source_combined = source_df[source_cols].fillna('').agg(' '.join, axis=1).str.strip()
        
        # Ensure target_df has enough rows
        target_df = self._extend_target_if_needed(target_df, len(source_df))
        
        if target_col in target_df.columns and not target_df[target_col].isna().all():
            # Combine with existing data
            existing = target_df[target_col].fillna('').astype(str)
            target_df[target_col] = (existing + ' | ' + source_combined.astype(str)).str.strip(' |')
        else:
            # New column or empty existing column
            target_df[target_col] = source_combined.values[:len(target_df)]
        
        return target_df
    
    def _apply_transform_strategy(self, target_df: pd.DataFrame, source_df: pd.DataFrame,
                                 target_col: str, source_col: str, transformation_rule: str) -> pd.DataFrame:
        """Apply transform strategy: apply specific transformations"""
        
        if source_col not in source_df.columns:
            print(f"        ⚠️  Source column '{source_col}' not found")
            return target_df
        
        # Ensure target has the column
        if target_col not in target_df.columns:
            target_df[target_col] = None
        
        # Ensure target_df has enough rows
        target_df = self._extend_target_if_needed(target_df, len(source_df))
        
        # Get source data
        source_data = source_df[source_col]
        
        # Apply transformation based on rule
        if "lowercase" in transformation_rule:
            transformed = source_data.astype(str).str.lower()
        elif "standardize_email" in transformation_rule:
            transformed = source_data.astype(str).str.strip().str.lower()
        elif "format_phone" in transformation_rule:
            # Remove non-digit characters from phone numbers
            transformed = source_data.astype(str).str.replace(r'[^\d]', '', regex=True)
        elif "clean_company_name" in transformation_rule:
            # Clean company names
            transformed = source_data.astype(str).str.strip().str.title()
        else:
            # Default: just clean whitespace
            transformed = source_data.astype(str).str.strip()
        
        target_df[target_col] = transformed.values[:len(target_df)]
        
        return target_df
    
    def _apply_quality_rules(self, df: pd.DataFrame, quality_rules: Dict) -> pd.DataFrame:
        """Apply data quality and cleanup rules"""
        
        validation = quality_rules.get('validation', {})
        cleanup = quality_rules.get('cleanup', {})
        
        # Email validation
        if validation.get('email_validation'):
            for col in df.columns:
                if 'email' in col.lower() and col in df.columns:
                    # Basic email validation
                    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
                    df[col] = df[col].apply(lambda x: x if pd.isna(x) or re.match(email_pattern, str(x)) else None)
        
        # Phone formatting
        if validation.get('phone_formatting'):
            for col in df.columns:
                if 'phone' in col.lower() and col in df.columns:
                    # Clean phone numbers
                    df[col] = df[col].astype(str).str.replace(r'[^\d]', '', regex=True)
                    # Add formatting for US numbers
                    df[col] = df[col].apply(lambda x: f"({x[:3]}) {x[3:6]}-{x[6:]}" if len(str(x)) == 10 else x)
        
        # Name standardization
        if validation.get('name_standardization'):
            for col in df.columns:
                if 'name' in col.lower() and col in df.columns:
                    # Capitalize names properly
                    df[col] = df[col].astype(str).str.title().str.strip()
        
        # Whitespace cleanup
        if cleanup.get('trim_whitespace'):
            # Trim whitespace from all text columns
            text_columns = df.select_dtypes(include=['object']).columns
            for col in text_columns:
                df[col] = df[col].astype(str).str.strip()
        
        # Handle nulls
        if cleanup.get('handle_nulls') == 'replace_with_empty':
            df = df.fillna('')
        
        return df 