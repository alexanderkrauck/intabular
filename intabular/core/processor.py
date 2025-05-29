"""
Intelligent CSV ingestion processor with field-by-field strategy execution.
"""

import json
import time
import pandas as pd
from typing import Dict, Any, List
from pathlib import Path
from openai import OpenAI
from .logging_config import get_logger, log_prompt_response, log_field_processing, log_data_quality


class IntelligentProcessor:
    """Executes field-by-field ingestion strategies with data quality controls"""
    
    def __init__(self, openai_client: OpenAI):
        self.client = openai_client
        self.logger = get_logger('processor')
    
    def execute_ingestion(self, target_table: str, source_csv: str, 
                         strategy: Dict[str, Any], target_config) -> pd.DataFrame:
        """Execute field-by-field ingestion based on strategy"""
        
        self.logger.info("🔀 Executing intelligent field-by-field ingestion...")
        
        # Load source data
        source_df = pd.read_csv(source_csv)
        
        # Load or create target data
        if Path(target_table).exists():
            target_df = pd.read_csv(target_table)
        else:
            # Create empty target structure
            target_columns = target_config.get_enrichment_column_names()
            target_df = pd.DataFrame(columns=target_columns)
        
        # Log initial data state
        self.logger.info(f"📊 Source: {len(source_df)} rows, Target: {len(target_df)} rows",
                        extra={
                            'source_rows': len(source_df),
                            'target_rows': len(target_df),
                            'source_columns': list(source_df.columns),
                            'target_columns': list(target_df.columns)
                        })
        
        # Start with target data or create structure
        if len(target_df) == 0:
            # Create rows for source data
            target_columns = target_config.get_enrichment_column_names()
            result_df = pd.DataFrame(index=range(len(source_df)), columns=target_columns)
            result_df = result_df.fillna('')
        else:
            result_df = target_df.copy()
        
        initial_count = len(result_df)
        
        # Process each field strategy
        field_strategies = strategy.get('field_strategies', {})
        self.logger.info(f"🎯 Processing {len(field_strategies)} field strategies...",
                        extra={'strategy_count': len(field_strategies)})
        
        for target_col, field_strategy in field_strategies.items():
            strategy_type = field_strategy.get('strategy', 'preserve')
            
            self.logger.info(f"• {target_col}: {strategy_type}")
            
            try:
                result_df = self._apply_field_strategy(
                    result_df, source_df, target_col, field_strategy
                )
                
                log_field_processing(
                    self.logger, target_col, strategy_type, True
                )
                
            except Exception as e:
                self.logger.error(f"⚠️  Error applying {strategy_type} strategy to {target_col}: {e}",
                                extra={
                                    'target_column': target_col,
                                    'strategy': strategy_type,
                                    'error': str(e)
                                })
                log_field_processing(
                    self.logger, target_col, strategy_type, False, str(e)
                )
        
        # Apply data quality rules
        quality_rules = strategy.get('data_quality_rules', {})
        if quality_rules:
            self.logger.info("🧹 Applying data quality rules...")
            result_df = self._apply_data_quality_rules(result_df, quality_rules)
        
        final_count = len(result_df)
        self.logger.info(f"✅ Ingestion complete: {final_count} rows processed",
                        extra={
                            'initial_rows': initial_count,
                            'final_rows': final_count,
                            'rows_changed': final_count - initial_count
                        })
        
        return result_df
    
    def _apply_field_strategy(self, target_df: pd.DataFrame, source_df: pd.DataFrame,
                            target_col: str, strategy: Dict[str, Any]) -> pd.DataFrame:
        """Apply specific strategy to a target field"""
        
        strategy_type = strategy.get('strategy', 'preserve')
        source_mapping = strategy.get('source_mapping', [])
        
        self.logger.debug(f"Applying {strategy_type} strategy to {target_col}",
                         extra={
                             'target_column': target_col,
                             'strategy': strategy_type,
                             'source_mapping': source_mapping
                         })
        
        if strategy_type == 'replace':
            return self._apply_replace_strategy(target_df, source_df, target_col, source_mapping)
        elif strategy_type == 'concat':
            return self._apply_concat_strategy(target_df, source_df, target_col, source_mapping, strategy)
        elif strategy_type == 'prompt_merge':
            return self._apply_prompt_merge_strategy(target_df, source_df, target_col, source_mapping, strategy)
        elif strategy_type == 'derive':
            return self._apply_derive_strategy(target_df, source_df, target_col, source_mapping, strategy)
        elif strategy_type == 'transform':
            return self._apply_transform_strategy(target_df, source_df, target_col, source_mapping, strategy)
        elif strategy_type == 'preserve':
            self.logger.debug(f"Preserving existing data for {target_col}")
            return target_df
        else:
            self.logger.warning(f"Unknown strategy: {strategy_type}, preserving existing data")
            return target_df
    
    def _apply_replace_strategy(self, target_df: pd.DataFrame, source_df: pd.DataFrame,
                              target_col: str, source_mapping: List[str]) -> pd.DataFrame:
        """Direct replacement of target column with source column"""
        
        if not source_mapping:
            self.logger.warning(f"No source mapping provided for replace strategy on {target_col}")
            return target_df
            
        source_col = source_mapping[0]
        
        if source_col not in source_df.columns:
            self.logger.warning(f"Source column '{source_col}' not found for {target_col}")
            return target_df
        
        # Simple replacement - copy source data to target
        min_rows = min(len(target_df), len(source_df))
        target_df.loc[:min_rows-1, target_col] = source_df[source_col].iloc[:min_rows].values
        
        self.logger.debug(f"Replaced {target_col} with {source_col} ({min_rows} rows)")
        return target_df
    
    def _apply_concat_strategy(self, target_df: pd.DataFrame, source_df: pd.DataFrame,
                             target_col: str, source_mapping: List[str], strategy: Dict[str, Any]) -> pd.DataFrame:
        """Concatenate multiple source columns with separator"""
        
        if not source_mapping:
            self.logger.warning(f"No source mapping provided for concat strategy on {target_col}")
            return target_df
            
        missing_cols = [col for col in source_mapping if col not in source_df.columns]
        if missing_cols:
            self.logger.warning(f"Missing source columns for {target_col}: {missing_cols}")
            return target_df
        
        # Extract separator from transformation rule
        transformation_rule = strategy.get('transformation_rule', '')
        separator = ', ' if 'comma' in transformation_rule.lower() else ' '
        
        # Concatenate columns
        min_rows = min(len(target_df), len(source_df))
        combined_data = []
        
        for idx in range(min_rows):
            values = []
            for col in source_mapping:
                val = source_df[col].iloc[idx]
                if pd.notna(val) and str(val).strip():
                    values.append(str(val).strip())
            combined_data.append(separator.join(values) if values else '')
        
        target_df.loc[:min_rows-1, target_col] = combined_data
        
        self.logger.debug(f"Concatenated {source_mapping} to {target_col} with separator '{separator}' ({min_rows} rows)")
        return target_df
    
    def _apply_prompt_merge_strategy(self, target_df: pd.DataFrame, source_df: pd.DataFrame,
                                   target_col: str, source_mapping: List[str], strategy: Dict[str, Any]) -> pd.DataFrame:
        """Use LLM to intelligently merge source data"""
        
        if not source_mapping:
            self.logger.warning(f"No source mapping provided for prompt_merge strategy on {target_col}")
            return target_df
            
        missing_cols = [col for col in source_mapping if col not in source_df.columns]
        if missing_cols:
            self.logger.warning(f"Missing source columns for {target_col}: {missing_cols}")
            return target_df
        
        prompt_template = strategy.get('prompt_template', '')
        if not prompt_template:
            prompt_template = f"Intelligently combine the following data into a meaningful {target_col}: {{data}}"
        
        min_rows = min(len(target_df), len(source_df))
        merged_data = []
        processing_count = 0
        
        for idx in range(min_rows):
            # Gather source data for this row
            row_data = {}
            for col in source_mapping:
                val = source_df[col].iloc[idx]
                if pd.notna(val) and str(val).strip():
                    row_data[col] = str(val).strip()
            
            if not row_data:
                merged_data.append('')
                continue
            
            # Use LLM to merge data
            try:
                data_context = ', '.join([f"{k}: {v}" for k, v in row_data.items()])
                prompt = prompt_template.replace('{data}', data_context)
                
                start_time = time.time()
                
                response = self.client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.1,
                    max_tokens=100
                )
                
                duration = time.time() - start_time
                response_content = response.choices[0].message.content.strip()
                
                log_prompt_response(
                    self.logger, prompt, response_content,
                    model="gpt-4o-mini", duration=duration
                )
                
                merged_data.append(response_content)
                processing_count += 1
                
            except Exception as e:
                self.logger.error(f"LLM merge failed for {target_col} row {idx}: {e}")
                merged_data.append(data_context)  # Fallback
        
        target_df.loc[:min_rows-1, target_col] = merged_data
        
        self.logger.info(f"Completed prompt_merge for {target_col}: {processing_count} LLM calls",
                        extra={
                            'target_column': target_col,
                            'llm_calls': processing_count,
                            'source_columns': source_mapping
                        })
        
        return target_df
    
    def _apply_derive_strategy(self, target_df: pd.DataFrame, source_df: pd.DataFrame,
                             target_col: str, source_mapping: List[str], strategy: Dict[str, Any]) -> pd.DataFrame:
        """Apply rule-based derivation to create new data"""
        
        transformation_rule = strategy.get('transformation_rule', '')
        
        if not transformation_rule:
            self.logger.warning(f"No transformation rule provided for derive strategy on {target_col}")
            return target_df
        
        min_rows = min(len(target_df), len(source_df))
        derived_data = []
        
        for idx in range(min_rows):
            try:
                if 'email_domain' in transformation_rule.lower():
                    # Extract domain from email
                    email_col = source_mapping[0] if source_mapping else None
                    if email_col and email_col in source_df.columns:
                        email = source_df[email_col].iloc[idx]
                        if pd.notna(email) and '@' in str(email):
                            domain = str(email).split('@')[-1]
                            derived_data.append(domain)
                        else:
                            derived_data.append('')
                    else:
                        derived_data.append('')
                else:
                    # Generic derivation
                    derived_data.append(f"Derived from {source_mapping}")
                    
            except Exception as e:
                self.logger.error(f"Derivation failed for {target_col} row {idx}: {e}")
                derived_data.append('')
        
        target_df.loc[:min_rows-1, target_col] = derived_data
        
        self.logger.debug(f"Applied derive strategy to {target_col} using rule: {transformation_rule} ({min_rows} rows)")
        return target_df
    
    def _apply_transform_strategy(self, target_df: pd.DataFrame, source_df: pd.DataFrame,
                                target_col: str, source_mapping: List[str], strategy: Dict[str, Any]) -> pd.DataFrame:
        """Apply transformation to source data before copying"""
        
        if not source_mapping:
            self.logger.warning(f"No source mapping provided for transform strategy on {target_col}")
            return target_df
            
        source_col = source_mapping[0]
        if source_col not in source_df.columns:
            self.logger.warning(f"Source column '{source_col}' not found for {target_col}")
            return target_df
        
        transformation_rule = strategy.get('transformation_rule', '')
        min_rows = min(len(target_df), len(source_df))
        transformed_data = []
        
        for idx in range(min_rows):
            val = source_df[source_col].iloc[idx]
            
            if pd.notna(val):
                val_str = str(val).strip()
                
                if 'title_case' in transformation_rule.lower():
                    transformed_data.append(val_str.title())
                elif 'upper' in transformation_rule.lower():
                    transformed_data.append(val_str.upper())
                elif 'lower' in transformation_rule.lower():
                    transformed_data.append(val_str.lower())
                elif 'phone_format' in transformation_rule.lower():
                    digits = ''.join(filter(str.isdigit, val_str))
                    if len(digits) == 10:
                        formatted = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
                        transformed_data.append(formatted)
                    else:
                        transformed_data.append(val_str)
                else:
                    transformed_data.append(val_str)
            else:
                transformed_data.append('')
        
        target_df.loc[:min_rows-1, target_col] = transformed_data
        
        self.logger.debug(f"Applied transform strategy to {target_col}: {transformation_rule} ({min_rows} rows)")
        return target_df
    
    def _apply_data_quality_rules(self, df: pd.DataFrame, quality_rules: Dict[str, Any]) -> pd.DataFrame:
        """Apply data quality rules from strategy"""
        
        result_df = df.copy()
        initial_count = len(result_df)
        
        # Apply cleanup rules
        cleanup_rules = quality_rules.get('cleanup', {})
        if cleanup_rules.get('trim_whitespace', False):
            before_count = len(result_df)
            
            for col in result_df.select_dtypes(include=['object']).columns:
                result_df[col] = result_df[col].astype(str).str.strip()
            
            log_data_quality(
                self.logger, "trim_whitespace", before_count, len(result_df)
            )
        
        # Apply deduplication
        dedup_rules = quality_rules.get('deduplication', {})
        if dedup_rules:
            before_count = len(result_df)
            strategy_type = dedup_rules.get('strategy', 'email_based')
            key_fields = dedup_rules.get('key_fields', [])
            
            if strategy_type == 'email_based' and any('email' in col.lower() for col in result_df.columns):
                email_cols = [col for col in result_df.columns if 'email' in col.lower()]
                if email_cols:
                    result_df = result_df.drop_duplicates(subset=[email_cols[0]], keep='first')
            elif key_fields and all(field in result_df.columns for field in key_fields):
                result_df = result_df.drop_duplicates(subset=key_fields, keep='first')
            
            after_count = len(result_df)
            if after_count != before_count:
                duplicates_removed = before_count - after_count
                log_data_quality(
                    self.logger, f"deduplication_{strategy_type}", 
                    before_count, after_count,
                    {'duplicates_removed': duplicates_removed}
                )
        
        # Apply validation
        validation_rules = quality_rules.get('validation', {})
        if validation_rules.get('email_validation', False):
            email_cols = [col for col in result_df.columns if 'email' in col.lower()]
            for col in email_cols:
                valid_emails = result_df[col].str.contains('@', na=False)
                invalid_count = (~valid_emails).sum()
                if invalid_count > 0:
                    self.logger.warning(f"Found {invalid_count} invalid emails in {col}")
        
        final_count = len(result_df)
        
        if final_count != initial_count:
            self.logger.info(f"Data quality processing: {initial_count} → {final_count} rows",
                           extra={
                               'initial_count': initial_count,
                               'final_count': final_count,
                               'rows_removed': initial_count - final_count
                           })
        
        return result_df 