"""
Simplified Dataframe analysis for informed column understanding.
"""

import json
import time
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List
from openai import OpenAI

from intabular.core.config import GatekeeperConfig
from .logging_config import get_logger, log_prompt_response
from .utils import parallel_map


class DataframeAnalysis:
    """Container for DataFrame analysis results"""
    
    def __init__(self, general_ingestion_analysis: Dict[str, Any], 
                 dataframe_column_analysis: Dict[str, Any]):
        self.general_ingestion_analysis = general_ingestion_analysis
        self.dataframe_column_analysis = dataframe_column_analysis



class UnclearAssumptionsException(Exception):
    """
    Raised when fundamental assumptions about the data cannot be determined.
    
    Following L_1: "Without any assumption, no learning can occur"
    This exception indicates that the gatekeeper cannot proceed without 
    clearer information about the data structure or intent.
    """
    def __init__(self, message: str, assumption_type: str = "general"):
        self.assumption_type = assumption_type
        super().__init__(f"Unclear assumption ({assumption_type}): {message}")


class DataframeAnalyzer:
    """Analyzes DF columns to understand basic data types for later informed merging"""
    
    def __init__(self, openai_client: OpenAI, gatekeeper_config: GatekeeperConfig):
        self.client = openai_client
        self.sample_rows = gatekeeper_config.sample_rows  # Configurable number of rows to analyze
        self.logger = get_logger('analyzer')
    
    def analyze_dataframe_structure(self, df: pd.DataFrame, additional_info: str = None) -> DataframeAnalysis:
        """Simple analysis of DF structure focusing on column classification.
        
        This does inplace modifications to the dataframe.
        """
        
        self.logger.info(f"📊 Starting simplified Dataframe analysis for DataFrame")
        
        # Set default additional_info if not provided
        if additional_info is None:
            additional_info = "DataFrame provided without additional context"
        
        # Validate basic pandas-level assumptions
        self._validate_basic_structure(df)
        
        # Remove columns that have no non-null values or only empty strings
        empty_cols = []
        for col in df.columns:
            # Check if column is all null OR all empty strings after stripping
            if df[col].isna().all() or (df[col].astype(str).str.strip() == '').all():
                empty_cols.append(col)
        
        if empty_cols:
            self.logger.info(f"🗑️ Removing {len(empty_cols)} empty columns: {empty_cols}")
            df.drop(columns=empty_cols, inplace=True)
        
        # Modify column names to be python style (lowercase with underscores and no special characters)
        df.columns = df.columns.str.replace('[^a-zA-Z0-9]', '_', regex=True).str.lower()
        
        # Analyze Dataframe structure and semantic purpose with LLM
        df_analysis = self._analyze_dataframe_with_llm(df, additional_info)
        
        self.logger.info(f"📈 DF dimensions: {len(df)} rows × {len(df.columns)} columns")
        self.logger.info(f"🔍 Using {self.sample_rows} sample rows for semantic analysis")
        self.logger.info(f"🎯 Semantic purpose: {df_analysis.get('semantic_purpose', 'Unknown')}")
        
        # Analyze individual columns in parallel
        column_results = parallel_map(
            lambda col_name: self._analyze_single_column(df[col_name], col_name),
            df.columns,
            max_workers=5,
            timeout=30
        )
        
        # Convert list results back to dict mapping column names to results
        column_semantics = dict(zip(df.columns, column_results))
        
        # Create general ingestion analysis
        general_analysis = {
            "row_count": len(df),
            "column_count": len(df.columns),
            "table_purpose": df_analysis['semantic_purpose']
        }
        
        
        # Create and return DataframeAnalysis object
        return DataframeAnalysis(
            general_ingestion_analysis=general_analysis,
            dataframe_column_analysis=column_semantics
        )
    
    def _validate_basic_structure(self, df: pd.DataFrame):
        """Validate basic pandas-level assumptions about the DF structure"""
        
        # Check if dataframe is empty
        if df.empty:
            raise UnclearAssumptionsException(
                f"DataFrame is empty - cannot make assumptions about data structure",
                assumption_type="data_presence"
            )
        
        # Check for meaningful column headers
        if len(df.columns) == 0:
            raise UnclearAssumptionsException(
                f"DataFrame has no columns - cannot determine data structure",
                assumption_type="column_headers"
            )
        
        self.logger.debug(f"✅ Basic structure validated for DataFrame")
    
    def _analyze_dataframe_with_llm(self, df: pd.DataFrame, additional_info: str) -> dict:
        """Use LLM to analyze DF structure and semantic purpose by examining first two rows"""
        
        try:
            if len(df) < 1:
                raise UnclearAssumptionsException(
                    f"DataFrame is empty or has no data rows",
                    assumption_type="data_presence"
                )
            
            # Get first two rows from the DataFrame
            header = df.columns.tolist()
            first_row = df.iloc[0].astype(str).tolist() if len(df) >= 1 else []
            
            # Response schema for comprehensive DF analysis
            response_schema = {
                "type": "object",
                "properties": {
                    "has_header": {"type": "boolean"},
                    "semantic_purpose": {"type": "string"},
                    "reasoning": {"type": "string"}
                },
                "required": ["has_header", "semantic_purpose", "reasoning"],
                "additionalProperties": False
            }
            
            prompt = f"""
            Analyze this Dataframe to understand both its structure and semantic purpose:
            
            FIRST ROW/HEADER: {header}
            {f"SECOND ROW: {first_row}" if first_row else "ONLY ONE ROW AVAILABLE"}
            
            ADDITIONAL CONTEXT (for reference only, not definitive): {additional_info}
            Note: The additional context above is supplementary information that may provide hints 
            about the dataframe's purpose, but you should base your analysis primarily on the actual 
            data structure and content patterns you observe.
            
            Please determine:
            
            1. HEADER DETECTION: Does the first row contain column headers or actual data/palceholder values?
               - Headers: descriptive names like ["name", "email", "company", "first_name"]  
               - Data: actual values like ["John Smith", "john@example.com", "Acme Corp"] or placeholder values like ["1", "2", "3"]
            
            2. SEMANTIC PURPOSE: What does this Dataframe represent? Provide a clear, concise description.
               Examples: "Contact list with names and email addresses", "Employee directory with contact information", 
               "Customer database with purchase history", "Survey responses about product satisfaction"
            
            Base your analysis on the column names (if headers exist) and data patterns you observe.
            """
            
            start_time = time.time()
            
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                response_format={
                    "type": "json_schema",
                    "json_schema": {
                        "name": "dataframe_analysis",
                        "schema": response_schema
                    }
                },
                temperature=0.1
            )
            
            duration = time.time() - start_time
            response_content = response.choices[0].message.content
            
            # Log the prompt and response
            log_prompt_response(
                self.logger, prompt, response_content,
                model="gpt-4o-mini", duration=duration
            )
            
            result = json.loads(response_content)
            
            # Check for header assumption violation
            if not result.get('has_header', False):
                raise UnclearAssumptionsException(
                    f"DataFrame appears to have no header row - "
                    f"first row contains data values rather than descriptive column names. "
                    f"Cannot make semantic assumptions without proper column headers. "
                    f"LLM reasoning: {result.get('reasoning', '')}",
                    assumption_type="column_headers"
                )
            
            self.logger.info(f"🔍 DF Analysis for DataFrame:")
            self.logger.info(f"  📋 Has headers: {result.get('has_header', False)}")
            self.logger.info(f"  🎯 Purpose: {result.get('semantic_purpose', 'Unknown')}")
            self.logger.info(f"  💭 Reasoning: {result.get('reasoning', '')}")
            
            return result
            
        except UnclearAssumptionsException:
            # Re-raise UnclearAssumptionsException as-is
            raise
        except Exception as e:
            self.logger.error(f"❌ DF analysis failed for DataFrame: {e}")
            # Fallback: assume it has headers and is unknown type
            return {
                "has_header": True,
                "semantic_purpose": "Unknown data file",
                "reasoning": f"Analysis failed due to error: {e}"
            }
    
    def _analyze_single_column(self, series: pd.Series, col_name: str) -> Dict[str, Any]:
        """Classify column as either 'identifier' or 'text' based on content"""
        
        # Get sample values (non-null, unique, limited by sample_rows)
        sample_values = series.dropna().unique()[:self.sample_rows].tolist()
        
        # Basic statistics - convert to native Python types for JSON serialization
        stats = {
            "total_count": int(len(series)),
            "non_null_count": int(series.count()),
            "unique_count": int(series.nunique()),
            "completeness": float(series.count() / len(series)) if len(series) > 0 else 0.0
        }
        
        # Simple schema - just identifier vs text
        response_schema = {
            "type": "object",
            "properties": {
                "data_type": {
                    "type": "string",
                    "enum": ["identifier", "text"]
                },
                "purpose": {"type": "string"},
                "reasoning": {"type": "string"}
            },
            "required": ["data_type", "purpose", "reasoning"],
            "additionalProperties": False
        }
        
        # Clean sample values to handle multiline content
        cleaned_samples = [
            str(val).replace('\n', ' ').replace('\r', ' ') 
            if isinstance(val, str) else str(val)
            for val in sample_values
        ]

        prompt = f"""
        Classify this Dataframe column as either "identifier" or "text":
        
        COLUMN NAME: {col_name}
        SAMPLE VALUES (newlines removed): {cleaned_samples}
        COLUMN STATISTICS: {stats}
        
        CLASSIFICATION RULES:
        - "identifier": Names, emails, phone numbers, IDs, websites, addresses, companies, titles, categories, dates, numbers
        - "text": Free-form text content like descriptions, notes, comments, explanations, statements, etc.
        
        Data should be classified as "identifier" unless it's clearly free-form text content.
        
        Please provide:
        1. Your classification ("identifier" or "text")
        2. A single sentence explaining what this column represents (purpose)
        3. Brief reasoning for your classification
        
        Example purpose descriptions:
        - "Person's full name for identification"
        - "Email address for contact information"
        - "Company name where person works"
        - "Detailed product description or review text"
        - "Customer feedback comments"
        """
        
        start_time = time.time()
        
        try:
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                response_format={
                    "type": "json_schema",
                    "json_schema": {
                        "name": "column_classification",
                        "schema": response_schema
                    }
                },
                temperature=0.1
            )
            
            duration = time.time() - start_time
            response_content = response.choices[0].message.content
            
            # Log the prompt and response
            log_prompt_response(
                self.logger, prompt, response_content,
                model="gpt-4o-mini", duration=duration
            )
            
            result = json.loads(response_content)
            result.update(stats)  # Add statistics to result
            
            self.logger.info(f"✅ {col_name}: {result.get('data_type', 'unknown')} - {result.get('purpose', 'No purpose provided')}")
            
            return result
            
        except Exception as e:
            self.logger.error(f"❌ {col_name}: Classification failed - {e}")
            raise e # We don't want to fall back to a default analysis so we re-raise the error. This whole thing does not make sense without LLMs.
    

    