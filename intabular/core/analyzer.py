"""
Simplified CSV analysis for informed column understanding.
"""

import json
import time
import pandas as pd
from pathlib import Path
from typing import Dict, Any
from openai import OpenAI

from intabular.core.config import GatekeeperConfig
from .logging_config import get_logger, log_prompt_response
from .utils import parallel_map


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


class CSVAnalyzer:
    """Analyzes CSV columns to understand basic data types for later informed merging"""
    
    def __init__(self, openai_client: OpenAI, gatekeeper_config: GatekeeperConfig):
        self.client = openai_client
        self.sample_rows = gatekeeper_config.sample_rows  # Configurable number of rows to analyze
        self.logger = get_logger('analyzer')
    
    def analyze_csv_structure(self, csv_path: str, additional_info: str = None) -> Dict[str, Any]:
        """Simple analysis of CSV structure focusing on column classification"""
        
        self.logger.info(f"📊 Starting simplified CSV analysis for: {Path(csv_path).name}")
        
        # Set default additional_info to filename if not provided
        if additional_info is None:
            additional_info = f"the file name is {Path(csv_path).name}"
        
        # Load the CSV
        try:
            df = pd.read_csv(csv_path)
        except Exception as e:
            raise UnclearAssumptionsException(
                f"Cannot read CSV file {csv_path}: {e}",
                assumption_type="file_structure"
            )
        
        # Validate basic pandas-level assumptions
        self._validate_basic_structure(df, csv_path)
        
        # Remove columns that have no non-null values or only empty strings
        empty_cols = df.columns[
            (df.isna().all()) | 
            ((df.astype(str).str.strip() == '').all())
        ].tolist()
        if empty_cols:
            self.logger.info(f"🗑️ Removing {len(empty_cols)} empty columns: {empty_cols}")
            df = df.drop(columns=empty_cols)
        
        # Analyze CSV structure and semantic purpose with LLM
        csv_analysis = self._analyze_csv_with_llm(csv_path, additional_info)
        
        self.logger.info(f"📈 CSV dimensions: {len(df)} rows × {len(df.columns)} columns")
        self.logger.info(f"🔍 Using {self.sample_rows} sample rows for semantic analysis")
        self.logger.info(f"🎯 Semantic purpose: {csv_analysis.get('semantic_purpose', 'Unknown')}")
        
        # Analyze individual columns in parallel
        column_results = parallel_map(
            lambda col_name: self._analyze_single_column(df[col_name], col_name),
            df.columns,
            max_workers=5,
            timeout=30
        )
        
        # Convert list results back to dict mapping column names to results
        column_semantics = dict(zip(df.columns, column_results))
        
        # Combine structure analysis with semantic insights
        analysis = {
            "row_count": len(df),
            "column_count": len(df.columns),
            "column_names": list(df.columns),
            "column_semantics": column_semantics,
            # Add semantic analysis from LLM
            "table_purpose": csv_analysis['semantic_purpose'],
        }
        
        return analysis
    
    def _validate_basic_structure(self, df: pd.DataFrame, csv_path: str):
        """Validate basic pandas-level assumptions about the CSV structure"""
        
        # Check if dataframe is empty
        if df.empty:
            raise UnclearAssumptionsException(
                f"CSV file {csv_path} is empty - cannot make assumptions about data structure",
                assumption_type="data_presence"
            )
        
        # Check for meaningful column headers
        if len(df.columns) == 0:
            raise UnclearAssumptionsException(
                f"CSV file {csv_path} has no columns - cannot determine data structure",
                assumption_type="column_headers"
            )
        
        self.logger.debug(f"✅ Basic structure validated for {csv_path}")
    
    def _analyze_csv_with_llm(self, csv_path: str, additional_info: str) -> dict:
        """Use LLM to analyze CSV structure and semantic purpose by examining first two rows"""
        
        try:
            # Read first two rows without assuming headers
            df_raw = pd.read_csv(csv_path, header=None, nrows=2)
            
            if len(df_raw) < 1:
                raise UnclearAssumptionsException(
                    f"CSV file {csv_path} is empty or has no header row",
                    assumption_type="data_presence"
                )
            
            first_row = df_raw.iloc[0].astype(str).tolist()
            second_row = df_raw.iloc[1].astype(str).tolist() if len(df_raw) > 1 else []
            
            # Response schema for comprehensive CSV analysis
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
            Analyze this CSV file to understand both its structure and semantic purpose:
            
            FIRST ROW: {first_row}
            {f"SECOND ROW: {second_row}" if second_row else "ONLY ONE ROW AVAILABLE"}
            
            ADDITIONAL CONTEXT (for reference only, not definitive): {additional_info}
            Note: The additional context above is supplementary information that may provide hints 
            about the file's purpose, but you should base your analysis primarily on the actual 
            data structure and content patterns you observe.
            
            Please determine:
            
            1. HEADER DETECTION: Does the first row contain column headers or actual data?
               - Headers: descriptive names like ["name", "email", "company", "first_name"]  
               - Data: actual values like ["John Smith", "john@example.com", "Acme Corp"]
            
            2. SEMANTIC PURPOSE: What does this CSV file represent? Provide a clear, concise description.
               Examples: "Contact list with names and email addresses", "Employee directory with contact information", 
               "Customer database with purchase history", "Survey responses about product satisfaction"
            
            Base your analysis on the column names (if headers exist) and data patterns you observe.
            """
            
            start_time = time.time()
            
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                response_format={
                    "type": "json_schema",
                    "json_schema": {
                        "name": "csv_analysis",
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
                    f"CSV file {csv_path} appears to have no header row - "
                    f"first row contains data values rather than descriptive column names. "
                    f"Cannot make semantic assumptions without proper column headers. "
                    f"LLM reasoning: {result.get('reasoning', '')}",
                    assumption_type="column_headers"
                )
            
            self.logger.info(f"🔍 CSV Analysis for {csv_path}:")
            self.logger.info(f"  📋 Has headers: {result.get('has_header', False)}")
            self.logger.info(f"  🎯 Purpose: {result.get('semantic_purpose', 'Unknown')}")
            self.logger.info(f"  💭 Reasoning: {result.get('reasoning', '')}")
            
            return result
            
        except UnclearAssumptionsException:
            # Re-raise UnclearAssumptionsException as-is
            raise
        except Exception as e:
            self.logger.error(f"❌ CSV analysis failed for {csv_path}: {e}")
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
        
        # Basic statistics
        stats = {
            "total_count": len(series),
            "non_null_count": series.count(),
            "unique_count": series.nunique(),
            "completeness": series.count() / len(series) if len(series) > 0 else 0
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
        Classify this CSV column as either "identifier" or "text":
        
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
                model="gpt-4o-mini",
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
    

    