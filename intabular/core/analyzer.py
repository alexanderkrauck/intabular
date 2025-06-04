"""
Simplified Dataframe analysis for informed column understanding.
"""

import json
import os
import textwrap
import pandas as pd
from typing import Dict, Any
from openai import OpenAI

from intabular.core.config import GatekeeperConfig
from .logging_config import get_logger
from .utils import parallel_map
from .llm_logger import log_llm_call


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
        # Number of rows to use when analyzing the dataframe structure with the LLM
        self.df_analysis_rows = int(os.getenv("INTABULAR_DF_ANALYSIS_ROWS", "2"))
        self.logger = get_logger('analyzer')
    
    def analyze_dataframe_structure(self, df: pd.DataFrame, additional_info: str = None) -> DataframeAnalysis:
        """Simple analysis of DF structure focusing on column classification.
        
        This does inplace modifications to the dataframe.
        """
        
        self.logger.info("Starting dataframe analysis")
        
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
            self.logger.info(f"Removing {len(empty_cols)} empty columns: {empty_cols}")
            df.drop(columns=empty_cols, inplace=True)
        
        # Modify column names to be python style (lowercase with underscores and no special characters)
        df.columns = df.columns.str.replace('[^a-zA-Z0-9]', '_', regex=True).str.lower()
        
        # Analyze Dataframe structure and semantic purpose with LLM
        df_analysis = self._analyze_dataframe_with_llm(df, additional_info)
        
        self.logger.info(f"DataFrame: {len(df)} rows × {len(df.columns)} columns")
        self.logger.info(f"Purpose: {df_analysis.get('semantic_purpose', 'Unknown')}")
        
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
        
        self.logger.debug("Basic structure validated")
    
    def _analyze_dataframe_with_llm(self, df: pd.DataFrame, additional_info: str) -> dict:
        """Use LLM to analyze DF structure and semantic purpose.

        The number of data rows inspected can be configured via the
        ``INTABULAR_DF_ANALYSIS_ROWS`` environment variable (default ``2``).
        """
        
        try:
            if len(df) < 1:
                raise UnclearAssumptionsException(
                    f"DataFrame is empty or has no data rows",
                    assumption_type="data_presence"
                )
            
            # Get the header and sample data rows from the DataFrame
            header = df.columns.tolist()
            num_rows = min(self.df_analysis_rows, len(df))
            sampled_rows = [df.iloc[i].astype(str).tolist() for i in range(num_rows)]
            rows_text = "\n".join([f"ROW {i + 1}: {row}" for i, row in enumerate(sampled_rows)])
            
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
            
            prompt = textwrap.dedent(f"""
                Analyze this Dataframe to understand both its structure and semantic purpose:
                
                FIRST ROW/HEADER: {header}
                {rows_text}
                
                ADDITIONAL CONTEXT (for reference only, not definitive): {additional_info}
                Note: The additional context above is supplementary information that may provide hints 
                about the dataframe's purpose, but you should base your analysis primarily on the actual 
                data structure and content patterns you observe.
                
                Please determine:
                
                1. HEADER DETECTION: Does the first row contain column headers or actual data/placeholder values?
                   - Headers: descriptive names like ["name", "email", "company", "first_name"]  
                   - Data: actual values like ["John Smith", "john@example.com", "Acme Corp"] or placeholder values like ["1", "2", "3"]
                
                2. SEMANTIC PURPOSE: What does this Dataframe represent? Provide a clear, concise description.
                   Examples: "Contact list with names and email addresses", "Employee directory with contact information", 
                   "Customer database with purchase history", "Survey responses about product satisfaction"
                
                Base your analysis on the column names (if headers exist) and data patterns you observe.
            """).strip()
                        
            llm_kwargs = {
                "model": os.getenv("INTABULAR_STRATEGY_MODEL", "gpt-4o"),
                "messages": [{"role": "user", "content": prompt}],
                "response_format": {
                    "type": "json_schema",
                    "json_schema": {
                        "name": "dataframe_analysis",
                        "schema": response_schema
                    }
                },
                "temperature": 0.1
            }
                        
            response = log_llm_call(
                lambda: self.client.chat.completions.create(**llm_kwargs),
                **llm_kwargs
            )
            
            response_content = response.choices[0].message.content
            
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
            
            self.logger.debug(f"DataFrame analysis complete: {result.get('semantic_purpose', 'Unknown')}")
            
            return result
            
        except UnclearAssumptionsException:
            # Re-raise UnclearAssumptionsException as-is
            raise
        except Exception as e:
            self.logger.error(f"DataFrame analysis failed: {e}")
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
        
        # Clean sample values to handle multiline content
        cleaned_samples = [
            str(val).replace('\n', ' ').replace('\r', ' ') 
            if isinstance(val, str) else str(val)
            for val in sample_values
        ]

        prompt = textwrap.dedent(f"""
            You are a data analyst. The user wants to ingest a dataframe into an existing database. To do that you must analyze a particular column of the incoming dataframe and determine multiple things about it.
            
            Incoming column name: {col_name}
            Here are the first few non-null values: {cleaned_samples}
            Here are some basic statistics about the column: {stats}
            
            Classify according to the schema requirements.
        """).strip()
        
        # Enhanced schema with embedded rules and guidance
        response_schema = {
            "type": "object",
            
            "properties": {
                "reasoning": {
                    "type": "string",
                    "title": "Classification Reasoning",
                    "description": "Brief explanation of why this classification was chosen based on content structure and usage",
                    "minLength": 15,
                    "maxLength": 200
                },
                
                "data_type": {
                    "type": "string", 
                    "enum": ["identifier", "text"],
                    "title": "Column Classification",
                    "description": "identifier: structured references (names, emails, phones, IDs, websites, addresses, companies, titles, categories, dates, numbers) | text: free-form content (descriptions, notes, comments, explanations, statements)",
                    "$comment": "Default to 'identifier' unless clearly free-form text content"
                },
                
                "purpose": {
                    "type": "string",
                    "title": "Column Purpose",  
                    "description": "Explanation how the information of this column does or would contribute to the purpose of the database",
                    "examples": [
                        "Primary email address used for contacting customers and managing communication preferences",
                        "Product identifier used for inventory tracking and sales reporting in e-commerce system", 
                        "Customer feedback content used for sentiment analysis and product improvement insights",
                        "Unique identifier for employees used in HR management and payroll processing",
                        "Purchase value used for financial reporting and customer spending analysis",
                        "Clinical observations used for patient care coordination and treatment planning",
                        "Educational content identifier used for student enrollment and academic tracking"
                    ],
                    "pattern": "^[A-Z].*[^.]$|^[A-Z].*\\.$",
                    "minLength": 10,
                    "maxLength": 150
                }
            },
            
            "required": ["data_type", "purpose", "reasoning"],
            "additionalProperties": False,
            
            # Conditional validation based on classification
            "if": {
                "properties": {"data_type": {"const": "identifier"}}
            },
            "then": {
                "properties": {
                    "purpose": {
                        "description": "Should describe how this identifier is used for recognition, categorization, or reference"
                    }
                }
            },
            "else": {
                "properties": {
                    "purpose": {
                        "description": "Should describe the narrative or descriptive content and its business purpose"
                    }
                }
            }
        }
        
        try:
            llm_kwargs = {
                "model": os.getenv("INTABULAR_PROCESSOR_MODEL", "gpt-4o-mini"),
                "messages": [{"role": "user", "content": prompt}],
                "response_format": {
                    "type": "json_schema",
                    "json_schema": {
                        "name": "column_classification",
                        "schema": response_schema
                    }
                },
                "temperature": 0.1
            }
            
            response = log_llm_call(
                lambda: self.client.chat.completions.create(**llm_kwargs),
                **llm_kwargs
            )
            
            response_content = response.choices[0].message.content
            
            result = json.loads(response_content)
            result.update(stats)  # Add statistics to result
            
            self.logger.debug(f"{col_name}: {result.get('data_type', 'unknown')} - {result.get('purpose', 'No purpose provided')}")
            
            return result
            
        except Exception as e:
            self.logger.error(f"{col_name}: Classification failed - {e}")
            raise e # We don't want to fall back to a default analysis so we re-raise the error. This whole thing does not make sense without LLMs.
    

    