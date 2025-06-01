"""
Entity-aware ingestion strategy creation for intelligent CSV mapping and merging.
"""

import json
import os
import textwrap
from typing import Dict, Any
from openai import OpenAI
from intabular.core.analyzer import DataframeAnalysis
from intabular.core.processor import SAFE_NAMESPACE
from .config import GatekeeperConfig
from .logging_config import get_logger
from .utils import parallel_map
from .llm_logger import log_llm_call


class DataframeIngestionStrategyResult:
    """Simple container for ingestion strategy results"""
    
    def __init__(self, no_merge_column_mappings: Dict[str, Any], merge_column_mappings: Dict[str, Any]):
        self.no_merge_column_mappings = no_merge_column_mappings
        self.merge_column_mappings = merge_column_mappings


class DataframeIngestionStrategy:
    """Creates entity-aware mapping and merging strategies for CSV ingestion"""

    def __init__(self, openai_client: OpenAI):
        self.client = openai_client
        self.logger = get_logger("strategy")

    def create_ingestion_strategy(
        self, target_config: GatekeeperConfig, dataframe_analysis: DataframeAnalysis
    ) -> DataframeIngestionStrategyResult:
        """Create entity-aware strategy to intelligently map and merge CSV data"""

        self.logger.info("Creating entity-aware ingestion strategy...")


        # Process entity matching columns in parallel
        no_merge_column_mappings = dict(parallel_map(
            lambda target_col: (target_col, self._create_no_merge_column_mappings(target_col, target_config, dataframe_analysis)),
            list(target_config.all_columns.keys()),
            max_workers=5,
            timeout=30,
            retries=3
        ))


        # Process remaining columns in parallel  
        merge_column_mappings = dict(parallel_map(
            lambda target_col: (target_col, self._create_descriptive_column_mapping(target_col, target_config, dataframe_analysis)),
            list(target_config.descriptive_columns.keys()),
            max_workers=5,
            timeout=30,
            retries=3
        ))

        return DataframeIngestionStrategyResult(no_merge_column_mappings, merge_column_mappings)

    def _get_remaining_columns(
        self, target_config: GatekeeperConfig, entity_matching_columns: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Get all columns that are NOT used for entity matching"""

        remaining_columns = {}

        for col_name, col_config in target_config.all_columns.items():
            if col_name not in entity_matching_columns:
                remaining_columns[col_name] = col_config

        self.logger.info(f"Remaining columns: {list(remaining_columns.keys())}")
        return remaining_columns

    def _create_no_merge_column_mappings(
        self,
        target_col: str,
        target_config: GatekeeperConfig,
        dataframe_analysis: DataframeAnalysis,
    ) -> Dict[str, Any]:
        """Create mapping strategy for entity identifier columns - keep or replace, never merge content"""

        self.logger.info(f"Creating no merge column mapping for column {target_col} using dataframe analysis {dataframe_analysis.dataframe_column_analysis}")

        prompt_no_merge = textwrap.dedent("""
            You are creating a data transformation strategy for entity identifier columns in a database ingestion process.
            
            CONTEXT: New incoming data needs to be transformed into entity identifier columns that uniquely identify records.
            These are columns used for entity matching and should never merge content - they either replace or keep existing values.
            Your task is to analyze the incoming columns and determine how to transform them into the target column format.
            
            The general purpose of the database is: {target_purpose}
            The details about the target column we are transforming into is: {target_column_info}
            
            Rigorous information about the incoming dataframe columns is:
            {source_columns}
            
            The goal is to create a transformation rule that converts the incoming source data into the exact target column format,
            ensuring proper normalization for entity identification purposes.
        """).strip()

        # Enhanced schema with simplified transformation types
        response_schema = {
            "type": "object",
            "title": "Entity Column Transformation Strategy",
            "description": "Strategy for transforming source columns into entity identifier column format.",
            
            "properties": {
                "reasoning": {
                    "type": "string",
                    "title": "Transformation Reasoning", 
                    "description": "Explanation of why this transformation approach was chosen and how it maps source to target for entity identification",
                    "minLength": 20,
                    "maxLength": 300
                },
                
                "transformation_type": {
                    "type": "string",
                    "enum": ["format", "llm_format", "none"],
                    "title": "Transformation Method",
                    "description": "format: Apply deterministic Python transformation rules for normalization | llm_format: Use LLM to directly parse all source columns into target format | none: No suitable source mapping found",
                    "examples": ["format", "llm_format", "none"],
                    "$comment": "Choose 'format' for deterministic transformations like email normalization, 'llm_format' when complex interpretation of multiple source columns is needed, 'none' when no mapping is possible. For llm_format, the LLM will receive all source column values and types."
                },
                
                "transformation_rule": {
                    "type": "string",
                    "title": "Transformation Rule", 
                    "description": f"Python expression for transforming source data into normalized entity identifier format. You may use source column names as variables. Only required for 'format' transformation type. The ONLY available functions for transformations are: {str(SAFE_NAMESPACE.keys())}.",
                    "examples": [
                        "email.strip().lower()",
                        "f'{{first_name.strip().lower()}} {{last_name.strip().lower()}}'",
                        "re.sub(r'[^\\d]', '', phone)[:10]",
                        "company_name.strip().upper()",
                        "user_id.strip()",
                        "datetime.strptime(date_str, '%Y-%m-%d').strftime('%Y-%m-%d')"
                    ],
                    "minLength": 1,
                    "$comment": "The transformation rule must normalize the source column into a format that perfectly matches the target column requirements. Only required for 'format' type. For 'llm_format', this field is optional."
                },

                "llm_source_columns": {
                    "type": "array",
                    "items": {"type": "string"},
                    "title": "Source Columns for LLM",
                    "description": "List of source column names to provide to LLM for parsing. Only used when transformation_type is 'llm_format'. If not specified, all source columns will be provided.",
                    "examples": [
                        ["first_name", "last_name"],
                        ["email_address", "contact_email"],
                        ["company", "organization", "business_name"]
                    ],
                    "$comment": "Optional field to limit which source columns the LLM should consider. Useful for performance and focus when many irrelevant columns exist."
                }
            },
            
            "required": ["reasoning", "transformation_type"],
            "additionalProperties": False
        }

        llm_kwargs = {
            "model": os.getenv("INTABULAR_STRATEGY_MODEL", "gpt-4o"),
            "messages": [{"role": "user", "content": prompt_no_merge.format(
                target_purpose=target_config.purpose,
                target_column_info=target_config.get_interpretable_column_information(target_col),
                source_columns=json.dumps(dataframe_analysis.dataframe_column_analysis, indent=2)
            )}],
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "entity_column_mapping",
                    "schema": response_schema
                },
            },
            "temperature": 0.1,
        }
        
        response = log_llm_call(
            lambda: self.client.chat.completions.create(**llm_kwargs),
            **llm_kwargs
        )
        
        result = json.loads(response.choices[0].message.content)
        
        # Validate that format transformations have transformation rules
        if result["transformation_type"] == "format" and not result.get("transformation_rule"):
            raise ValueError(f"Transformation rule is required for format transformation type for column {target_col}")

        return result

    def _create_descriptive_column_mapping(
        self,
        target_col: str,
        target_config: GatekeeperConfig,
        dataframe_analysis: DataframeAnalysis,
    ) -> Dict[str, Any]:
        """Create mapping strategy for descriptive columns - intelligent content merging with existing values"""

        prompt_merge = textwrap.dedent("""
            You are creating a data transformation strategy for ingesting new data into an existing database.
            
            CONTEXT: New incoming data in the form of a dataframe needs to be transformed and merged into an existing database column. 
            Your task is to analyze the incoming columns and determine how to transform them into 
            the target column format if applicable.
            
            The general purpose of the database is: {target_purpose}
            The details about the column we are trying to merge into is: {target_column_info}
            
            Rigorous information about the incoming dataframe columns is:
            {source_columns}
            
            The goal is to create a transformation rule that converts the incoming source data into the target column format, potentially merging with existing content when appropriate or combining the content of multiple incoming columns.
        """).strip()
        
        # Enhanced schema with simplified transformation types
        response_schema = {
            "type": "object",
            "title": "Column Transformation Strategy",
            "description": f"Strategy for transforming source columns into target column format.",
            
            "properties": {
                "reasoning": {
                    "type": "string",
                    "title": "Transformation Reasoning",
                    "description": "Explanation of why this transformation approach was chosen and how it maps source to target",
                    "minLength": 20,
                    "maxLength": 300
                },
                
                "transformation_type": {
                    "type": "string",
                    "enum": ["format", "llm_format", "none"],
                    "title": "Transformation Method",
                    "description": "format: Apply deterministic Python transformation rules | llm_format: Use LLM to directly parse all source columns into target format | none: No suitable source mapping found",
                    "examples": ["format", "llm_format", "none"],
                    "$comment": "Choose 'format' for deterministic transformations, 'llm_format' when complex interpretation of multiple source columns is needed for merging/combining content, 'none' when no mapping is possible. For llm_format, the LLM will receive all source column values and types."
                },
                
                "transformation_rule": {
                    "type": "string", 
                    "title": "Transformation Rule",
                    "description": f"Python expression or column reference for transforming source data. Only required for 'format' transformation type. You may use source column names as variables and 'current' for existing target value. The ONLY available functions for transformations are: {str(SAFE_NAMESPACE.keys())}.",
                    "examples": [
                        "email.strip().lower()",
                        "f'{{first_name.strip().lower()}} {{last_name.strip().lower()}}'",
                        "re.sub(r'[^\\d]', '', phone)[:10]",
                        "f'Current: {{current}}, Notes: {{notes}}'",
                        "notes",
                        "company_name.strip().upper()",
                        "datetime.strptime(date_str, '%Y-%m-%d').strftime('%m/%d/%Y')"
                    ],
                    "minLength": 1,
                    "$comment": "Must be a valid Python expression that can be executed. Only required for 'format' type. For 'llm_format', this field is optional."
                },

                "llm_source_columns": {
                    "type": "array",
                    "items": {"type": "string"},
                    "title": "Source Columns for LLM",
                    "description": "List of source column names to provide to LLM for parsing. Only used when transformation_type is 'llm_format'. If not specified, all source columns will be provided.",
                    "examples": [
                        ["notes", "comments", "description"],
                        ["address", "city", "state", "zip"],
                        ["phone", "mobile", "contact_number"]
                    ],
                    "$comment": "Optional field to limit which source columns the LLM should consider. Useful for performance and focus when many irrelevant columns exist."
                }
            },
            
            "required": ["reasoning", "transformation_type"],
            "additionalProperties": False
        }

        llm_kwargs = {
            "model": os.getenv("INTABULAR_STRATEGY_MODEL", "gpt-4o"),
            "messages": [{"role": "user", "content": prompt_merge.format(
                target_purpose=target_config.purpose,
                target_column_info=target_config.get_interpretable_column_information(target_col),
                source_columns=json.dumps(dataframe_analysis.dataframe_column_analysis, indent=2)
            )}],
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "descriptive_column_mapping",
                    "schema": response_schema
                },
            },
            "temperature": 0.1,
        }

        response = log_llm_call(
            lambda: self.client.chat.completions.create(**llm_kwargs),
            **llm_kwargs
        )

        result = json.loads(response.choices[0].message.content)
        
        # Validate that format transformations have transformation rules
        if result["transformation_type"] == "format" and not result.get("transformation_rule"):
            raise ValueError(f"Transformation rule is required for format transformation type for column {target_col}")

        return result