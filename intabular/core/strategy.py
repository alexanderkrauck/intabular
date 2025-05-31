"""
Entity-aware ingestion strategy creation for intelligent CSV mapping and merging.
"""

import json
import time
import re
from typing import Dict, Any, List, Tuple, Optional
from openai import OpenAI
from intabular.core.analyzer import DataframeAnalysis
from intabular.core.processor import SAFE_NAMESPACE
from .config import GatekeeperConfig
from .logging_config import get_logger, log_prompt_response, log_strategy_creation
from .utils import parallel_map


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

        prompt = f"""
        Create a transformation strategy for columns to be transformed into the target column. That means there are input columns and a target column and the goal is to transform the input columns into the target column if possible.
        
        GENERAL PURPOSE OF DATA: {target_config.purpose}
        TARGET COLUMN INFORMATION:
        {target_config.get_interpretable_column_information(target_col)}
        
        AVAILABLE SOURCE COLUMNS:
        {json.dumps(dataframe_analysis.dataframe_column_analysis, indent=2)}
        
        TRANSFORMATION TYPES:
        1. "format" - Apply deterministic transformation to normalize the value. Make sure to return rules that perfectly transform the input columns into the target column including all rules that the target column requires.
           Examples:
           - "email.strip().lower()" for email normalization
           - "f'{{first_name.strip().lower()}} {{last_name.strip().lower()}}'" for name combination
           - "re.sub(r'[^\\d]', '', phone)[:10]" for phone number cleanup
        2. "llm_format" - Use LLM for complex normalization decisions. Thereby first, the transformation rules are applied and then fed to an LLM to transform the value into the target column.
        3. "none" - No suitable source mapping found
        
        SAFE_NAMESPACE functions if using transformation rules in python syntax:
        {SAFE_NAMESPACE.keys()}"""

        response = self.client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "entity_column_mapping",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "transformation_type": {
                                "type": "string",
                                "enum": ["format", "llm_format", "none"],
                            },
                            "reasoning": {"type": "string"},
                            "transformation_rule": {"type": "string"},
                        },
                        "required": ["transformation_type", "reasoning"],
                        "additionalProperties": False,
                    },
                },
            },
            temperature=0.1,
        )
        
        result = json.loads(response.choices[0].message.content)
        
        if result["transformation_type"] != "none" and not result["transformation_rule"]:
            #TODO: fallback to explicit LLM call to generate transformation rule only.
            raise ValueError(f"Transformation rule is required for format transformation type for column {target_col}")

        return result

    def _create_descriptive_column_mapping(
        self,
        target_col: str,
        target_config: GatekeeperConfig,
        dataframe_analysis: DataframeAnalysis,
    ) -> Dict[str, Any]:
        """Create mapping strategy for descriptive columns - intelligent content merging with existing values"""

        prompt_merge = f"""
        Create a transformation strategy for a column.
        
        GENERAL PURPOSE OF DATA: {target_config.purpose}
        TARGET COLUMN INFORMATION:
        {target_config.get_interpretable_column_information(target_col)}
        
        AVAILABLE SOURCE COLUMNS:
        {json.dumps(dataframe_analysis.dataframe_column_analysis, indent=2)}
        
        CURRENT COLUMN INFORMATION:
        You can also in the transformation_rules utilize the value of the target column that we merge into by using "current".

        TRANSFORMATION TYPES:
        1. "format" - Apply deterministic transformation to normalize the value. Make sure to return rules that perfectly transform the input columns into the target column including all rules that the target column requires.
           Examples:
           - "email.strip().lower()" for email normalization
           - "f'{{first_name.strip().lower()}} {{last_name.strip().lower()}}'" for name combination
           - "re.sub(r'[^\\d]', '', phone)[:10]" for phone number cleanup
           - "f'Current: {{current}}, Notes: {{notes}}'" for merging of the current value with the notes column.
           - "notes" for notes column alone without any other columns or modifications.
        2. "llm_format" - Use LLM for complex normalization decisions. Thereby first, the transformation rules are applied and then fed to an LLM to transform the value into the target column.
        3. "none" - No suitable source mapping found
        
        SAFE_NAMESPACE functions if using transformation rules in python syntax:
        {SAFE_NAMESPACE.keys()}"""
        

        response = self.client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt_merge}],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "descriptive_column_mapping",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "transformation_type": {
                                "type": "string",
                                "enum": ["format", "llm_format", "none"],
                            },
                            "reasoning": {"type": "string"},
                            "transformation_rule": {"type": "string"},
                        },
                        "required": ["transformation_type", "reasoning", "transformation_rule"],
                        "additionalProperties": False,
                    },
                },
            },
            temperature=0.1,
        )

        return json.loads(response.choices[0].message.content)