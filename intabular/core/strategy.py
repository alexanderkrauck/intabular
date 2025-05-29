"""
Ingestion strategy creation for mapping unknown CSV to target schema.
"""

import json
from typing import Dict, Any, List, Tuple
from openai import OpenAI
from .config import TableConfig
from concurrent.futures import ThreadPoolExecutor
import threading


class IngestionStrategy:
    """Creates intelligent mapping strategies for CSV ingestion"""
    
    def __init__(self, openai_client: OpenAI):
        self.client = openai_client
        self.max_parallel_calls = 4  # Limit parallel API calls for strategy creation
    
    def create_ingestion_strategy(self, target_config: TableConfig, unknown_analysis: Dict) -> Dict[str, Any]:
        """Create strategy to ingest unknown CSV into target table structure"""
        
        print(f"🧠 Creating field-by-field ingestion strategy...")
        
        # Build target context
        target_context = self._build_target_context(target_config)
        
        # Get all target columns that need mapping
        target_columns = self._get_target_columns(target_config)
        source_columns = unknown_analysis.get('column_semantics', {})
        
        print(f"   📋 Analyzing {len(target_columns)} target fields...")
        
        # Create individual field mapping strategies in parallel
        field_strategies = self._create_field_mappings_parallel(
            target_columns, source_columns, target_context, unknown_analysis
        )
        
        # Create overall ingestion plan
        print(f"   🗺️ Creating overall ingestion plan...")
        ingestion_plan = self._create_ingestion_plan(
            target_config, unknown_analysis, field_strategies
        )
        
        # Identify unmapped source columns
        unmapped_columns = self._identify_unmapped_columns(
            source_columns, field_strategies
        )
        
        # Combine all results
        strategy = {
            "field_strategies": field_strategies,
            "unmapped_source_columns": unmapped_columns,
            **ingestion_plan
        }
        
        # Log strategy summary
        self._log_strategy_results(strategy)
        
        return strategy
    
    def _get_target_columns(self, config: TableConfig) -> List[str]:
        """Extract target column names from config"""
        if isinstance(config.enrichment_columns, dict):
            return list(config.enrichment_columns.keys())
        else:
            return list(config.enrichment_columns)
    
    def _create_field_mappings_parallel(self, target_columns: List[str], 
                                      source_columns: Dict[str, Any], 
                                      target_context: str,
                                      unknown_analysis: Dict) -> Dict[str, Any]:
        """Create field mapping strategies in parallel"""
        
        field_strategies = {}
        
        # Process target columns in batches
        batch_size = self.max_parallel_calls
        for i in range(0, len(target_columns), batch_size):
            batch = target_columns[i:i + batch_size]
            
            with ThreadPoolExecutor(max_workers=min(len(batch), self.max_parallel_calls)) as executor:
                futures = {
                    executor.submit(
                        self._create_single_field_mapping, 
                        target_col, 
                        source_columns, 
                        target_context,
                        unknown_analysis
                    ): target_col
                    for target_col in batch
                }
                
                for future in futures:
                    target_col = futures[future]
                    try:
                        result = future.result(timeout=30)
                        field_strategies[target_col] = result
                        strategy_type = result.get('strategy', 'unknown')
                        confidence = result.get('confidence', 0)
                        print(f"      ✅ {target_col}: {strategy_type} (confidence: {confidence:.2f})")
                    except Exception as e:
                        print(f"      ❌ {target_col}: Strategy creation failed - {e}")
                        field_strategies[target_col] = self._get_fallback_field_strategy(target_col)
        
        return field_strategies
    
    def _create_single_field_mapping(self, target_column: str, 
                                   source_columns: Dict[str, Any],
                                   target_context: str,
                                   unknown_analysis: Dict) -> Dict[str, Any]:
        """Create mapping strategy for a single target field with schema-forced response"""
        
        # Define response schema for field mapping
        response_schema = {
            "type": "object",
            "properties": {
                "strategy": {
                    "type": "string",
                    "enum": ["replace", "prompt_merge", "concat", "derive", "preserve", "transform"]
                },
                "source_mapping": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "confidence": {
                    "type": "number",
                    "minimum": 0,
                    "maximum": 1
                },
                "reasoning": {
                    "type": "string"
                },
                "transformation_rule": {
                    "type": "string"
                },
                "prompt_template": {
                    "type": "string"
                },
                "fallback_strategy": {
                    "type": "string",
                    "enum": ["preserve", "empty", "default_value"]
                }
            },
            "required": ["strategy", "source_mapping", "confidence", "reasoning", "transformation_rule", "prompt_template", "fallback_strategy"],
            "additionalProperties": False
        }
        
        # Prepare source column summary for this analysis
        relevant_sources = self._find_relevant_source_columns(target_column, source_columns)
        
        prompt = f"""
        Create a mapping strategy for this single target field by analyzing available source columns:
        
        TARGET FIELD: {target_column}
        Target Context: {target_context}
        
        AVAILABLE SOURCE COLUMNS:
        {json.dumps(relevant_sources, indent=2)}
        
        SOURCE DATA CONTEXT:
        Purpose: {unknown_analysis.get('table_purpose', 'unknown')}
        Data Source: {unknown_analysis.get('data_source', 'unknown')}
        
        Strategy Options:
        - "replace": Use source column to completely replace target
        - "prompt_merge": Use LLM to intelligently combine data
        - "concat": Concatenate multiple sources with separator
        - "derive": Create value by combining/transforming sources
        - "preserve": Keep existing target data, ignore sources
        - "transform": Apply specific transformation to source data
        
        Focus on finding the best single mapping for this target field.
        Consider data quality, semantic similarity, and business value.
        If no good source mapping exists, choose "preserve" strategy.
        """
        
        response = self.client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "field_mapping",
                    "schema": response_schema
                }
            },
            temperature=0.1
        )
        
        return json.loads(response.choices[0].message.content)
    
    def _find_relevant_source_columns(self, target_column: str, 
                                    source_columns: Dict[str, Any]) -> Dict[str, Any]:
        """Find source columns most relevant to target column"""
        
        # Return top 5 most relevant source columns based on semantic similarity
        # For now, return all source columns but this could be optimized
        return {col: data for col, data in list(source_columns.items())[:5]}
    
    def _create_ingestion_plan(self, target_config: TableConfig, 
                             unknown_analysis: Dict,
                             field_strategies: Dict[str, Any]) -> Dict[str, Any]:
        """Create overall ingestion plan with schema-forced response"""
        
        # Define response schema for ingestion plan
        response_schema = {
            "type": "object",
            "properties": {
                "data_quality_rules": {
                    "type": "object",
                    "properties": {
                        "deduplication": {
                            "type": "object",
                            "properties": {
                                "strategy": {
                                    "type": "string",
                                    "enum": ["email_based", "name_company_based", "custom"]
                                },
                                "key_fields": {
                                    "type": "array",
                                    "items": {"type": "string"}
                                },
                                "resolution": {
                                    "type": "string",
                                    "enum": ["prefer_target", "prefer_source", "merge_both"]
                                }
                            },
                            "required": ["strategy", "key_fields", "resolution"]
                        },
                        "validation": {
                            "type": "object",
                            "properties": {
                                "email_validation": {"type": "boolean"},
                                "phone_formatting": {"type": "boolean"},
                                "name_standardization": {"type": "boolean"}
                            },
                            "required": ["email_validation", "phone_formatting", "name_standardization"]
                        },
                        "cleanup": {
                            "type": "object",
                            "properties": {
                                "trim_whitespace": {"type": "boolean"},
                                "standardize_formats": {"type": "boolean"},
                                "handle_nulls": {
                                    "type": "string",
                                    "enum": ["preserve", "replace_with_empty", "skip"]
                                }
                            },
                            "required": ["trim_whitespace", "standardize_formats", "handle_nulls"]
                        }
                    },
                    "required": ["deduplication", "validation", "cleanup"]
                },
                "ingestion_plan": {
                    "type": "object",
                    "properties": {
                        "processing_order": {
                            "type": "array",
                            "items": {"type": "string"}
                        },
                        "conflict_resolution": {"type": "string"},
                        "error_handling": {
                            "type": "string",
                            "enum": ["continue", "stop", "log_and_continue"]
                        },
                        "validation_checks": {
                            "type": "array",
                            "items": {"type": "string"}
                        }
                    },
                    "required": ["processing_order", "conflict_resolution", "error_handling", "validation_checks"]
                },
                "confidence_score": {
                    "type": "number",
                    "minimum": 0,
                    "maximum": 1
                },
                "risk_assessment": {
                    "type": "object",
                    "properties": {
                        "data_loss_risk": {
                            "type": "string",
                            "enum": ["low", "medium", "high"]
                        },
                        "quality_impact": {
                            "type": "string",
                            "enum": ["positive", "neutral", "negative"]
                        },
                        "schema_compatibility": {
                            "type": "string",
                            "enum": ["excellent", "good", "fair", "poor"]
                        }
                    },
                    "required": ["data_loss_risk", "quality_impact", "schema_compatibility"]
                },
                "execution_summary": {
                    "type": "object",
                    "properties": {
                        "total_fields_mapped": {"type": "integer"},
                        "fields_requiring_llm": {"type": "integer"},
                        "complex_transformations": {"type": "integer"},
                        "estimated_processing_time": {
                            "type": "string",
                            "enum": ["fast", "medium", "slow"]
                        }
                    },
                    "required": ["total_fields_mapped", "fields_requiring_llm", "complex_transformations", "estimated_processing_time"]
                }
            },
            "required": ["data_quality_rules", "ingestion_plan", "confidence_score", "risk_assessment", "execution_summary"],
            "additionalProperties": False
        }
        
        # Calculate summary statistics
        strategy_types = [fs.get('strategy', 'unknown') for fs in field_strategies.values()]
        llm_fields = sum(1 for s in strategy_types if s in ['prompt_merge', 'derive'])
        complex_transforms = sum(1 for s in strategy_types if s == 'transform')
        
        prompt = f"""
        Create an overall ingestion plan based on individual field mapping strategies:
        
        TARGET TABLE:
        Purpose: {target_config.purpose}
        Column Policy: {target_config.get_column_policy_text()}
        
        SOURCE DATA:
        Purpose: {unknown_analysis.get('table_purpose', 'unknown')}
        Quality: {unknown_analysis.get('quality_assessment', {})}
        
        FIELD STRATEGIES SUMMARY:
        Total Fields: {len(field_strategies)}
        Strategy Distribution: {dict([(s, strategy_types.count(s)) for s in set(strategy_types)])}
        LLM-dependent Fields: {llm_fields}
        Complex Transformations: {complex_transforms}
        
        Create comprehensive data quality rules, processing plan, and risk assessment.
        Consider deduplication needs, validation requirements, and error handling.
        Base recommendations on the specific field strategies already determined.
        """
        
        response = self.client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "ingestion_plan",
                    "schema": response_schema
                }
            },
            temperature=0.1
        )
        
        return json.loads(response.choices[0].message.content)
    
    def _identify_unmapped_columns(self, source_columns: Dict[str, Any], 
                                 field_strategies: Dict[str, Any]) -> Dict[str, Any]:
        """Identify source columns that weren't mapped to any target field"""
        
        # Get all source columns used in mappings
        mapped_sources = set()
        for strategy in field_strategies.values():
            mapped_sources.update(strategy.get('source_mapping', []))
        
        # Find unmapped columns
        unmapped = {}
        for col_name, col_data in source_columns.items():
            if col_name not in mapped_sources:
                unmapped[col_name] = {
                    "reason": f"No suitable target field found for {col_data.get('semantic_type', 'unknown')} data",
                    "potential_use": self._suggest_potential_use(col_data),
                    "action": "ignore"  # Default action
                }
        
        return unmapped
    
    def _suggest_potential_use(self, col_data: Dict[str, Any]) -> str:
        """Suggest potential use for unmapped column"""
        semantic_type = col_data.get('semantic_type', 'unknown')
        business_value = col_data.get('business_value', 'low')
        
        if business_value == 'high':
            return "Consider adding to target schema for future use"
        elif semantic_type in ['email', 'phone', 'name']:
            return "Could be useful for contact enrichment"
        elif semantic_type in ['company', 'title']:
            return "Could be useful for business intelligence"
        else:
            return "May be stored as metadata if needed"
    
    def _get_fallback_field_strategy(self, target_column: str) -> Dict[str, Any]:
        """Provide fallback strategy when API call fails"""
        return {
            "strategy": "preserve",
            "source_mapping": [],
            "confidence": 0.1,
            "reasoning": f"Strategy creation failed for {target_column}, defaulting to preserve",
            "transformation_rule": "",
            "prompt_template": "",
            "fallback_strategy": "preserve"
        }
    
    def _build_target_context(self, config: TableConfig) -> str:
        """Build detailed context for target table"""
        
        context = f"Purpose: {config.purpose}\n"
        context += f"Column Policy: {config.get_column_policy_text()}\n\n"
        
        context += "Target Column Specifications:\n"
        if isinstance(config.enrichment_columns, dict):
            for col, desc in config.enrichment_columns.items():
                context += f"  - {col}: {desc}\n"
        else:
            for col in config.enrichment_columns:
                context += f"  - {col}: Important field\n"
        
        return context
    
    def _log_strategy_results(self, strategy: Dict[str, Any]):
        """Log strategy creation results"""
        if 'field_strategies' in strategy:
            print(f"   📋 Field mapping strategies:")
            strategy_counts = {}
            total_confidence = 0
            for target_col, field_strategy in strategy['field_strategies'].items():
                strategy_type = field_strategy.get('strategy', 'unknown')
                confidence = field_strategy.get('confidence', 0)
                strategy_counts[strategy_type] = strategy_counts.get(strategy_type, 0) + 1
                total_confidence += confidence
            
            avg_confidence = total_confidence / len(strategy['field_strategies']) if strategy['field_strategies'] else 0
            print(f"      📊 Strategy distribution: {dict(strategy_counts)}")
            print(f"      🎯 Average field confidence: {avg_confidence:.2f}")
        
        if 'unmapped_source_columns' in strategy:
            unmapped_count = len(strategy['unmapped_source_columns'])
            if unmapped_count > 0:
                print(f"   ⚠️  {unmapped_count} source columns will not be mapped")
        
        overall_confidence = strategy.get('confidence_score', 0)
        print(f"   🎯 Overall strategy confidence: {overall_confidence:.2f}")
        
        if 'execution_summary' in strategy:
            summary = strategy['execution_summary']
            print(f"   ⚡ Execution plan: {summary.get('total_fields_mapped', 0)} fields, "
                  f"{summary.get('fields_requiring_llm', 0)} LLM calls, "
                  f"{summary.get('estimated_processing_time', 'unknown')} speed") 