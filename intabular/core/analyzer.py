"""
CSV analysis module for semantic understanding of unknown data structures.
"""

import json
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List
from openai import OpenAI
import asyncio
from concurrent.futures import ThreadPoolExecutor
import threading


class CSVAnalyzer:
    """Analyzes unknown CSV files to understand their semantic structure"""
    
    def __init__(self, openai_client: OpenAI):
        self.client = openai_client
        self.max_parallel_calls = 5  # Limit parallel API calls
    
    def analyze_unknown_csv(self, csv_path: str) -> Dict[str, Any]:
        """Analyze unknown CSV structure and infer semantic meaning of columns"""
        
        # Read sample data
        df = pd.read_csv(csv_path)
        
        print(f"📊 CSV File Analysis: {Path(csv_path).name}")
        print(f"   📈 Dimensions: {len(df)} rows × {len(df.columns)} columns")
        print(f"   📋 Columns: {', '.join(df.columns[:5])}{'...' if len(df.columns) > 5 else ''}")
        
        # Analyze individual columns in parallel
        print(f"   🧠 Analyzing {len(df.columns)} columns individually...")
        column_analyses = self._analyze_columns_parallel(df)
        
        # Analyze overall table structure
        print(f"   🧠 Analyzing overall table structure...")
        table_analysis = self._analyze_table_structure(df, csv_path, column_analyses)
        
        # Combine results
        analysis = {
            "file_path": csv_path,
            "row_count": len(df),
            "column_count": len(df.columns),
            "column_semantics": column_analyses,
            **table_analysis
        }
        
        # Log analysis results
        self._log_analysis_results(analysis)
        
        return analysis
    
    def _analyze_columns_parallel(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze individual columns in parallel with schema-forced responses"""
        
        column_analyses = {}
        
        # Prepare column data for analysis
        column_data_list = []
        for col in df.columns:
            sample_values = df[col].dropna().head(5).tolist()
            non_null_count = df[col].notna().sum()
            completeness = (non_null_count / len(df)) * 100
            
            column_data_list.append({
                "name": col,
                "sample_values": sample_values,
                "completeness_percent": round(completeness, 1),
                "data_type": str(df[col].dtype),
                "unique_values": df[col].nunique(),
                "total_values": len(df)
            })
        
        # Process columns in batches to respect parallel limits
        batch_size = self.max_parallel_calls
        for i in range(0, len(column_data_list), batch_size):
            batch = column_data_list[i:i + batch_size]
            
            with ThreadPoolExecutor(max_workers=min(len(batch), self.max_parallel_calls)) as executor:
                futures = {
                    executor.submit(self._analyze_single_column, col_data): col_data["name"]
                    for col_data in batch
                }
                
                for future in futures:
                    col_name = futures[future]
                    try:
                        result = future.result(timeout=30)
                        column_analyses[col_name] = result
                        print(f"      ✅ {col_name}: {result.get('semantic_type', 'unknown')}")
                    except Exception as e:
                        print(f"      ❌ {col_name}: Analysis failed - {e}")
                        column_analyses[col_name] = self._get_fallback_column_analysis(col_name)
        
        return column_analyses
    
    def _analyze_single_column(self, column_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze a single column with schema-forced response"""
        
        # Define the response schema
        response_schema = {
            "type": "object",
            "properties": {
                "semantic_type": {
                    "type": "string",
                    "enum": ["email", "name", "company", "phone", "address", "identifier", "text", "number", "date", "url", "social", "industry", "title", "location", "other"]
                },
                "confidence": {
                    "type": "number",
                    "minimum": 0,
                    "maximum": 1
                },
                "description": {
                    "type": "string"
                },
                "data_pattern": {
                    "type": "string"
                },
                "business_value": {
                    "type": "string",
                    "enum": ["high", "medium", "low"]
                },
                "data_quality": {
                    "type": "string",
                    "enum": ["excellent", "good", "fair", "poor"]
                }
            },
            "required": ["semantic_type", "confidence", "description", "data_pattern", "business_value", "data_quality"],
            "additionalProperties": False
        }
        
        prompt = f"""
        Analyze this single CSV column to determine its semantic meaning and business value:
        
        Column Name: {column_data['name']}
        Data Type: {column_data['data_type']}
        Sample Values: {column_data['sample_values']}
        Completeness: {column_data['completeness_percent']}%
        Unique Values: {column_data['unique_values']} out of {column_data['total_values']}
        
        Based on both the column name and actual data values, determine:
        1. What type of information this column contains
        2. How confident you are in this assessment
        3. What business value this data provides
        4. The quality of the data
        
        Focus on the actual data values to understand what this column really represents.
        """
        
        response = self.client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "column_analysis",
                    "schema": response_schema
                }
            },
            temperature=0.1
        )
        
        return json.loads(response.choices[0].message.content)
    
    def _analyze_table_structure(self, df: pd.DataFrame, csv_path: str, column_analyses: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze overall table structure and purpose with schema-forced response"""
        
        # Define the response schema
        response_schema = {
            "type": "object",
            "properties": {
                "table_purpose": {"type": "string"},
                "data_source": {"type": "string"},
                "data_patterns": {
                    "type": "object",
                    "properties": {
                        "primary_entity": {"type": "string"},
                        "identifier_candidates": {
                            "type": "array",
                            "items": {"type": "string"}
                        },
                        "contact_info": {
                            "type": "array",
                            "items": {"type": "string"}
                        },
                        "personal_data": {
                            "type": "array",
                            "items": {"type": "string"}
                        },
                        "business_data": {
                            "type": "array",
                            "items": {"type": "string"}
                        },
                        "behavioral_data": {
                            "type": "array",
                            "items": {"type": "string"}
                        },
                        "metadata": {
                            "type": "array",
                            "items": {"type": "string"}
                        }
                    },
                    "required": ["primary_entity", "identifier_candidates", "contact_info", "personal_data", "business_data", "behavioral_data", "metadata"]
                },
                "quality_assessment": {
                    "type": "object",
                    "properties": {
                        "overall_completeness": {"type": "string"},
                        "data_consistency": {"type": "string"},
                        "potential_duplicates": {"type": "string"},
                        "enrichment_level": {"type": "string"}
                    },
                    "required": ["overall_completeness", "data_consistency", "potential_duplicates", "enrichment_level"]
                },
                "source_identification": {
                    "type": "object",
                    "properties": {
                        "platform_confidence": {
                            "type": "number",
                            "minimum": 0,
                            "maximum": 1
                        },
                        "platform_indicators": {
                            "type": "array",
                            "items": {"type": "string"}
                        },
                        "export_type": {
                            "type": "string",
                            "enum": ["contact_list", "lead_export", "crm_export", "social_export", "other"]
                        }
                    },
                    "required": ["platform_confidence", "platform_indicators", "export_type"]
                }
            },
            "required": ["table_purpose", "data_source", "data_patterns", "quality_assessment", "source_identification"],
            "additionalProperties": False
        }
        
        # Prepare column summary for context
        column_summary = {}
        for col_name, analysis in column_analyses.items():
            column_summary[col_name] = {
                "semantic_type": analysis.get("semantic_type", "unknown"),
                "confidence": analysis.get("confidence", 0),
                "business_value": analysis.get("business_value", "unknown")
            }
        
        prompt = f"""
        Analyze this CSV table's overall structure and purpose based on individual column analyses:
        
        File: {Path(csv_path).name}
        Total Rows: {len(df)}
        Total Columns: {len(df.columns)}
        
        Individual Column Analysis Results:
        {json.dumps(column_summary, indent=2)}
        
        Based on the semantic types and patterns of all columns together, determine:
        1. What is the overall business purpose of this dataset?
        2. What platform or system likely generated this data?
        3. How the columns work together to represent business entities
        4. Overall data quality and completeness patterns
        5. Source identification based on column patterns and naming conventions
        
        Consider the relationships between columns and what they reveal about the data source.
        """
        
        response = self.client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "table_analysis",
                    "schema": response_schema
                }
            },
            temperature=0.1
        )
        
        return json.loads(response.choices[0].message.content)
    
    def _get_fallback_column_analysis(self, col_name: str) -> Dict[str, Any]:
        """Provide fallback analysis when API call fails"""
        return {
            "semantic_type": "other",
            "confidence": 0.1,
            "description": f"Analysis failed for column {col_name}",
            "data_pattern": "unknown",
            "business_value": "low",
            "data_quality": "unknown"
        }
    
    def _log_analysis_results(self, analysis: Dict[str, Any]):
        """Log the results of the analysis"""
        print(f"✅ Semantic Analysis Complete:")
        print(f"   🎯 Business purpose: {analysis.get('table_purpose', 'Unknown')}")
        print(f"   📦 Identified source: {analysis.get('data_source', 'Unknown')}")
        
        if 'source_identification' in analysis:
            platform = analysis['source_identification'].get('platform_confidence', 0)
            export_type = analysis['source_identification'].get('export_type', 'unknown')
            print(f"   🔍 Platform detection: {platform:.2f} confidence ({export_type})")
        
        if 'quality_assessment' in analysis:
            completeness = analysis['quality_assessment'].get('overall_completeness', 'unknown')
            consistency = analysis['quality_assessment'].get('data_consistency', 'unknown')
            print(f"   📊 Data quality: {completeness} complete, {consistency} consistent")
        
        # Log column analysis summary
        column_types = {}
        for col_name, col_analysis in analysis.get('column_semantics', {}).items():
            semantic_type = col_analysis.get('semantic_type', 'unknown')
            column_types[semantic_type] = column_types.get(semantic_type, 0) + 1
        
        print(f"   📋 Column types detected: {dict(column_types)}") 